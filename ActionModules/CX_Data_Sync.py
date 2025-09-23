## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

## Import necessary modules
import traceback, os, sys, logging, requests, csv, threading, time, pandas as pd, re, json, zipfile, tempfile
from datetime import datetime, date

## Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = "CX_Data_Sync"

scriptPurpose = r"""
This script pulls the Canvas SIS data from the Canvas Resources directory, zips the files, and uploads them to the Canvas API.
"""
externalRequirements = r"""
This script requires the following external resources:
1. Canvas Resources directory containing the SIS data files in CSV format.
2. A valid Canvas API access token stored in the Configs TLC directory as Canvas_Access_Token.txt.
3. The Core_Canvas_Url.txt file in the Configs TLC directory containing the base URL for the Canvas API.
4. The Base_External_Paths.json file in the Configs TLC directory containing the baseExternalInputPath and baseIeDepartmentDataExternalOutputPath values.
5. The ResourceModules and ActionModules directories in the Scripts TLC directory for additional functionality.
"""

## Date Variables
currentDate = date.today()
currentDatetime = datetime.now()
currentMonth = currentDate.month
currentYear = currentDate.year
lastYear = currentYear - 1
nextYear = currentYear + 1
century = str(currentYear)[:2]
decade = str(currentYear)[2:]

## Set working directory
os.chdir(os.path.dirname(__file__))

## Relative Path (this changes depending on the working directory of the main script)
pfRelativePath = r".\\"

## If the Canvas directory is not in the folder the relative path points to
## find the Canvas directory and set the relative path to its parent folder
while "Scripts TLC" not in os.listdir(pfRelativePath):
    pfRelativePath = f"..\\{pfRelativePath}"

## Change the relative path to an absolute path
pfAbsolutePath = f"{os.path.abspath(pfRelativePath)}\\"


## Local Path Variables
baseLogPath = f"{pfAbsolutePath}Logs\\{scriptName}\\"
baseLocalInputPath = f"{pfAbsolutePath}Canvas Resources\\"
baseLocalOutputPath = f"{pfAbsolutePath}Canvas Resources\\"
configPath = f"{pfAbsolutePath}Configs TLC\\"

## External Path Variables

## Define a variable to hold the base external input path and output path 
baseExternalInputPath = None ## Where the sis input files are stored
baseExternalOutputPath = None ## Where the output files are stored

## Open Base_External_Paths.json from the config path and get the baseExternalInputPath and baseExternalOutputPath values
with open (f"{configPath}Base_External_Paths.json", "r") as file:
    fileJson = json.load(file)
    baseExternalInputPath = fileJson["baseExternalInputPath"]
    baseExternalOutputPath = fileJson["baseIeDepartmentDataExternalOutputPath"]

## If the base log path doesn't already exist, create it
if not os.path.exists(baseLogPath):
    os.makedirs(baseLogPath, mode=0o777, exist_ok=False)

## Add Input Modules to the sys path
sys.path.append(f"{pfAbsolutePath}Scripts TLC\\ResourceModules")
sys.path.append(f"{pfAbsolutePath}Scripts TLC\\ActionModules")

## Import local modules
from Error_Email_API import errorEmailApi
from Make_Api_Call import makeApiCall
from Get_Courses import termGetCourses
from Get_TUG_Students import termGetTugStudents

## Canvas Instance Url
coreCanvasApiUrl = None
## Open the Core_Canvas_Url.txt from the config path
with open(f"{configPath}Core_Canvas_Url.txt", "r") as file:
    coreCanvasApiUrl = file.readlines()[0]

## If the script is run as main the folder with the access token is in the parent directory
canvasAccessToken = ""
## Open and retrieve the Canvas Access Token
with open(f"{configPath}Canvas_Access_Token.txt", "r") as file:
    canvasAccessToken = file.readlines()[0]
    ## Read the Canvas Access Token

## Log configurations
logger = logging.getLogger(__name__)
rootFormat = ("%(asctime)s %(levelname)s %(message)s")
FORMAT = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
logging.basicConfig(format=rootFormat, filemode="a", level=logging.INFO)

## Info Log Handler
infoLogFile = f"{baseLogPath}\\Info Log.txt"
logInfo = logging.FileHandler(infoLogFile, mode='a')
logInfo.setLevel(logging.INFO)
logInfo.setFormatter(FORMAT)
logger.addHandler(logInfo)

## Warning Log handler
warningLogFile = f"{baseLogPath}\\Warning Log.txt"
logWarning = logging.FileHandler(warningLogFile, mode='a')
logWarning.setLevel(logging.WARNING)
logWarning.setFormatter(FORMAT)
logger.addHandler(logWarning)

## Error Log handler
errorLogFile = f"{baseLogPath}\\Error Log.txt"
logError = logging.FileHandler(errorLogFile, mode='a')
logError.setLevel(logging.ERROR)
logError.setFormatter(FORMAT)
logger.addHandler(logError)

## The variable below holds a set of the functions that have had errors. This enables the error_handler function to only send
## an error email the first time the function triggers an error
setOfFunctionsWithErrors = set()

## This function handles function errors
def error_handler(p1_errorLocation, p1_errorInfo, sendOnce=True):
    functionName = "error_handler"
    logger.error(f"\nA script error occurred while running {p1_errorLocation}. Error: {str(p1_errorInfo)}")

    ## If the function with the error has not already been processed send an email alert
    if p1_errorLocation not in setOfFunctionsWithErrors:
        errorEmailApi.sendEmailError(p2_scriptName=scriptName, p2_scriptPurpose=scriptPurpose,
                                     p2_externalRequirements=externalRequirements,
                                     p2_errorLocation=p1_errorLocation, p2_ErrorInfo=p1_errorInfo)
        setOfFunctionsWithErrors.add(p1_errorLocation)
        logger.error(f"\nError Email Sent")
    else:
        logger.error(f"\nError email already sent")

## This function reads the CSV file, deletes the enrollment, and re-enrolls the user with the new role
def importCXData():

    functionName = "importCXData"

    try:

        ## Create the url to check if there is an ongoing sis import
        checkSisImportUrl = f"{coreCanvasApiUrl}accounts/1/sis_imports"

        ## Make the api call to check if there is an ongoing sis import
        sisImportCheckResponse = makeApiCall(p1_apiUrl=checkSisImportUrl, firstPageOnly = True)

        ## Define a blank object to hold the sis imports
        sisImports = None

        ## If the response was not successful, log the error
        if sisImportCheckResponse.status_code != 200:

            logger.error(f"Failed to check SIS imports. Status code: {sisImportCheckResponse.status_code}")
            logger.error(f"Response: {sisImportCheckResponse.text}")
            ## Send an error email
            error_handler(p1_errorLocation="importCXData", p1_errorInfo=f"Failed to check SIS imports. Status code: {sisImportCheckResponse.status_code}. Response: {sisImportCheckResponse.text}")
            return False

        ## Otherwise
        else:
            ## Get the sis imports from the response
            sisImports = sisImportCheckResponse.json()["sis_imports"]

        ## If the first element of the the sis_imports list has a status of "initializing"
        if sisImports and sisImports[0]['workflow_state'] == 'initializing':

            ## Get the id of the import
            importId = sisImports[0]['id']

            ## Define an abort api url 
            abortImportUrl = f"{coreCanvasApiUrl}accounts/1/sis_imports/{importId}/abort"

            ## Make the api call to abort the import
            abortImportResponse = makeApiCall(p1_apiUrl=abortImportUrl, apiCallType="put")

            ## If the response was not successful, log the error
            if abortImportResponse.status_code != 200:

                logger.error(f"Failed to abort SIS import. Status code: {abortImportResponse.status_code}")
                logger.error(f"Response: {abortImportResponse.text}")
                ## Send an error email
                error_handler(p1_errorLocation="importCXData", p1_errorInfo=f"Failed to abort SIS import. Status code: {abortImportResponse.status_code}. Response: {abortImportResponse.text}")
                return False

        ## Make a list of the CSV files in the baseExternalInputPath directory
        sisImportFilesList = os.listdir(baseExternalInputPath)

        ## For each file
        for sisImportFile in sisImportFilesList:

            ## Define an empty DataFrame to hold the file data
            fileDataDf = pd.DataFrame()

            ## Open it as a pd.DataFrame
            if sisImportFile.endswith('.csv'):
                filePath = os.path.join(baseExternalInputPath, sisImportFile)
                fileDataDf = pd.read_csv(filePath)

            ## If there are start_date and an end_date columns
            if not fileDataDf.empty and ('start_date' in fileDataDf.columns and 'end_date' in fileDataDf.columns):

                ## Set the date format to be compatible with the Canvas API (i.e. 2012-03-14)
                fileDataDf['start_date'] = pd.to_datetime(fileDataDf['start_date']).dt.strftime('%Y-%m-%d')
                fileDataDf['end_date'] = pd.to_datetime(fileDataDf['end_date']).dt.strftime('%Y-%m-%d')

                ## Overwrite the file with the updated dates
                fileDataDf.to_csv(filePath, index=False)



        ## Define the zip file name and path
        zipFileName = "cxDataImportZip.zip"
        zipFilePath = os.path.join(baseExternalInputPath, zipFileName)

        ## Create a zip file from all CSVs in the directory
        with zipfile.ZipFile(zipFilePath, 'w') as zipf:
            for file in os.listdir(baseExternalInputPath):
                if file.endswith('.csv') and file != "canvas_dept.csv":
                    filePath = os.path.join(baseExternalInputPath, file)
                    zipf.write(filePath, arcname=file)

        ## Open the temporary zip file
        with open (zipFilePath, 'rb') as CXDataFile:

            ## Define the neccessary files dict
            files = {
                'attachment': ('sis_import.zip', CXDataFile, 'application/zip')
            }

            ## Define the parameters
            params = {
                'import_type': 'instructure_csv'
            }

            ## Define the neccessary url
            importSisDataUrl = f"{coreCanvasApiUrl}accounts/1/sis_imports"

            ## Make the api call and save the response
            sisImportOjbect = makeApiCall (p1_apiUrl = importSisDataUrl
                                             , p1_payload = params
                                             , p1_files = files
                                             , apiCallType = "post"
                                             )

            ## If the response was not successful, log the error
            if sisImportOjbect.status_code != 200:

                logger.error(f"Failed to import SIS data. Status code: {sisImportOjbect.status_code}")
                logger.error(f"Response: {sisImportOjbect.text}")

                ## Send an error email
                error_handler(p1_errorLocation="importCXData", p1_errorInfo=f"Failed to import SIS data. Status code: {sisImportOjbect.status_code}. Response: {sisImportOjbect.text}")

            ## Otherwise
            else:

                ## Get the id of the import
                importId = sisImportOjbect.json()['id']

                ## Define the import status check url by adding 1/sis_imports/:id
                checkImportStatusUrl = f"{coreCanvasApiUrl}accounts/1/sis_imports/{importId}"

                ## Define a blank ojbect to hold the import status
                importStatus = None

                ## While the import status is not complete
                while importStatus != 100:

                    ## Make the api call to check the import status
                    importStatusResponse = makeApiCall(p1_apiUrl=checkImportStatusUrl)

                    ## If the response was not successful, log the error
                    if importStatusResponse.status_code != 200:
                        logger.error(f"Failed to check SIS import status. Status code: {importStatusResponse.status_code}")
                        logger.error(f"Response: {importStatusResponse.text}")
                        
                        ## Send an error email
                        error_handler(p1_errorLocation="importCXData", p1_errorInfo=f"Failed to check SIS import status. Status code: {importStatusResponse.status_code}. Response: {importStatusResponse.text}")
                        break

                    ## Otherwise, get the import status from the response
                    else:

                        importStatus = importStatusResponse.json()['progress']
                        logger.info(f"SIS Import Status: {importStatus}")

                
                    ## Wait 5 minutes
                    logger.info("Waiting 2 minutes for the SIS import to complete...")
                    time.sleep(120)

                ## Once the import is complete, log the success
                logger.info(f"SIS Import Status: {importStatus}")

                ## If the import was successful, return True
                return True
        
        ## If the import was not successful, return False    
        return False
        

    except Exception as error:
        error_handler(functionName, error)

if __name__ == "__main__":
    ## Set working directory
    os.chdir(os.path.dirname(__file__))
    
    ## Change the role for the listed enrollments
    importCXData()

    ## Wait for user input to exit
    input("Press enter to exit")
