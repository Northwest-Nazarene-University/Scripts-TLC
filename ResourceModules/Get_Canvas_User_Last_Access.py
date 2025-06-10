# Author: Bryce Miller - brycezmiller@nnu.edu
# Last Updated by: Bryce Miller

from datetime import datetime
from Download_File import downloadFile
import requests, time, json, os, logging, sys, re

# Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = "Get_Canvas_User_Last_Accesss"

scriptPurpose = r"""
This script (Get_Canvas_User_Last_Accesss) uses the Canvas API to run a enrollments report and download the .csv result.
It requires the following paramters: the path the .csv should be saved to (p1_SavePath), the API 
header (p1_header), and the p1_inputTerm.
"""
externalRequirements = r"""
To function properly this script requires either no provided parameters (in which case it runs on 
the current term) or a term code in the FA20 format.
"""

## Date Variables
currentDate = datetime.now()
currentMonth = currentDate.month
currentYear = currentDate.year
century = str(currentYear)[:2]
decade = str(currentYear)[2:]

## Relative Path (this changes depending on the working directory of the main script)
PFRelativePath = r".\\"

## If the Canvas directory is not in the folder the relative path points to
## find the Canvas directory and set the relative path to its parent folder
while "Scripts TLC" not in os.listdir(PFRelativePath):

    PFRelativePath = f"..\\{PFRelativePath}"

## Change the relative path to an absolute path
PFAbsolutePath = f"{os.path.abspath(PFRelativePath)}\\"

## Add Input Modules to the sys path
sys.path.append(f"{PFAbsolutePath}Scripts TLC\\ResourceModules")
sys.path.append(f"{PFAbsolutePath}Scripts TLC\\ActionModules")

## Import local modules
from Error_Email_API import errorEmailApi
from Make_Api_Call import makeApiCall

## Local Path Variables
baseLogPath = f"{PFAbsolutePath}Logs\\{scriptName}\\"
outputPath = f"{PFAbsolutePath}Canvas Resources\\"
configPath = f"{PFAbsolutePath}Configs TLC\\"

## If the base log path doesn't already exist, create it
if not (os.path.exists(baseLogPath)):
    os.makedirs(baseLogPath, mode=0o777, exist_ok=False)

## Final length of relative Path
relPathLen = len(PFRelativePath)

## Canvas Instance Url
CoreCanvasAPIUrl = None
## Open the Core_Canvas_Url.txt from the config path
with open (f"{configPath}Core_Canvas_Url.txt", "r") as file:
    CoreCanvasAPIUrl = file.readlines()[0]

## If the script is run as main the folder with the access token is in the parent directory
canvasAccessToken = ""

## Open and retrieve the Canvas Access Token
with open (f"{configPath}\Canvas_Access_Token.txt", "r") as file:
    canvasAccessToken = file.readlines()[0]

## Log configurations
logger = logging.getLogger(__name__)
rootFormat = ("%(asctime)s %(levelname)s %(message)s")
FORMAT = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
logging.basicConfig(format=rootFormat, filemode = "a", level=logging.INFO)

## Info Log Handler
infoLogFile = f"{baseLogPath}\\Info Log.txt"
logInfo = logging.FileHandler(infoLogFile, mode = 'a')
logInfo.setLevel(logging.INFO)
logInfo.setFormatter(FORMAT)
logger.addHandler(logInfo)

## Warning Log handler
warningLogFile = f"{baseLogPath}\\Warning Log.txt"
logWarning = logging.FileHandler(warningLogFile, mode = 'a')
logWarning.setLevel(logging.WARNING)
logWarning.setFormatter(FORMAT)
logger.addHandler(logWarning)

## Error Log handler
errorLogFile = f"{baseLogPath}\\Error Log.txt"
logError = logging.FileHandler(errorLogFile, mode = 'a')
logError.setLevel(logging.ERROR)
logError.setFormatter(FORMAT)
logger.addHandler(logError)

## The variable below holds a set of the functions that have had errors. This enables the except function to only send
## an error email the first time the function triggeres an error
setOfFunctionsWithErrors = set()

## This function handles function errors
def error_handler (p1_ErrorLocation, p1_ErrorInfo, sendOnce = True):
    functionName = "except"
    logger.error (f"     \nA script error occured while running {p1_ErrorLocation}. " +
                     f"Error: {str(p1_ErrorInfo)}")
    ## If the function with the error has not already been processed send an email alert
    if (p1_ErrorLocation not in setOfFunctionsWithErrors):
        errorEmailApi.sendEmailError(p2_ScriptName = scriptName, p2_ScriptPurpose = scriptPurpose, 
                                     p2_ExternalRequirements = externalRequirements, 
                                     p2_ErrorLocation = p1_ErrorLocation, p2_ErrorInfo = f"{p1_ErrorInfo}: \n\n {traceback.format_exc()}")
        setOfFunctionsWithErrors.add(p1_ErrorLocation)
        ## Note that an error email was sent
        logger.error (f"     \nError Email Sent")
    ## Otherwise log the fact that an error email as already been sent
    else:
        logger.error (f"     \nError email already sent")

## This function uses the user provided term to determines which Canvas school enrollments to pull users from
## with the Canvas Reports API and creates a .csv file of the courses in those Canvas school enrollments
def createLastUserAccesssCSV(p1_header, p1_inputTerm, attempt = 0):
    functionName = "createLastUserAccesssCSV"
    try:
        logger.info (f"     \nStarting {p1_inputTerm} last_user_access report")

        ## Determine and save the term's school year
        schoolYear = None
        if re.search("AF|FA|GF", p1_inputTerm):
            ## Fall terms are the first terms of a new school year so FA20 is part of the 2020-21 school year.
            schoolYear = (century + p1_inputTerm[2:] + "-" + str(int(p1_inputTerm[2:]) + 1))
        elif re.search("SP|GS|AS|SG|SA|SU", p1_inputTerm):
            ## Spring and Summer terms belong in the same school year as the fall terms before them, so SP21 is part of the same 2020-21 school year as FA20.
            schoolYear = (century + str(int(p1_inputTerm[2:]) - 1) + "-" + p1_inputTerm[2:])

        ## Create the school year relavent output path
        schoolYearOutputPath = f"{outputPath}\\{schoolYear}\\"
        
        ## Define the term specific output path
        termOutputPath = f"{schoolYearOutputPath}{p1_inputTerm}\\"
        
        ## If the user did not provide a term, set the target path to the default
        targetDestination = None
        if p1_inputTerm:
            targetDestination = f"{termOutputPath}{p1_inputTerm}_Canvas_User_Last_Access.csv"
        else:
            targetDestination = f"{outputPath}Canvas_User_Last_Access.csv"
        
        ## If the target file exists
        if os.path.exists(targetDestination):
            
            ## Get its last moddifed timestamp
            targetFileTimestamp = os.path.getmtime(targetDestination)

            ## Convert the timestamp to datetime
            targetFileDateTime = datetime.fromtimestamp(targetFileTimestamp)

            ## Subtract the file's datetime from the current datetime
            targetFileHoursOld = int((currentDate - targetFileDateTime).total_seconds() // 3600)

            ## If it has been less than hour or more since the target was updated
            if targetFileHoursOld < 3.5:

                ## logger.info that the file is up to date and return
                logger.info (f"     \n{p1_inputTerm} Target CSV is up to date")
                return targetDestination
        
        ## Define and initialize the api url for starting reports
        start_report_API_URL = CoreCanvasAPIUrl + "accounts/1/reports/last_user_access_csv"
        
        ##Define and initialize the lists of relavent enrollments, the IDs of their related reports, and their download urls
        term_report_ID = ""
        term_report_download_url = ""
        
        ## Make an api call to start a last_user_access report for each of the relavent enrollments and append the report id to the term_report_ID list
        logger.info ("\n" + "Calling last_user_access reports")

        ## Initilize payload with one of the relavent enrollments
        payload = None
        if p1_inputTerm:
            payload = {"parameters[enrollment_term_id]":f"sis_term_id:{p1_inputTerm}"}
        else:
            payload = {}

        ## Make the api call using makeApiCall
        report_object = makeApiCall(p1_header = p1_header, p1_apiUrl = start_report_API_URL, p1_payload = payload, apiCallType = "post")
            
        ## Make the API call
        #report_object = requests.post(start_report_API_URL, headers = p1_header, params = payload)

        ## Convert report_textFAFA_jsonObject recieved through the API call in json to a Python Dictionary
        report_text_jsonObject = json.loads(report_object.text)
        term_report_ID = report_text_jsonObject["id"]

        ## Check the status of each report and, if the progress == 100, append the report's download url to term_report_download_url
        logger.info ("\nChecking statuses of last_user_access reports")

        ## Define the status report the Google api url with the report ID added on the end
        status_report_API_URL = CoreCanvasAPIUrl + "accounts/1/reports/last_user_access_csv/" + str(term_report_ID)
        ## Make the status api call
        status_object = requests.get(status_report_API_URL, headers = p1_header)
        ## Convert status_text_jsonObject from json to a Python Dictionary
        status_text_jsonObject = json.loads(status_object.text)
        ## Define relavent report variables
        reportTerm = None
        if p1_inputTerm:
            reportTerm = status_text_jsonObject["parameters"]["enrollment_term_id"][-4:]
        else:
            reportTerm = "All"

        ## Check whether the report has finished and if it isn't, wait 10 seconds 
        ## continue to check until the report shows progress as 100
        while (status_text_jsonObject["progress"] != 100):
            ## Wait 10 seconds
            logger.info ("\n" + f"{p1_inputTerm} last_user_access report is incomplete. Waiting 10 seconds")
            time.sleep(10)
            
            ## Remake the call
            status_object = requests.get(status_report_API_URL, headers = p1_header)
            
            ## Reinitilize the the status text dictionary so the progress can be checked again
            status_text_jsonObject = json.loads(status_object.text)

        ## If the report failed
        if "parameters" in status_text_jsonObject.keys():
            if "extra_text" in status_text_jsonObject["parameters"].keys():
                if "failed" in status_text_jsonObject["parameters"]["extra_text"]:
                    
                    ## Recursively call this function again unless it has already been called 3 times
                    if attempt != 4:
                        createLastUserAccesssCSV(p1_header, p1_inputTerm, attempt + 1)
                    
                    ## If this is the third attempt, log that the report failed
                    else:
                        logger.warning (f"\nError: {p1_inputTerm} last_user_access report failed")
                        return None
            
        ## : to append the download url to the term_report_download_url list
        ## If for whatever reason the report failed and there is no download url, make a note of it
        logger.info (f"     \n\nRecording download url for report term: {reportTerm}")
        reportDownloadUrl = status_text_jsonObject["attachment"]["url"]
        reportText = status_text_jsonObject["parameters"]["extra_text"]
        
        try: ## Irregular try clause, do not comment out in testing
            ## Add the download url to the relavent list
            term_report_download_url = reportDownloadUrl
        
        except: ## Irregular except clause, do not comment out in testing
            ## Make note that there was no download URL
            logger.warning (f"\nError: No download url for the {reportTerm} report")

            ## Make note of the text of the status object with no url
            logger.info (f"     \n\nExtra Text: {reportText}")

        ## Download the .csv file located at the term_report_download_url
        ## Overwrite the file of the same name if it exists
        logger.info ("\nDownloading last_user_access users report")
        if p1_inputTerm:
            downloadFile(term_report_download_url, targetDestination, "w")
        else:
            downloadFile(term_report_download_url, targetDestination, "w")
            
        ## Log that the file was downloaded
        logger.info (f"     \nSucessfully downloaded {p1_inputTerm} Canvas_User_Last_Access CSV")
        
        ## Return the path to the downloaded file
        return targetDestination

    except Exception as error:
        error_handler (functionName, p1_ErrorInfo = error)

def termGetCanvasUserLastAccess(inputTerm = ""):
    functionName = "Get_Canvas_User_Last_Accesss"

    ## Define the API Call header using the retreived Canvas Token
    header = {'Authorization' : f"Bearer {canvasAccessToken}"}

    ## Start and download the Canvas report
    targetDestination = createLastUserAccesssCSV(p1_header = header, p1_inputTerm = inputTerm)

    ## Return the path to the downloaded file
    return targetDestination

if __name__ == "__main__":

    ## Set working directory
    os.chdir(os.path.dirname(__file__))

    ## Define the API Call header using the retreived Canvas Token
    header = {'Authorization' : f"Bearer {canvasAccessToken}"}

    ## Start and download the Canvas report
    createLastUserAccesssCSV (p1_header = header, p1_inputTerm = input("Enter the desired term in \
four character format (FA20, SU20, SP20): "))

    input("Press enter to exit")