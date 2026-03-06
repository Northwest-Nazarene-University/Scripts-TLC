## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

## Import necessary modules
import os, time, pandas as pd, zipfile, sys
from datetime import datetime

## Ensure ResourceModules path is available and import shared helpers
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))
from Local_Setup import LocalSetup
from TLC_Common import makeApiCall
from Common_Configs import coreCanvasApiUrl
from Error_Email import errorEmail

## Initialize LocalSetup and localSetup.logger so this module works when imported
localSetup = LocalSetup(datetime.now(), __file__)
logger = localSetup.logger

## External SIS resource path
SISResourcePath = localSetup.getExternalResourcePath("SIS")

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
4. The External_Resource_Paths.json file in the Configs TLC directory containing the SISResourcePath and IEResourcePath values.
5. The ResourceModules and ActionModules directories in the Scripts TLC directory for additional functionality.
"""

## Setup error handler
errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)

## This function reads the CSV file, deletes the enrollment, and re-enrolls the user with the new role
def importCXData():

    functionName = "importCXData"

    try:

        ## Create the url to check if there is an ongoing sis import
        checkSisImportUrl = f"{coreCanvasApiUrl}accounts/1/sis_imports"

        ## Make the api call to check if there is an ongoing sis import
        sisImportCheckResponse, _ = makeApiCall(localSetup, p1_apiUrl=checkSisImportUrl, firstPageOnly = True)

        ## Define a blank object to hold the sis imports
        sisImports = None

        ## If the response was not successful, log the error
        if sisImportCheckResponse.status_code != 200:

            localSetup.logger.error(f"Failed to check SIS imports. Status code: {sisImportCheckResponse.status_code}")
            localSetup.logger.error(f"Response: {sisImportCheckResponse.text}")
            ## Send an error email
            errorHandler.sendError(p1_errorLocation="importCXData", p1_errorInfo=f"Failed to check SIS imports. Status code: {sisImportCheckResponse.status_code}. Response: {sisImportCheckResponse.text}")
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
            abortImportResponse, _ = makeApiCall(localSetup, p1_apiUrl=abortImportUrl, p1_apiCallType="put")

            ## If the response was not successful, log the error
            if abortImportResponse.status_code != 200:

                localSetup.logger.error(f"Failed to abort SIS import. Status code: {abortImportResponse.status_code}")
                localSetup.logger.error(f"Response: {abortImportResponse.text}")
                ## Send an error email
                errorHandler.sendError(p1_errorLocation="importCXData", p1_errorInfo=f"Failed to abort SIS import. Status code: {abortImportResponse.status_code}. Response: {abortImportResponse.text}")
                return False

        ## Make a list of the CSV files in the SISResourcePath directory
        sisImportFilesList = os.listdir(SISResourcePath)

        ## For each file
        for sisImportFile in sisImportFilesList:

            ## Define an empty DataFrame to hold the file data
            fileDataDf = pd.DataFrame()

            ## Open it as a pd.DataFrame
            if sisImportFile.endswith('.csv'):
                filePath = os.path.join(SISResourcePath, sisImportFile)
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
        zipFilePath = os.path.join(SISResourcePath, zipFileName)

        ## Create a zip file from all CSVs in the directory
        with zipfile.ZipFile(zipFilePath, 'w') as zipf:
            for file in os.listdir(SISResourcePath):
                if file.endswith('.csv') and file != "canvas_dept.csv":
                    filePath = os.path.join(SISResourcePath, file)
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
            sisImportOjbect, _ = makeApiCall(localSetup,
                                          p1_apiUrl=importSisDataUrl,
                                          p1_payload=params,
                                          p1_files=files,
                                          p1_apiCallType="post",
                                          )

            ## If the response was not successful, log the error
            if sisImportOjbect.status_code != 200:

                localSetup.logger.error(f"Failed to import SIS data. Status code: {sisImportOjbect.status_code}")
                localSetup.logger.error(f"Response: {sisImportOjbect.text}")

                ## Send an error email
                errorHandler.sendError(p1_errorLocation="importCXData", p1_errorInfo=f"Failed to import SIS data. Status code: {sisImportOjbect.status_code}. Response: {sisImportOjbect.text}")

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
                    importStatusResponse, _ = makeApiCall(localSetup, p1_apiUrl=checkImportStatusUrl)

                    ## If the response was not successful, log the error
                    if importStatusResponse.status_code != 200:
                        localSetup.logger.error(f"Failed to check SIS import status. Status code: {importStatusResponse.status_code}")
                        localSetup.logger.error(f"Response: {importStatusResponse.text}")
                        
                        ## Send an error email
                        errorHandler.sendError(p1_errorLocation="importCXData", p1_errorInfo=f"Failed to check SIS import status. Status code: {importStatusResponse.status_code}. Response: {importStatusResponse.text}")
                        break

                    ## Otherwise, get the import status from the response
                    else:

                        importStatus = importStatusResponse.json()['progress']
                        localSetup.logger.info(f"SIS Import Status: {importStatus}")

                
                    ## Wait 5 minutes
                    localSetup.logger.info("Waiting 2 minutes for the SIS import to complete...")
                    time.sleep(120)

                ## Once the import is complete, log the success
                localSetup.logger.info(f"SIS Import Status: {importStatus}")

                ## If the import was successful, return True
                return True
        
        ## If the import was not successful, return False    
        return False
        

    except Exception as Error:
        errorHandler.sendError(functionName, Error)

if __name__ == "__main__":
    ## Set working directory
    os.chdir(os.path.dirname(__file__))

    ## Run the cx data sync
    CXDataSyncStatus = importCXData()
    
    ## If the cx data sync was successful
    if CXDataSyncStatus:
        ## Log the successful cx data sync
        localSetup.logger.info("CX Data Sync Successful")

    ## Otherwise
    else:

        ## Log the failed cx data sync
        localSetup.logger.error("CX Data Sync Failed")

        ## Send an error email
        errorHandler.sendError (scriptName, p1_ErrorInfo = "The CX Data Sync Failed. Please check the messages at https://nnu.instructure.com/accounts/1/sis_import for more information.")
