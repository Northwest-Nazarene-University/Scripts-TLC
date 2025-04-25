# Author: Bryce Miller - brycezmiller@nnu.edu
# Last Updated by: Bryce Miller


from Error_Email_API import errorEmailApi
from datetime import datetime
from Download_File import downloadFile
import requests, time, json, os, logging, sys

# Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = "Get_Terms"

scriptPurpose = r"""
This script (Get_Terms) uses the Canvas API to run a terms report and download the .csv result.
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

## Local Path Variables
baseLogPath = f"{PFAbsolutePath}Logs\\{scriptName}\\"
outputPath = f"{PFAbsolutePath}Canvas Resources\\"
configPath = f"{PFAbsolutePath}Configs TLC\\"

## If the base log path doesn't already exist, create it
if not (os.path.exists(baseLogPath)):
    os.makedirs(baseLogPath, mode=0o777, exist_ok=False)

if not (os.path.exists(outputPath)):
    os.makedirs(outputPath, mode=0o777, exist_ok=False)

## Final length of relative Path
relPathLen = len(PFRelativePath)

## Canvas Instance Url
CoreCanvasAPIUrl = None
## Open the Core_Canvas_Url.txt from the config path
with open (f"{configPath}Core_Canvas_Url.txt", "r") as file:
    CoreCanvasAPIUrl = file.readlines()[0]

## If the script is run as main the folder with the access token is in the parent directory

## Open and retrieve the Canvas Access Token
with open (f"{configPath}\Canvas_Access_Token.txt", "r") as file:
    canvasAccessToken = file.readlines()[0]

## Log configurations
logger = logging.getLogger(__name__)
rootFormat = ("%(asctime)s %(levelname)s %(message)s")
FORMAT = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
logging.basicConfig(format=FORMAT, filemode = "a", level=logging.INFO)

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

## The variable below holds a set of the functions that have had errors. This enables the error_handler function to only send
## an error email the first time the function triggeres an error
setOfFunctionsWithErrors = set()

## This function handles function errors
def error_handler (p1_ErrorLocation, p1_ErrorInfo, sendOnce = True):
    functionName = "error_handler"
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

## This function uses the user provided term to determines which Canvas school terms to pull terms from
## with the Canvas Reports API and creates a .csv file of the terms in those Canvas school terms
def createTermsCSV(p1_header, p1_inputTerm, attempt = 0):
    functionName = "createTermsCSV"
    try:
        logger.info (f"     \nStarting {p1_inputTerm} Terms report")
        
        ## Define the target destination for the report
        targetDestination = f"{outputPath}\\Canvas_Terms.csv"
        
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
                return
        
        ## Define and initialize the api url for starting reports
        start_report_API_URL = CoreCanvasAPIUrl + "accounts/1/reports/provisioning_csv"
        
        ##Define and initialize the lists of relavent terms, the IDs of their related reports, and their download urls
        term_report_ID = ""
        term_report_download_url = ""
        
        ## Make an api call to start a provisioning report for each of the relavent terms and append the report id to the term_report_ID list
        logger.info ("\n" + "Calling provisioning terms report")

        ## Initilize payload with one of the relavent terms
        payload_2 = {"parameters[enrollment_term_id]":f"sis_term_id:{p1_inputTerm}", 'parameters[terms]':'true', 'parameters[created_by_sis]':'true'}

        ## Make the API call
        report_object = requests.post(start_report_API_URL, headers = p1_header, params = payload_2)

        ## Convert report_text_jsonObject recieved through the API call in json to a Python Dictionary
        report_text_jsonObject = json.loads(report_object.text)
        term_report_ID = report_text_jsonObject["id"]

        ## Check the status of each report and, if the progress == 100, append the report's download url to term_report_download_url
        logger.info ("\nChecking statuses of provisioning terms reports")

        ## Define the status report the Google api url with the report ID added on the end
        status_report_API_URL = CoreCanvasAPIUrl + "accounts/1/reports/provisioning_csv/" + str(term_report_ID)
        ## Make the status api call
        status_object = requests.get(status_report_API_URL, headers = p1_header)
        ## Convert status_text_jsonObject from json to a Python Dictionary
        status_text_jsonObject = json.loads(status_object.text)
        ## Define relavent report variables
        reportTerm = status_text_jsonObject["parameters"]["enrollment_term_id"][-4:]

        ## Check whether the report has finished and if it isn't, wait 10 seconds 
        ## continue to check until the report shows progress as 100
        while (status_text_jsonObject["progress"] != 100):
            ## Wait 10 seconds
            logger.info ("\n" + f"{p1_inputTerm} Terms report is incomplete. Waiting 10 seconds")
            time.sleep(10)
            
            ## Remake the call
            status_object = requests.get(status_report_API_URL, headers = p1_header)
            
            ## Reinitilize the the status text dictionary so the progress can be checked again
            status_text_jsonObject = json.loads(status_object.text)

        ## If the report failed
        if "parameters" in status_text_jsonObject.keys():
            if "extra_text" in status_text_jsonObject["parameters"].keys():
                if "failed" in status_text_jsonObject["parameters"]["extra_text"]:
                    
                    ## Recursively call this function again unless this is the fifth attempt
                    if attempt != 3:
                        createTermsCSV(p1_header, p1_inputTerm, attempt + 1)
                    return

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
        logger.info ("\nDownloading provisioning terms report")
        downloadFile(term_report_download_url, f"{outputPath}\\Canvas_Terms.csv", "w")

        logger.info (f"     \nSucessfully downloaded {p1_inputTerm} User CSV")

    except Exception as error:
        error_handler (functionName, p1_ErrorInfo = error)

def termGetTerms(inputTerm = ""):
    functionName = "Get_Terms"

    ## Define the API Call header using the retreived Canvas Token
    header = {'Authorization' : f"Bearer {canvasAccessToken}"}

    currentTerm = ""

    if inputTerm:
        currentTerm = inputTerm
    
    else:
        ## January through May is the Spring Term
        if currentMonth >= 1 and currentMonth <= 5:
            currentTerm = f"SP{str(currentYear)[2:]}"

        ## June through August is the Summer Term
        elif currentMonth >= 6 and currentMonth <= 8:
            currentTerm = f"SU{str(currentYear)[2:]}"

        ## The other months (September through December) is the Fall Term
        else:
            currentTerm = f"FA{str(currentYear)[2:]}"

    ## Start and download the Canvas report
    createTermsCSV(p1_header = header, p1_inputTerm = currentTerm)

if __name__ == "__main__":

    ## Define the API Call header using the retreived Canvas Token
    header = {'Authorization' : f"Bearer {canvasAccessToken}"}

    ## Start and download the Canvas report
    createTermsCSV (p1_header = header, p1_inputTerm = input("Enter the desired term in \
four character format (FA20, SU20, SP20): "))

    input("Press enter to exit")