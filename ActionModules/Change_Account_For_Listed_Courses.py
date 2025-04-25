# Author: Bryce Miller - brycezmiller@nnu.edu
# Last Updated by: Bryce Miller

import traceback, os, sys, logging, requests, csv, threading, time, pandas as pd
from datetime import datetime

# Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = "Change_Account_For_Listed_Courses"

scriptPurpose = r"""
This script reads a CSV file containing Canvas course IDs and changes the account for each course using the Canvas API.
"""
externalRequirements = r"""
To function properly, this script requires a valid access header and URL, and a CSV file named "courses_to_set_account.csv" located in the Canvas Resources directory.
"""

## Date Variables
currentDate = datetime.now()
currentMonth = currentDate.month
currentYear = currentDate.year

## Set working directory
os.chdir(os.path.dirname(__file__))

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
baseInputPath = f"{PFAbsolutePath}Canvas Resources\\"
configPath = f"{PFAbsolutePath}Configs TLC\\"

## If the base log path doesn't already exist, create it
if not os.path.exists(baseLogPath):
    os.makedirs(baseLogPath, mode=0o777, exist_ok=False)

## Add Input Modules to the sys path
sys.path.append(f"{PFAbsolutePath}Scripts TLC\\ResourceModules")
sys.path.append(f"{PFAbsolutePath}Scripts TLC\\ActionModules")

## Import local modules
from Error_Email_API import errorEmailApi  # Import errorEmailApi
from Make_Api_Call import makeApiCall  # Import makeApiCall

## Canvas Instance Url
CoreCanvasAPIUrl = None
## Open the Core_Canvas_Url.txt from the config path
with open(f"{configPath}Core_Canvas_Url.txt", "r") as file:
    CoreCanvasAPIUrl = file.readlines()[0]

## If the script is run as main the folder with the access token is in the parent directory
canvasAccessToken = ""

## Open and retrieve the Canvas Access Token
with open(f"{configPath}Canvas_Access_Token.txt", "r") as file:
    canvasAccessToken = file.readlines()[0]

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
def error_handler(p1_ErrorLocation, p1_ErrorInfo, sendOnce=True):
    functionName = "error_handler"
    logger.error(f"\nA script error occurred while running {p1_ErrorLocation}. Error: {str(p1_ErrorInfo)}")

    ## If the function with the error has not already been processed send an email alert
    if p1_ErrorLocation not in setOfFunctionsWithErrors:
        errorEmailApi.sendEmailError(p2_ScriptName=scriptName, p2_ScriptPurpose=scriptPurpose,
                                     p2_ExternalRequirements=externalRequirements,
                                     p2_ErrorLocation=p1_ErrorLocation, p2_ErrorInfo=p1_ErrorInfo)
        setOfFunctionsWithErrors.add(p1_ErrorLocation)
        logger.error(f"\nError Email Sent")
    else:
        logger.error(f"\nError email already sent")

## This function changes the account for a course given its Canvas course ID and account ID
def changeCourseAccount(p1_header, courseId, account_id):
    functionName = "changeCourseAccount"
    try:
        change_account_url = f"{CoreCanvasAPIUrl}courses/{courseId}"
        payload = {"course": {"account_id": account_id}}
        response = makeApiCall(p1_header=p1_header, p1_apiUrl=change_account_url, p1_payload=payload, apiCallType="put")

        if response.status_code == 200:
            logger.info(f"Successfully changed account for course with ID: {courseId}")
        else:
            logger.warning(f"Failed to change account for course with ID: {courseId}. Status code: {response.status_code}")

    except Exception as error:
        error_handler(functionName, error)

## This function reads the CSV file and changes the account for the listed courses
def changeListedCoursesAccount():
    functionName = "changeListedCoursesAccount"
    try:
        targetCoursesCsvFilePath = f"{baseInputPath}Target_Canvas_Course_Ids.csv"
        header = {'Authorization': f"Bearer {canvasAccessToken}"}

        ## Define the necessary thread list
        ongoingChangeAccountThreads = []

        ## Read the CSV file using pandas
        rawTargetCoursesDf = pd.read_csv(targetCoursesCsvFilePath)

        ## Retain only rows that have a value in canvas_course_id
        targetCoursesDf = rawTargetCoursesDf[rawTargetCoursesDf["canvas_course_id"].notna()]

        ## Iterate over each row in the DataFrame
        for index, row in targetCoursesDf.iterrows():

            ## Get the course id from the row
            courseID = str(row["canvas_course_id"]).replace('.0', '')

            ## Get the term id from the row
            accountID = str(row["canvas_account_id"]).replace('.0', '')

            ## Create a thread to change the account for the course
            changeAccountThread = threading.Thread(target=changeCourseAccount, args=(header, courseID, accountID))

            ## Start the thread
            changeAccountThread.start()

            ## Add the thread to the ongoing change account threads list
            ongoingChangeAccountThreads.append(changeAccountThread)

            ## Sleep for a short time to avoid overloading the server
            time.sleep(0.1)

        ## Check if all ongoing change account threads have completed
        for thread in ongoingChangeAccountThreads:
            thread.join()

    except Exception as error:
        error_handler(functionName, error)

if __name__ == "__main__":
    ## Set working directory
    os.chdir(os.path.dirname(__file__))

    ## Change the account for the listed courses
    changeListedCoursesAccount()

    input("Press enter to exit")