## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

## Import necessary modules
import traceback, os, sys, logging, requests, csv, threading, time, pandas as pd
from datetime import datetime

## Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = "Change_Role_For_Listed_Enrollments"

scriptPurpose = r"""
This script reads a CSV file containing Canvas enrollment IDs and changes the role for each enrollment using the Canvas API.
"""
externalRequirements = r"""
To function properly, this script requires a valid access header and URL, and a CSV file named "Target_Canvas_Enrollment_Ids.csv" located in the Canvas Resources directory.
"""

## Date Variables
currentDate = datetime.now()
## Get the current date and time
currentMonth = currentDate.month
## Get the current month
currentYear = currentDate.year
## Get the current year

## Set working directory
os.chdir(os.path.dirname(__file__))
## Change the working directory to the script's directory

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
## Define the base log path
baseInputPath = f"{pfAbsolutePath}Canvas Resources\\"
## Define the base input path
configPath = f"{pfAbsolutePath}Configs TLC\\"
## Define the config path

## If the base log path doesn't already exist, create it
if not os.path.exists(baseLogPath):
    os.makedirs(baseLogPath, mode=0o777, exist_ok=False)
    ## Create the base log path

## Add Input Modules to the sys path
sys.path.append(f"{pfAbsolutePath}Scripts TLC\\ResourceModules")
## Add ResourceModules to sys path
sys.path.append(f"{pfAbsolutePath}Scripts TLC\\ActionModules")
## Add ActionModules to sys path

## Import local modules
from Error_Email_API import errorEmailApi
## Import ErrorEmailApi
from Make_Api_Call import makeApiCall
## Import makeApiCall

## Canvas Instance Url
coreCanvasApiUrl = None
## Open the Core_Canvas_Url.txt from the config path
with open(f"{configPath}Core_Canvas_Url.txt", "r") as file:
    coreCanvasApiUrl = file.readlines()[0]
    ## Read the Canvas URL

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

## The variable below holds a set of the functions that have had errors. This enables the errorHandler function to only send
## an error email the first time the function triggers an error
setOfFunctionsWithErrors = set()

## This function handles function errors
def errorHandler(p1_errorLocation, p1_errorInfo, sendOnce=True):
    functionName = "errorHandler"
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

## This function deletes an enrollment given its Canvas enrollment ID
def deleteEnrollment(p1_header, p3_courseId, p1_enrollmentId):
    functionName = "deleteEnrollment"
    try:

        ## Define the API URL for deleting the enrollment
        deleteEnrollmentUrl = f"{coreCanvasApiUrl}courses/{p3_courseId}/enrollments/{p1_enrollmentId}"

        ## Define the API URL for deleting the enrollment
        response = makeApiCall(p1_header=p1_header, p1_apiUrl=deleteEnrollmentUrl, apiCallType="delete")

        ## Make the API call to delete the enrollment
        if response.status_code == 200:
            logger.info(f"Successfully deleted enrollment with ID: {p1_enrollmentId}")
        else:
            logger.warning(f"Failed to delete enrollment with ID: {p1_enrollmentId}. Status code: {response.status_code}")

    except Exception as error:
        errorHandler(functionName, error)

## This function re-enrolls a user with a new role given the Canvas user ID, course ID, role ID, and base role type
def reEnrollUser(p1_header, p1_userId, p2_courseId, p3_roleId, p4_baseRoleType):
    functionName = "reEnrollUser"
    try:

        ## Define the API URL for re-enrolling the user
        reEnrollUrl = f"{coreCanvasApiUrl}courses/{p2_courseId}/enrollments"

        ## Define the API URL for re-enrolling the user
        payload = {"enrollment[user_id]": p1_userId
                   , "enrollment[type]": p4_baseRoleType
                   , "enrollment[role_id]": p3_roleId
                   , "enrollment[enrollment_state]": "active"
                   }

        ## Define the payload
        response = makeApiCall(p1_header=p1_header, p1_apiUrl=reEnrollUrl, p1_payload=payload, apiCallType="post")

        ## Make the API call to re-enroll the user
        if response.status_code == 200:
            logger.info(f"Successfully re-enrolled user with ID: {p1_userId} in course with ID: {p2_courseId} with role ID: {p3_roleId}")
        else:
            logger.warning(f"Failed to re-enroll user with ID: {p1_userId} in course with ID: {p2_courseId}. Status code: {response.status_code}")

    except Exception as error:
        errorHandler(functionName, error)

## This function deletes the enrollment and re-enrolls the user with the new role
def deleteAndReenroll(p1_header, p1_enrollmentId, p1_userId, p2_courseId, p3_roleId, p4_baseRoleType):
    reEnrollUser(p1_header, p1_userId, p2_courseId, p3_roleId, p4_baseRoleType)
    deleteEnrollment(p1_header, p2_courseId, p1_enrollmentId)

## This function reads the CSV file, deletes the enrollment, and re-enrolls the user with the new role
def changeListedEnrollmentsRole():
    functionName = "changeListedEnrollmentsRole"
    try:
        targetEnrollmentsCsvFilePath = f"{baseInputPath}Target_Canvas_Enrollment_Ids.csv"
        ## Define the CSV file path
        header = {'Authorization': f"Bearer {canvasAccessToken}"}
        ## Define the header

        ## Define the necessary thread list
        ongoingChangeRoleThreads = []

        ## Read the CSV file using pandas
        rawTargetEnrollmentsDf = pd.read_csv(targetEnrollmentsCsvFilePath)

        ## Retain only rows that have a value in canvas_enrollment_id
        targetEnrollmentsDf = rawTargetEnrollmentsDf[rawTargetEnrollmentsDf["canvas_enrollment_id"].notna()]

                ## Iterate over each row in the DataFrame
        for index, row in targetEnrollmentsDf.iterrows():

            ## Get the enrollment id from the row
            enrollmentId = str(row["canvas_enrollment_id"]).replace('.0', '')

            ## Get the user id from the row
            userId = str(row["canvas_user_id"]).replace('.0', '')
            
            ## Get the course id from the row
            courseId = str(row["canvas_course_id"]).replace('.0', '')

            ## Get the role id from the row
            roleId = str(row["role_id"]).replace('.0', '')

            ## Get the base role type from the row
            baseRoleType = str(row["base_role_type"])

            ## Create a thread to delete the enrollment and re-enroll the user
            changeRoleThread = threading.Thread(target=deleteAndReenroll, args=(header, enrollmentId, userId, courseId, roleId, baseRoleType))

            ## Start the thread
            changeRoleThread.start()

            ## Add the thread to the ongoing change role threads list
            ongoingChangeRoleThreads.append(changeRoleThread)

            ## Sleep for a short time to avoid overloading the server
            time.sleep(0.2)

        ## Check if all ongoing change role threads have completed
        for thread in ongoingChangeRoleThreads:
            thread.join()

    except Exception as error:
        errorHandler(functionName, error)

if __name__ == "__main__":
    ## Set working directory
    os.chdir(os.path.dirname(__file__))
    
    ## Change the role for the listed enrollments
    changeListedEnrollmentsRole()

    ## Wait for user input to exit
    input("Press enter to exit")
