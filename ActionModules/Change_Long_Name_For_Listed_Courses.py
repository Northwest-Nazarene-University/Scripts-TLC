# Author: Bryce Miller - brycezmiller@nnu.edu
# Last Updated by: Bryce Miller

import traceback, os, sys, logging, requests, threading, time, pandas as pd
from datetime import datetime

# Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = "Change_Long_Name_For_Listed_Courses"

scriptPurpose = r"""
This script reads a CSV file containing Canvas course IDs and changes the long name for each course using the Canvas API.
"""
externalRequirements = r"""
To function properly, this script requires a valid access header and URL, and a CSV file named "Target_Canvas_Course_Ids.csv" located in the Canvas Resources directory.
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

## The variable below holds a set of the functions that have had errors. This enables the except function to only send
## an error email the first time the function triggers an error
setOfFunctionsWithErrors = set()

## This function handles function errors
def  except(p1_ErrorLocation, p1_ErrorInfo, sendOnce=True):
    functionName = "except"
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

## This function sets the long name for a course given its Canvas course ID and long name
def setCourseLongName(p1_header, p1_courseId, p1_longName):
    functionName = "setCourseLongName"
    try:
        setLongNameApiUrl = f"{CoreCanvasAPIUrl}courses/{p1_courseId}"
        payload = {"course": {"name": p1_longName}}
        response = makeApiCall(p1_header=p1_header, p1_apiUrl=setLongNameApiUrl, p1_payload=payload, apiCallType="put")

        if response.status_code == 200:
            logger.info(f"Successfully set long name for course with ID: {p1_courseId}")
        else:
            logger.warning(f"Failed to set long name for course with ID: {p1_courseId}. Status code: {response.status_code}")

    except Exception as error:
        except(functionName, error)

## This function reads the CSV file and sets the long name for the listed courses
def setListedCoursesLongName():
    functionName = "setListedCoursesLongName"
    try:
        targetCoursesCsvFilePath = f"{baseInputPath}Target_Canvas_Course_Ids.csv"
        header = {'Authorization': f"Bearer {canvasAccessToken}"}
        
        ## Define the necessary thread list
        ongoingSetLongNameThreads = []

        ## Read the CSV file using pandas
        rawTargetCoursesDf = pd.read_csv(targetCoursesCsvFilePath)

        ## Retain only rows that have a value in canvas_course_id
        targetCoursesDf = rawTargetCoursesDf[rawTargetCoursesDf["canvas_course_id"].notna()]

        ## Iterate over each row in the DataFrame
        for index, row in targetCoursesDf.iterrows():

            ## Get the course id from the row
            courseId = str(row["canvas_course_id"]).replace('.0', '')

            ## Get the long name from the row
            longName = str(row["long_name"])

            ## Create a thread to set the long name for the course
            setLongNameThread = threading.Thread(target=setCourseLongName, args=(header, courseId, longName))

            ## Start the thread
            setLongNameThread.start()

            ## Add the thread to the ongoing set long name threads list
            ongoingSetLongNameThreads.append(setLongNameThread)

            ## Sleep for a short time to avoid overloading the server
            time.sleep(0.1)

        ## Check if all ongoing set long name threads have completed
        for thread in ongoingSetLongNameThreads:
            thread.join()

    except Exception as error:
        except(functionName, error)

if __name__ == "__main__":
    ## Set working directory
    os.chdir(os.path.dirname(__file__))

    ## Set the long name for the listed courses
    setListedCoursesLongName()

    input("Press enter to exit")
