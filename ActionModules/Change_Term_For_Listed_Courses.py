## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

import traceback, os, sys, logging, requests, pandas as pd
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from datetime import datetime

## Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = "Change_Term_For_Listed_Courses"

scriptPurpose = r"""
This script reads a CSV file containing Canvas course IDs and changes the term for each course using the Canvas API.
"""
externalRequirements = r"""
To function properly, this script requires a valid access header and URL, and a CSV file named "courses_to_set_term.csv" located in the Canvas Resources directory.
"""

## Date Variables
currentDateTime = datetime.now()
currentMonth = currentDateTime.month
currentYear = currentDateTime.year

## Set working directory
os.chdir(os.path.dirname(__file__))

## Relative Path (this changes depending on the working directory of the main script)
PFRelativePath = r".\\"

## If the Canvas directory is not in the folder the relative path points to
## find the Canvas directory and set the relative path to its parent folder
while "Scripts TLC" not in os.listdir(PFRelativePath):
    PFRelativePath = f"..\\{PFRelativePath}"

## Change the relative path to an absolute path
absolutePath = f"{os.path.abspath(PFRelativePath)}\\"

## Local Path Variables
baseLogPath = f"{absolutePath}Logs\\{scriptName}\\"
baseLocalInputPath = f"{absolutePath}Canvas Resources\\"
configPath = f"{absolutePath}Configs TLC\\"

## If the base log path doesn't already exist, create it
if not os.path.exists(baseLogPath):
    os.makedirs(baseLogPath, mode=0o777, exist_ok=False)

## Add Input Modules to the sys path
sys.path.append(f"{absolutePath}Scripts TLC\\ResourceModules")
sys.path.append(f"{absolutePath}Scripts TLC\\ActionModules")

## Import local modules
from TLC_Common import makeApiCall  ## Import makeApiCall

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

## Log configurations
logger = logging.getLogger(__name__)
rootFormat = ("%(asctime)s %(levelname)s %(message)s")
FORMAT = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
logging.basicConfig(format=rootFormat, filemode="a", level=logging.INFO)

## Local setup shim for compatibility with shared modules
class _LoggerShim:
    def __init__(self, p_logger):
        self.logger = p_logger
localSetup = _LoggerShim(logger)
setOfFunctionsWithErrors = set()

## Info Log Handler
infoLogFile = f"{baseLogPath}\\Info Log.txt"
logInfo = logging.FileHandler(infoLogFile, mode='a')
logInfo.setLevel(logging.INFO)
logInfo.setFormatter(FORMAT)
localSetup.logger.addHandler(logInfo)

## Warning Log handler
warningLogFile = f"{baseLogPath}\\Warning Log.txt"
logWarning = logging.FileHandler(warningLogFile, mode='a')
logWarning.setLevel(logging.WARNING)
logWarning.setFormatter(FORMAT)
localSetup.logger.addHandler(logWarning)

## Error Log handler
errorLogFile = f"{baseLogPath}\\Error Log.txt"
logError = logging.FileHandler(errorLogFile, mode='a')
logError.setLevel(logging.ERROR)
logError.setFormatter(FORMAT)
localSetup.logger.addHandler(logError)

## This function handles function errors
def errorHandler(p1_ErrorLocation, p1_errorInfo, sendOnce=True):
    functionName = "errorHandler"
    localSetup.logger.error(f"\nA script error occurred while running {p1_ErrorLocation}. Error: {str(p1_errorInfo)}")

    ## Only log once per function
    if p1_ErrorLocation not in setOfFunctionsWithErrors:
        setOfFunctionsWithErrors.add(p1_ErrorLocation)
        localSetup.logger.error(f"\nError logged for {p1_ErrorLocation}")
    else:
        localSetup.logger.error(f"\nError already logged for {p1_ErrorLocation}")

## This function sets the term for a course given its Canvas course ID and term ID
def setCourseTerm(p1_header, p1_courseId, p1_termId):
    functionName = "setCourseTerm"
    try:
        set_term_url = f"{coreCanvasApiUrl}courses/{p1_courseId}"
        payload = {"course": {"enrollment_term_id": p1_termId}}
        response, _ = makeApiCall(localSetup, p1_header=p1_header, p1_apiUrl=set_term_url, p1_payload=payload, p1_apiCallType="put")

        if response.status_code == 200:
            localSetup.logger.info(f"Successfully set term for course with ID: {p1_courseId}")
        else:
            localSetup.logger.warning(f"Failed to set term for course with ID: {p1_courseId}. Status code: {response.status_code}")

    except Exception as Error:
        errorHandler(functionName, Error)

## This function reads the CSV file and sets the term for the listed courses
def setListedCoursesTerm():
    functionName = "setListedCoursesTerm"
    try:
        targetCoursesCsvFilePath = f"{baseLocalInputPath}Target_Canvas_Course_Ids.csv"
        header = {'Authorization': f"Bearer {canvasAccessToken}"}
        
        ## Read the CSV file using pandas
        rawTargetCoursesDf = pd.read_csv(targetCoursesCsvFilePath)

        ## Retain only rows that have a value in canvas_course_id
        targetCoursesDf = rawTargetCoursesDf[rawTargetCoursesDf["canvas_course_id"].notna()]

        ## Process each course in a thread pool
        MAX_WORKERS = 25
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for index, row in targetCoursesDf.iterrows():

                ## Get the course id from the row
                courseId = str(row["canvas_course_id"]).replace('.0', '')

                ## Get the term id from the row
                termId = str(row["canvas_term_id"]).replace('.0', '')

                ## Submit the task to the thread pool
                executor.submit(setCourseTerm, header, courseId, termId)

    except Exception as Error:
        errorHandler(functionName, Error)

if __name__ == "__main__":
    ## Set working directory
    os.chdir(os.path.dirname(__file__))

    ## Set the term for the listed courses
    setListedCoursesTerm()

    input("Press enter to exit")
