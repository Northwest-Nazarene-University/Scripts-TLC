# Author: Bryce Miller - brycezmiller@nnu.edu
# Last Updated by: Bryce Miller

import traceback, os, sys, logging, requests, threading, time, pandas as pd
from datetime import datetime

# Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = "Count_Respondus_Quizzes"

scriptPurpose = r"""
This script counts the number of Respondus quizzes and the number of unique students who have submitted to these quizzes in Canvas courses.
"""
externalRequirements = r"""
To function properly, this script requires a valid access header and URL, and a CSV file named "courses_to_check.csv" located in the Canvas Resources directory.
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
baseLocalInputPath = f"{PFAbsolutePath}Canvas Resources\\"
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

## This function counts Respondus quizzes and students for a given course
def countRespondusQuizzes(p1_header, p1_courseId, result_dict):
    functionName = "countRespondusQuizzes"
    try:
        quizzes_count = 0
        students_count = set()

        ## Get the assignments for the course
        assignments_url = f"{coreCanvasApiUrl}courses/{p1_courseId}/assignments"
        courseAssignmentsParams = {'search_term': 'Respondus', 'include[]': 'submission'}
        response = makeApiCall(p1_header=p1_header, p1_apiUrl=assignments_url, p1_payload=courseAssignmentsParams, apiCallType="get")

        ## If the response is successful
        if response.status_code == 200:

            ## Convert the response to JSON
            assignments = response.json()

            ## For each assignment in the course
            for assignment in assignments:

                ## If the assignment name contains "Respondus"
                if "Respondus" in assignment['name']:

                    ## Save the assignment ID
                    assignment_id = assignment['id']

                    ## Save the assignment URL
                    assignment_details_url = f"{coreCanvasApiUrl}courses/{p1_courseId}/assignments/{assignment_id}"

                    ## Make an API call to get the assignment details
                    assignment_response = makeApiCall(p1_header=p1_header, p1_apiUrl=assignment_details_url, apiCallType="get")

                    ## If the response is successful
                    if assignment_response.status_code == 200:

                        ## Convert the response to JSON
                        assignment_details = assignment_response.json()

                        ## If the assignment is published and has submitted submissions
                        if assignment_details['published'] and assignment_details['has_submitted_submissions']:
                            quizzes_count += 1

                            ## If the length of students_count is 0
                            if len(students_count) == 0:

                                ## Define an api url to get the course's enrollments
                                enrollments_url = f"{coreCanvasApiUrl}courses/{p1_courseId}/enrollments"

                                 ## Define a payload to get only student enrollments
                                enrollments_params = {'type[]': 'StudentEnrollment'}

                                ## Make an API call to get the course's enrollments
                                enrollments_response = makeApiCall(p1_header=p1_header, p1_apiUrl=enrollments_url, p1_payload=enrollments_params, apiCallType="get")

                                ## If the response is successful
                                if enrollments_response.status_code == 200:

                                    ## Convert the response to JSON
                                    enrollments = enrollments_response.json()

                                    ## For each enrollment in the course
                                    for enrollment in enrollments:

                                        ## If the enrollment is active
                                        if enrollment['enrollment_state'] == 'active':

                                            ## Get the user ID from the enrollment
                                            user_id = enrollment['user_id']

                                            ## Add the user ID to the students_count set
                                            students_count.add(user_id)

        else:
            logger.warning(f"Failed to get assignments for course with ID: {p1_courseId}. Status code: {response.status_code}")

        result_dict[p1_courseId] = (quizzes_count, students_count)

    except Exception as error:
        except(functionName, error)
        #result_dict[p1_courseId] = (0, 0)

## This function reads the CSV file and counts Respondus quizzes and students for the listed courses
def countListedCoursesRespondusQuizzes():
    functionName = "countListedCoursesRespondusQuizzes"
    try:
        targetCoursesCsvFilePath = f"{baseLocalInputPath}Target_Canvas_Course_Ids.csv"
        header = {'Authorization': f"Bearer {canvasAccessToken}"}
        
        ## Define the necessary thread list
        ongoingCountThreads = []
        result_dict = {}

        ## Read the CSV file using pandas
        rawTargetCoursesDf = pd.read_csv(targetCoursesCsvFilePath)

        ## Retain only rows that have a value in canvas_course_id
        targetCoursesDf = rawTargetCoursesDf[rawTargetCoursesDf["canvas_course_id"].notna()]

        ## Iterate over each row in the DataFrame
        for index, row in targetCoursesDf.iterrows():

            ## Get the course id from the row
            courseId = str(row["canvas_course_id"]).replace('.0', '')

            ## Create a thread to count Respondus quizzes for the course
            countThread = threading.Thread(target=countRespondusQuizzes, args=(header, courseId, result_dict))

            ## Start the thread
            countThread.start()

            ## Add the thread to the ongoing count threads list
            ongoingCountThreads.append(countThread)

            ## Sleep for a short time to avoid overloading the server
            time.sleep(0.1)

        ## Check if all ongoing count threads have completed
        for thread in ongoingCountThreads:
            thread.join()

        total_quizzes = sum(result[0] for result in result_dict.values())
        total_students = set()
        for result in result_dict.values():
            total_students.update(result[1])

        logger.info(f"Total Respondus quizzes: {total_quizzes}")
        logger.info(f"Total unique students: {len(total_students)}")

    except Exception as error:
        except(functionName, error)

if __name__ == "__main__":
    ## Set working directory
    os.chdir(os.path.dirname(__file__))

    ## Count Respondus quizzes for the listed courses
    countListedCoursesRespondusQuizzes()

    input("Press enter to exit")
