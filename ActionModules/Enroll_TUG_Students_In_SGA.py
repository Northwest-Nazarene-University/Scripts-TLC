## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

## Import necessary modules
import traceback, os, sys, logging, requests, csv, threading, time, pandas as pd, re
from datetime import datetime, date

## Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = "Enroll_TUG_Students_In_SGA"

scriptPurpose = r"""
This script reads a CSV file containing Canvas enrollment IDs and changes the role for each enrollment using the Canvas API.
"""
externalRequirements = r"""
To function properly, this script requires a valid access header and URL, and a CSV file named "Target_Canvas_Enrollment_Ids.csv" located in the Canvas Resources directory.
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
configPath = f"{pfAbsolutePath}Configs TLC\\"

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
        errorEmailApi.sendEmailError(p2_ScriptName=scriptName, p2_ScriptPurpose=scriptPurpose,
                                     p2_ExternalRequirements=externalRequirements,
                                     p2_ErrorLocation=p1_errorLocation, p2_ErrorInfo=p1_errorInfo)
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
        error_handler(functionName, error)

## This function enrolls a user with a new role given the Canvas user ID, course ID, role ID, and base role type
def reEnrollUser(p1_header, p1_userId, p2_courseId, p3_roleId, p4_baseRoleType):
    functionName = "reEnrollUser"
    try:

        ## Define the API URL for enrolling the user
        reEnrollUrl = f"{coreCanvasApiUrl}courses/{p2_courseId}/enrollments"

        ## Define the API URL for enrolling the user
        payload = {"enrollment[user_id]": p1_userId
                   , "enrollment[type]": p4_baseRoleType
                   , "enrollment[role_id]": p3_roleId
                   , "enrollment[enrollment_state]": "active"
                   }

        ## Define the payload
        response = makeApiCall(p1_header=p1_header, p1_apiUrl=reEnrollUrl, p1_payload=payload, apiCallType="post")

        ## Make the API call to enroll the user
        if response.status_code == 200:
            logger.info(f"Successfully enrolled user with ID: {p1_userId} in course with ID: {p2_courseId} with role ID: {p3_roleId}")
        else:
            logger.warning(f"Failed to enroll user with ID: {p1_userId} in course with ID: {p2_courseId}. Status code: {response.status_code}")

    except Exception as error:
        error_handler(functionName, error)

## This function deletes the enrollment and enrolls the user with the new role
def deleteAndReenroll(p1_header, p1_enrollmentId, p1_userId, p2_courseId, p3_roleId, p4_baseRoleType):
    reEnrollUser(p1_header, p1_userId, p2_courseId, p3_roleId, p4_baseRoleType)
    deleteEnrollment(p1_header, p2_courseId, p1_enrollmentId)

## This function reads the CSV file, deletes the enrollment, and enrolls the user with the new role
def enrollTugStudentsInSga(inputTerm):

    functionName = "enrollTugStudentsInSga"

    try:

        ## Determine and save the term's school year
        schoolYear = None
        if re.search("AF|FA|GF", inputTerm):
            ## Fall terms are the first terms of a new school year so FA20 is part of the 2020-21 school year.
            schoolYear = (century + inputTerm[2:] + "-" + str(int(inputTerm[2:]) + 1))
        elif re.search("SP|GS|AS|SG|SA|SU", inputTerm):
            ## Spring and Summer terms belong in the same school year as the fall terms before them, so SP21 is part of the same 2020-21 school year as FA20.
            schoolYear = (century + str(int(inputTerm[2:]) - 1) + "-" + inputTerm[2:])

        ## Create the school year relavent input path
        schoolYearOutputPath = f"{baseLocalInputPath}\\{schoolYear}\\"
        
        ## Define the term specific input path
        termOutputPath = f"{schoolYearOutputPath}{inputTerm}\\"

        ## Read the input term's TUG student csv into a df
        tugStudentsDf = pd.read_csv(f"{termGetTugStudents(inputTerm)}")

        ## Retrieve (and update if neccessary) the term relavent canvas courses file path
        SGACourseTermLocationDf = pd.read_csv(termGetCourses("All"))

        ## Find the "canvas_course_id" for the SGA course by looking for the target SGA sis id in the course short name
        targetOrientationCanvasCourseId = SGACourseTermLocationDf.loc[SGACourseTermLocationDf['short_name'] == "SGA", 'canvas_course_id'].values[0]
         
        ## Define the SGA course's base api url
        SGACourseCoreApiUrl = f"{coreCanvasApiUrl}courses/{targetOrientationCanvasCourseId}"

        ## Define the SGA courses users api url
        SGACourseUsersApiUrl = f"{SGACourseCoreApiUrl}/users"

        ## Define the payload to get the course's students
        SGACourseUserPayload = {"enrollment_type[]":["student"], "include[]": "enrollments", "per_page": 100}

        ## Make the API call to get the course's details
        SGACourseEnrollmentObjectOrObjectList = makeApiCall(p1_apiUrl = SGACourseUsersApiUrl, p1_payload = SGACourseUserPayload)

        ## Make a list to hold the target orientation students
        targetCourseEnrolledStudentsDict = {}

        ## If the SGACourseEnrollmentObjectOrObjectList is a list
        if isinstance(SGACourseEnrollmentObjectOrObjectList, list):

            ## For each json api object in the course's enrollment objects list
            for enrollmentsObject in SGACourseEnrollmentObjectOrObjectList:
                
                ## For each student within the text (dict) of the object
                for studentObject in enrollmentsObject.json():

                    ## Add the student's sis_user_id and the target student's SGA enrollment id to the targetCourseEnrolledStudentsDict
                    targetCourseEnrolledStudentsDict[studentObject["sis_user_id"]] = studentObject['enrollments'][0]["id"]
        
        ## If the SGACourseEnrollmentObjectOrObjectList is not a list, There was just one object returned
        else:
            
             ## For each student within the text (dict) of the object
                for studentObject in SGACourseEnrollmentObjectOrObjectList.json():

                    ## Define a variable to hold the student's enrollment id
                    targetStudentsSgaEnrollmentId = None

                    ## For each enrollment in the student object's enrollments list
                    for enrollment in studentObject["enrollments"]:

                        ## If the course id of the enrollment matches the target orientation course id
                        if enrollment["course_id"] == targetOrientationCanvasCourseId:

                            ## Set the target student's SGA enrollment id to the enrollment's id
                            targetStudentsSgaEnrollmentId = enrollment["id"]

                    ## Add the student's sis_user_id and the target student's SGA enrollment id to the targetCourseEnrolledStudentsDict
                    targetCourseEnrolledStudentsDict[studentObject["sis_user_id"]] = targetStudentsSgaEnrollmentId

        ## For each student in the targetCourseEnrolledStudentsDict
        for studentId, enrollmentID in targetCourseEnrolledStudentsDict.items():

            ## If the student is not in the tugStudentsDf
            if int(studentId) not in tugStudentsDf['user_id'].values:
                
                ## Create the deletion api url by adding the enrollment id to the end of the stuCourseEnrollmentApiUrl
                stuCourseEnrollmentDeletionApiUrl = f"{coreCanvasApiUrl}courses/{targetOrientationCanvasCourseId}/enrollments/{enrollmentID}"

                ## Defeine the parameter to delete the enrollment
                stuCourseEnrollmentDeleteParams = {
                    "task": "delete"
                }

                ## Make a delete enrollment api call to remove the reactivated enrollment
                enrollmentDeletionApiOjbect = makeApiCall(p1_apiUrl = stuCourseEnrollmentDeletionApiUrl, p1_payload = stuCourseEnrollmentDeleteParams, apiCallType = "delete")

                ## Define a deletion attempt variable
                enrollmentDeletionAttempt = 1

                ## If the enrollment deletion api call was not successful
                while enrollmentDeletionApiOjbect.status_code != 200 and enrollmentDeletionAttempt != 5:

                    ## Sleep 3 seconds
                    time.sleep(3)

                    ## Log a warning that the enrollment deletion failed
                    logger.warning(f"Enrollment deletion failed in the SGA course for student {studentId}")

                    #try to remove the reactiviated enrollment again
                    enrollmentDeletionApiOjbect = makeApiCall(p1_apiUrl = stuCourseEnrollmentDeletionApiUrl, p1_payload = stuCourseEnrollmentDeleteParams, apiCallType = "delete")

                    ## Increment the attempt number
                    enrollmentDeletionAttempt += 1

        ## Define the SGA courses's enrollment API URL
        SGACourseUsersApiUrl = f"{SGACourseCoreApiUrl}/enrollments"

        ## For each student in the tugStudentsDf
        for index, studentRow in tugStudentsDf.iterrows():

            ## Define the payload to enroll the student in the SGA course
            reEnrollPayload = {
                "enrollment[user_id]": studentRow['canvas_user_id'],
                "enrollment[type]": "StudentEnrollment",
                "enrollment[enrollment_state]": "active"
            }

            ## If the student is not already enrolled in the SGA course
            if str(studentRow['user_id']) not in targetCourseEnrolledStudentsDict.keys():

                ## Make a post api call to enroll the student in the SGA course
                reEnrollApiObject = makeApiCall(p1_apiUrl=SGACourseUsersApiUrl, p1_payload=reEnrollPayload, apiCallType="post")

                ## If the enrollment was successful
                if reEnrollApiObject.status_code == 200:
                    logger.info(f"Successfully enrolled student {studentRow['user_id']} in the SGA course")
                else:
                    logger.warning(f"Failed to enroll student {studentRow['user_id']} in the SGA course. Status code: {reEnrollApiObject.status_code}")

        

    except Exception as error:
        error_handler(functionName, error)

if __name__ == "__main__":
    ## Set working directory
    os.chdir(os.path.dirname(__file__))
    
    ## Change the role for the listed enrollments
    enrollTugStudentsInSga(inputTerm = input("Enter the desired term in \
four character format (FA20, SU20, SP20): "))

    ## Wait for user input to exit
    input("Press enter to exit")
