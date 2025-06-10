# Author: Bryce Miller - brycezmiller@nnu.edu
# Last Updated by: Bryce Miller

from datetime import datetime
import paramiko, traceback,  os, logging, sys, requests, json, re, threading, time
import pandas as pd #External Download from https://pypi.org/project/pandas/
import numpy as np

# Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = "Incoming Student Report"

scriptPurpose = r"""
This script (Incoming_Student_Report) connects to the NNU's primary Canvas instance and gets the last login date
for the students on Slates Incoming student list (retrieved through the Get_Slate_Info script)
"""
externalRequirements = r"""
To function properly this script requires access to the institutions Canvas instance via an Active Canvas Bearer Token
"""

## Date Variables
currentDate = datetime.now()
currentDateDatetime = datetime.now().date()
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

## Import local modules
from Error_Email_API import errorEmailApi
from Get_Courses import termGetCourses
from Get_Slate_Info import getSlateInfo

## Local Path Variables
baseLogPath = f"{PFAbsolutePath}Logs\\{scriptName}\\"
baseLocalInputPath = f"{PFAbsolutePath}Slate Resources\\"  ## This is only the base path as the real path requires the requested term
configPath = f"{PFAbsolutePath}\\Configs TLC\\"

## External Path Variables

## Define a variable to hold the base external input path which is where the sis input files are stored
baseExternalInputPath = None 
## Open Base_External_Paths.json from the config path and get the baseExternalInputPath value
with open (f"{configPath}Base_External_Paths.json", "r") as file:
    fileJson = json.load(file)
    baseExternalInputPath = fileJson["baseExternalInputPath"]

## If the base log path doesn't already exist, create it
if not (os.path.exists(baseLogPath)):
    os.makedirs(baseLogPath, mode=0o777, exist_ok=False)

## Canvas Instance Url
CoreCanvasAPIUrl = None
## Open the Core_Canvas_Url.txt from the config path
with open (f"{configPath}Core_Canvas_Url.txt", "r") as file:
    CoreCanvasAPIUrl = file.readlines()[0]

## If the script is run as main the folder with the access token is in the parent directory
canvasAccessToken = ""

## Open and retrieve the Canvas Access Token
with open (f"{configPath}Canvas_Access_Token.txt", "r") as file:
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


## This function 
def getTargetIncomingStudentInfo (row
                            , rowIndex
                            , p1_targetOrientation
                            , p1_targetOrientationStudents
                            , p1_targetOrientationSections
                            , p1_targetOrientationFinalQuizSubmissionDatesAndIds
                            , p1_header
                            , targetSlateDataDF
                            , p1_sisFeedCoursesDf
                            , p1_sisFeedEnrollmentDf
                            , p1_enrollmentDataActivityDF
                            ):
    functionName = "Get Incoming Student Info"

    try:
        
        indivigualOrientationDepartments = ["Business", "Education", "Theology", "Social Work", "Accounting", "Divinity", "Pastoral", "Spiritual", "Missional", "Family Ministry:"]
        indivigualOrientationDepartmentExceptions = ["Christian Ministry:", " AA", "Liberal Studies", "Nursing"]
    
        ## Create a variable to track whether the user is already enrolled in a orientation course
        userEnrolledInOrientation = False
        
        ## Get the student ID and major from the row
        studentSisId = row['StudentID']
        studentMajor = row['Major_Name']
        studentSectionSisId = f"{p1_targetOrientation}: {studentMajor}"

        ## Check if the user is has a major that has its own orientation
        if "GPS" in p1_targetOrientation:
            departmentException = False
            for major in indivigualOrientationDepartmentExceptions:
                if major in studentMajor:
                    departmentException = True
            for major in indivigualOrientationDepartments:
                if major in studentMajor and not departmentException:
                    userEnrolledInOrientation = True
                    break

        ## Get the user's username, email, and last login date
        ## Define the url to get the user data
        userApiUrl = f"{CoreCanvasAPIUrl}users/sis_user_id:{studentSisId}"

        ## Define the playload for the user payload
        userPayload = {"include[]":["last_login"]}

        ## Define a variable to hold the user's Canvas id
        userCanvasId = None

        ## Make a user api call
        userObject = requests.get(userApiUrl, headers = p1_header, params = userPayload)

        if (userObject.status_code == 200):

            ## Turn the user api result into a dictionary
            user_jsonString = userObject.text
            user_jsonObject = json.loads(user_jsonString)

            ## Save the users canvas id
            userCanvasId = user_jsonObject["id"]

            ## Save the user's email
            userEmail = user_jsonObject["email"]

            ## If there is not a user email but the login_id has @ in it
            if not userEmail and "@" in user_jsonObject["login_id"]: 

                ## Record the login_id as the email
                userEmail = user_jsonObject["login_id"]

            ## If they have an nnu email, record their username, and their last login date if it isn't blank
            if userEmail and "nnu.edu" in userEmail:
                userUsername = user_jsonObject["login_id"]
                userLastLoginDate = user_jsonObject["last_login"]

                targetSlateDataDF.loc[rowIndex, "NNU Username"] = userUsername
                targetSlateDataDF.loc[rowIndex, "Last Login Date"] = userLastLoginDate
                if userLastLoginDate:
                    targetSlateDataDF.loc[rowIndex, "Last Login Date Yes/No"] = "Yes"

                logger.info (f"{studentSisId} data saved")

            ## Otherwise
            else:

                ## The user does not have a valid nnu email/username, so return
                logger.info (f"{studentSisId} does not have a valid NNU email/username")
                return

        else:
            logger.info (f"{studentSisId} not in Canvas")
            return

        ## Define a variable to track whether the user is already enrolled in the target orientation
        if str(studentSisId) in p1_targetOrientationStudents:
            userEnrolledInOrientation = True

        ## Enroll the student in the relavent orientation if they are not already enrolled in one
        if userEnrolledInOrientation == False:

            ## Define a variable to track whether the section for the student's major already exists
            majorSectionExists = False

            ## Look for the major in the names of the existing sections to determine if it 
            for section in p1_targetOrientationSections:
                if studentMajor in section["name"]:
                    majorSectionExists = True
                    break

            ##If it a section for that major doesn't already exists
            if not majorSectionExists:
                createSectionApiUrl = f"{CoreCanvasAPIUrl}courses/sis_course_id:{p1_targetOrientation}/sections"

                createSectionPayload = {"course_section[name]": [studentMajor], "course_section[sis_section_id]": [studentSectionSisId]}

                createSectionObject = requests.post(createSectionApiUrl, headers = p1_header, params = createSectionPayload)
        
            ## Create the URLs for the API call will be made to
            enrollApiUrl = f"{CoreCanvasAPIUrl}courses/sis_course_id:{p1_targetOrientation}/enrollments"
            enrollSectionApiUrl = f"{CoreCanvasAPIUrl}sections/sis_section_id:{studentSectionSisId}/enrollments"

            ## Define the canvas api viarables
        
            enrollPayload = {"enrollment[user_id]":f"sis_user_id:{studentSisId}", "enrollment[type]":"StudentEnrollment", "enrollment[enrollment_state]":"active"}

            ## Make the API call to enroll the student Error
            enrollObject = requests.post(enrollApiUrl, headers = p1_header, params = enrollPayload)
            enrollSectionObject = requests.post(enrollSectionApiUrl, headers = p1_header, params = enrollPayload)

            logger.info (f"{studentSisId} enrolled in {p1_targetOrientation}")

        ## If the user has been enrolled in their orientation 
        else:
            ## And exists in the target oriention
            if studentSisId in p1_targetOrientationFinalQuizSubmissionDatesAndIds:
                ## Check if they took the final quiz and record the date it was taken if possible
                if p1_targetOrientationFinalQuizSubmissionDatesAndIds[studentSisId]:
                    targetSlateDataDF.loc[rowIndex, "Final Quiz Date Taken"] = p1_targetOrientationFinalQuizSubmissionDatesAndIds[studentSisId]
                    targetSlateDataDF.loc[rowIndex, "Final Quiz Date Taken Yes/No"] = "Yes"

            ## Create a target enrollment df by filtering by the student's sis id and the status of the enrollment
            targetEnrollmentsDF = p1_sisFeedEnrollmentDf[(p1_sisFeedEnrollmentDf['user_id'] == studentSisId) & (p1_sisFeedEnrollmentDf['status'] == "active")]        
            
            ## Create a target course df by filtering by the list of course sis ids within the target enrollment df
            targetCoursesDF = p1_sisFeedCoursesDf[p1_sisFeedCoursesDf['course_id'].isin(targetEnrollmentsDF['course_id'])]
            
            ## Convert the 10 day point to a date
            pd.to_datetime(targetCoursesDF['10_day_point']).dt.date
            
            ## Make a list of the unique tenth day point values
            tenthDayPointList = targetCoursesDF['10_day_point'].unique()

            ## Define a variable to hold the most recent tenth day point
            mostRecentTenthDayPoint = None
            
            ## For each tenth day point
            for tenthDayPoint in tenthDayPointList:

                ## Convert tenthDayPoint to a date if it's a Timestamp
                if isinstance(tenthDayPoint, pd.Timestamp):
                    tenthDayPoint = tenthDayPoint.date()
    
                ## Check if the tenth day point has passed
                if tenthDayPoint <= currentDateDatetime:
        
                    ## If the most recent tenth day point is None or the current tenth day point is greater than the most recent
                    if mostRecentTenthDayPoint is None or tenthDayPoint > mostRecentTenthDayPoint:
            
                        ## Set the most recent tenth day point to the current tenth day point
                        mostRecentTenthDayPoint = np.datetime64(tenthDayPoint)
                        
            ## Make a df of the courses whose tenth day point matches the most recent tenth day point
            mostRecentTenthDayCoursesDF = targetCoursesDF[targetCoursesDF['10_day_point'] == mostRecentTenthDayPoint]
            
            ## Create a target data activity df by filtering by the student's sis id
            targetDataActivityDF = p1_enrollmentDataActivityDF[p1_enrollmentDataActivityDF['Student ID'] == studentSisId]

            ## Create a yes/no variable to track whether the student has on or after the most recent tenth day of a course they are enrolled in
            hasParticipatedAfterTenthDay = ""

            ## If the targetDataActivityDF is not empty
            if not targetDataActivityDF.empty:

                ## For each course in the most recent tenth day courses df
                for index, row in mostRecentTenthDayCoursesDF.iterrows():

                    ## Create a target data activity df by filtering by the course id
                    targetDataActivityCourseDF = targetDataActivityDF[targetDataActivityDF['Course Number'] == row['course_id'].replace('_', '-')]

                    ## If the row's activity date is not NaT
                    if not pd.isnull(targetDataActivityCourseDF['Last Course Participation'].values[0]):

                        ## Convert the last course participation date to a date
                        targetCourseLastParticipationDate = datetime.strptime(f"{currentYear}-{targetDataActivityCourseDF['Last Course Participation'].values[0]}", '%Y-%m-%d').date()

                        ## If the row's activity date is equal to or after the most recent tenth day point
                        if targetCourseLastParticipationDate >= mostRecentTenthDayPoint:

                            ## Set the has participated after tenth day variable to yes
                            hasParticipatedAfterTenthDay = "Yes"
                            break

            ## If the has participated after tenth day variable is still blank and the mostRecentTenthDayPoint is not None
            if hasParticipatedAfterTenthDay == "" and mostRecentTenthDayPoint is not None:

                ## If the targetDataActivityDF not empty
                if not targetDataActivityDF.empty:
                    
                    ## Set the has participated after tenth day variable to no
                    hasParticipatedAfterTenthDay = "No"

            ## Set the Student Participated On Or After 10 Days (Y/N) value in the target slate data df to the has participated after tenth day value
            targetSlateDataDF.loc[rowIndex, "Student Participated On Or After 10 Days (Y/N)"] = hasParticipatedAfterTenthDay        

    except Exception as error:
        error_handler (functionName, p1_ErrorInfo = error)

def studentTypeGetIncomingStudentsInfo(p1_targetOrientation, p1_slateFile, p1_inputTerm):
    functionName = "Term Get Incoming Undergrad Student Info"

    try:

        ## Define the current slateFileName and slateFilePath
        slateFileName = p1_slateFile.split('\\')[-1]
        slateFilePath = p1_slateFile.replace(slateFileName, "")

        ## Define the target file ougoing output path
        outgoingTermInputPath = slateFilePath.replace("Incoming", "Outgoing")

        
 
        ## If the outgoingTermInputPath doesn't already exist, create it
        if not (os.path.exists(outgoingTermInputPath)):
            os.makedirs(outgoingTermInputPath, mode=0o777, exist_ok=False)
 
        ## Define the updated slate file name and path with name
        updatedFileName = slateFileName.replace(".csv", "") + '_canvas_data.csv'
        updatedSlateFilePathWithName = outgoingTermInputPath + updatedFileName
        
    
        ## If the file exists
        if os.path.exists(updatedSlateFilePathWithName):
            
            ## Get its last moddifed timestamp
            targetFileTimestamp = os.path.getmtime(updatedSlateFilePathWithName)

            ## Convert the timestamp to datetime
            targetFileDateTime = datetime.fromtimestamp(targetFileTimestamp)

            ## Subtract the file's datetime from the current datetime
            targetFileHoursOld = int((currentDate - targetFileDateTime).total_seconds() // 3600)

            ## If it has been less than 3 and a half hours since the target was updated
            if targetFileHoursOld < 3.5:

                ## logger.info that the file is up to date and return
                logger.info (f"     \n {updatedSlateFilePathWithName} is up to date")
                return

        ## Define the p1_header for all subsequent canvas api calls
        header = {'Authorization' : 'Bearer ' + canvasAccessToken}

        ## Define the Canvas input path by replacing "Slate Resources" with "Canvas Resources" and removing "Incoming"
        relaventCanvasInputPath = slateFilePath.replace("Slate Resources", "Canvas Resources").replace("Incoming", "")

        ## If the p1_targetOrientation has "GPS" in it
        if "GPS" in p1_targetOrientation:
           
           ## If the slateFilePath shows that the input term is a spring term
           if "SP" in slateFilePath:
               
               ## Define the relavent fall term year
               relaventFallTermYear = int(relaventCanvasInputPath.split('\\')[5][2:])-1
           
               ## Change SP + ## to GF + (## - 1) because the fall GPS orientation is used again in the spring orientation
               relaventCanvasInputPath = relaventCanvasInputPath.replace(p1_inputTerm, f"GF{relaventFallTermYear}")

           ## Otherwise
           else:
               
               ## Change the relavent Canvas input path to the ongoing Grad Fall Term
               relaventCanvasInputPath = relaventCanvasInputPath.replace(p1_inputTerm[:2], f"GF")

        ## Define the default relevant canvas input term
        relaventCanvasInputTerm = relaventCanvasInputPath.split('\\')[5]

        ## Redefine the relevant canvas term as "Default Term" if "Graduate" in the target orientation as their orientation is not specific to a term
        if "Graduate" in p1_targetOrientation or "GPS" in p1_targetOrientation:
            relaventCanvasInputTerm = "Default Term"

        ## Retrieve (and update if neccessary) the term relavent canvas courses file path
        orientationCourseTermLocationDf = pd.read_csv(termGetCourses(relaventCanvasInputTerm))

        ## Find the "canvas_course_id" by looking for the target orientation sis id in "course_id"
        p1_targetOrientationCanvasCourseId = orientationCourseTermLocationDf.loc[orientationCourseTermLocationDf['short_name'] == p1_targetOrientation, 'canvas_course_id'].values[0]
         
        ## Define the orientation course's base api url
        orientationCourseApiUrl = f"{CoreCanvasAPIUrl}courses/{p1_targetOrientationCanvasCourseId}"
        
        ## Define the url to get the course's students
        orientationCourseUserApiUrl = f"{orientationCourseApiUrl}/users"
         
        ## Define the payload to get the course's students
        orientationCourseUserPayload = {"enrollment_type[]":["student"], "per_page": 100}

        ## Make the course enrollment list
        orientationCourseEnrollmentObject = requests.get(orientationCourseUserApiUrl, headers = header, params = orientationCourseUserPayload)

        ## Save the result as a list of dicts for each student enrolled
        p1_targetOrientationStudentObjects = orientationCourseEnrollmentObject.json()

        ## If there are more pages to the enrollment response get them as well
        while "next" in orientationCourseEnrollmentObject.links:
            nextOrientationPageUrl = orientationCourseEnrollmentObject.links["next"]["url"]
            orientationCourseEnrollmentObject = requests.get(nextOrientationPageUrl, headers=header)  # Fetch the next page
            nextOrientationPage_jsonObject = orientationCourseEnrollmentObject.json()
            p1_targetOrientationStudentObjects.extend(nextOrientationPage_jsonObject)  # Collect students and add them to the first orientationCourse_jsonObject

        ## Record the student's ids
        p1_targetOrientationStudents = []

        for studentObject in p1_targetOrientationStudentObjects:
            p1_targetOrientationStudents.append(studentObject["sis_user_id"])

        logger.info ("Target Orientation Students recorded")

        ## Define the api url to get the target's sections
        orientationCourseSectionApiUrl = f"{orientationCourseApiUrl}/sections"

        ## Make the target section call
        orientationCourseSectionObject = requests.get(orientationCourseSectionApiUrl, headers = header)

        ## Define a variable to hold the target orientation's sections
        p1_targetOrientationSections = orientationCourseSectionObject.json()

        logger.info ("Target Orientation Sections recorded")

        ## Make an api call to get a list of user sis ids for those that have submitted the final quiz and when they took it
        ## This marks the completion of the orientation
        p1_targetOrientationFinalQuizSubmissionDatesAndIds ={}

        ## Define the core new quiz url to get a list of the courses quizzes
        newQuizCoreUrl = CoreCanvasAPIUrl.replace("api/v1", "api/quiz/v1")

        ## Define the course specific api new quiz url
        quizListApiUrl = f"{CoreCanvasAPIUrl}courses/{p1_targetOrientationCanvasCourseId}/quizzes"

        ## Make the new quiz list api call
        quizzListObject = requests.get(quizListApiUrl, headers = header)

        ## Turn the quizzes list api result into a dictionary
        quizzList_jsonString = quizzListObject.text
        quizzList_jsonObject = json.loads(quizzList_jsonString)

        ## Find the orientation quiz id and assignment id if they exist
        targetQuizID = None
        targetQuizAssignmentId = None
        for quiz in quizzList_jsonObject:
            if "orientation" in quiz["title"].lower():
                targetQuizID = quiz["id"]
                targetQuizAssignmentId = quiz["assignment_id"] if quiz["assignment_id"] else quiz["id"]
                break

        ## If the orientation quiz still hasn't been found
        if not targetQuizID:
    
            ## : again with the new quiz url
            quizListApiUrl = f"{newQuizCoreUrl}/courses/{p1_targetOrientationCanvasCourseId}/quizzes"

        ## Make the new quiz list api call
        quizzListObject = requests.get(quizListApiUrl, headers = header)
    
        ## Turn the quizzes list api result into a dictionary
        quizzList_jsonString = quizzListObject.text
        quizzList_jsonObject = json.loads(quizzList_jsonString)
    
        ## Find the orientation quiz id if it exists
        for quiz in quizzList_jsonObject:
            if "orientation" in quiz["title"].lower():
                targetQuizID = quiz["id"]
                targetQuizAssignmentId = quiz["id"]
                break

        ## Get the submissions for the quiz
        if targetQuizID:
    
            ## Create a variable to hold the api url for the quiz submission list
            quizSubmissionListApiUrl = None

            ## If api/quiz/v1 was used to get the quiz id
            if "api/quiz/v1" in quizListApiUrl:

                ## Set the submissin list api url to use the assignments path
                quizSubmissionListApiUrl = f"{CoreCanvasAPIUrl}courses/{p1_targetOrientationCanvasCourseId}/assignments/{targetQuizAssignmentId}/submissions"

            ## Otherwise 
            else:
        
                ## Use the quizzes path
                quizSubmissionListApiUrl = f"{CoreCanvasAPIUrl}courses/{p1_targetOrientationCanvasCourseId}/quizzes/{targetQuizID}/submissions"

        ## Define the playload for the submission list
        quizSubmissionPayload = {"include[]":["user"], "per_page": 100}

        ## Make the submission list api call
        quizzSubmissionsObject = requests.get(quizSubmissionListApiUrl, headers = header, params = quizSubmissionPayload)

        ## Turn the quizzes list api result into a dictionary
        quizzSubmissionsList_jsonString = quizzSubmissionsObject.text
        quizzSubmissionsList_jsonObject = json.loads(quizzSubmissionsList_jsonString)

        ## If there are more pages to the submission response get them as well
        while "next" in quizzSubmissionsObject.links:
            nextSubmissionPageUrl = quizzSubmissionsObject.links["next"]["url"]
            quizzSubmissionsObject = requests.get(nextSubmissionPageUrl, headers=header)  # Fetch the next page
            nextSubmissionsPage_jsonObject = quizzSubmissionsObject.json()
            
            ## Iterate through the keys and values of nextSubmissionsPage_jsonObject
            for key, value in nextSubmissionsPage_jsonObject.items():
                
                ## If the key exists in the original json object
                if key in quizzSubmissionsList_jsonObject:
                    
                    ## If both values are lists
                    if isinstance(quizzSubmissionsList_jsonObject[key], list) and isinstance(value, list):

                        ## Extend the list to include the new values
                        quizzSubmissionsList_jsonObject[key].extend(value)
                
                ## Otherwise
                else:
                    
                    ## If the key does not exist in both, add the key-value pair
                    quizzSubmissionsList_jsonObject[key] = value


        ## If quiz_submissions and users are both inthe json object's keys
        if "quiz_submissions" in quizzSubmissionsList_jsonObject.keys() and "users" in quizzSubmissionsList_jsonObject.keys():

            ## for each user and submission in a zipped list of the two dicts
            for user, submission in zip(quizzSubmissionsList_jsonObject["users"], quizzSubmissionsList_jsonObject["quiz_submissions"]):

                ## If the user has a sis_user_id
                if user["sis_user_id"]:

                    ## attempt to save the user's sis_user_id and the submission's submitted_at date
                    try: ## Irregular try clause, do not comment out in testing
                
                        p1_targetOrientationFinalQuizSubmissionDatesAndIds[int(user["sis_user_id"])] = submission['finished_at']
                    
                    ## If there wasn't a finished at value
                    except: ## Irregular except clause, do not comment out in testing
                    
                        ## Set the value as "Ongoing"
                        p1_targetOrientationFinalQuizSubmissionDatesAndIds[int(user["sis_user_id"])] = "Ongoing"
                    

        ## Otherwise
        else:         
            ## Just get both from the submission list
            for submission in quizzSubmissionsList_jsonObject:
                if submission["user"]["sis_user_id"]:
                    p1_targetOrientationFinalQuizSubmissionDatesAndIds[int(submission["user"]["sis_user_id"])] = submission['submitted_at']

        logger.info ("Target Orientation Submissions recorded")
 
        ## Read the current Slate 
        slateDataDF = pd.read_csv(p1_slateFile, converters = {"SlateID": str, "SlateAppID": str})
 
        ## Add columns for the username, last login date, and final quiz taken date
        slateDataDF["NNU Username"] = ""
        slateDataDF["Last Login Date"] = pd.NaT
        slateDataDF["Last Login Date Yes/No"] = "No"
        slateDataDF["Final Quiz Date Taken"] = pd.NaT
        slateDataDF["Final Quiz Date Taken Yes/No"] = "No"

        ## Open the current SIS Feed Enrollment and Course CSVs
        rawSisFeedEnrollmentDf = pd.read_csv(f"{baseExternalInputPath}canvas_enroll.csv")
        rawSisFeedCourseDf = pd.read_csv(f"{baseExternalInputPath}canvas_course.csv")
        
        ## Filter the SIS Feed Enrollment DF to only contain students
        sisFeedEnrollmentDf = rawSisFeedEnrollmentDf[rawSisFeedEnrollmentDf['role'] == "student"]

        ## Define the target term as undergrad or grad according to the target orientation
        targetTerm = p1_inputTerm if "TUG" in p1_targetOrientation else p1_inputTerm.replace('FA', 'GF').replace('SP', 'GS')

        ## Filter the SIS Feed Course DF to only contain the input term and the grad version of the input term
        sisFeedCourseDf = rawSisFeedCourseDf[(rawSisFeedCourseDf['term_id'] == targetTerm)]
        
        ## Add a start/end combination column to the target course df
        sisFeedCourseDf['start/end'] = sisFeedCourseDf['start_date'] + "/" + sisFeedCourseDf['end_date']

        ## Add a 10 day point column to the target course df by adding 31 days to the start date (start dates are always set 3 weeks before the official start date)
        sisFeedCourseDf['10_day_point'] = pd.to_datetime(sisFeedCourseDf['start_date']) + pd.DateOffset(days=31)
        
        ## Make a dict with the unique start/end values as the keys and a list with the start_at and end_at values as the values
        startEndDict = sisFeedCourseDf.groupby('start/end').agg({'start_date': 'first', 'end_date': 'first'}).to_dict(orient='index')

        ## Open up the enrollment data activity file
        enrollmentDataActivityDF = pd.read_csv(f"{baseExternalInputPath}output\\pharos\\Enrollment_Data_Activity.csv", delimiter='|')
 
        ongoingStudentThreads = []

        ## Define input threading objects
        for index, row in slateDataDF.iterrows():

            #if row['StudentID'] == 190996:

                newThread = threading.Thread(target=getTargetIncomingStudentInfo
                                             , args=(row
                                                     , index
                                                     , p1_targetOrientation
                                                     , p1_targetOrientationStudents
                                                     , p1_targetOrientationSections
                                                     , p1_targetOrientationFinalQuizSubmissionDatesAndIds
                                                     , header
                                                     , slateDataDF
                                                     , sisFeedCourseDf
                                                     , sisFeedEnrollmentDf
                                                     , enrollmentDataActivityDF
                                                     )
                                             )
                newThread.start()
                ongoingStudentThreads.append(newThread)
                time.sleep(1)
 
        ## Check if all ongoing input threads have completed
        for thread in ongoingStudentThreads:
            thread.join()

        ## Save the updated slate undergrad data DF
        slateDataDF.to_csv(updatedSlateFilePathWithName, index=False)

        ## Define a veriable to hold the slate creds json file
        slateCreds = None

        ## Open the slate creds json file from the configPath
        with open(f"{configPath}Slate_Creds.json", "r") as file:

            ## Load the json file
            slateCreds = json.load(file)

        ## Define the slate creds
        ASHost = slateCreds["ASHost"]
        ASPort = slateCreds["ASPort"]
        ASUsername = slateCreds["ASUsername"]
        ASPassword = slateCreds["ASPassword"]
        ASPublicKeyPath = f"{configPath}Slate_Public_Key.txt"
 
        # Create an SSH client
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        ## Define an attempt counter
        attemptCounter = 0

        ## Define a variable to hold the connection status
        connected = False

        ## While the connection is not established
        while not connected:
            ## Try to connect to the SFTP server
            try: ## Irregular except clause, do not comment out in testing
                ## Connect to the SFTP server
                ssh_client.connect(hostname=ASHost, port=ASPort, username=ASUsername, password=ASPassword, key_filename=ASPublicKeyPath)#, command=ASCommandLine)
                connected = True
            ## If the connection fails
            except Exception as error: ## Irregular except clause, do not comment out in testing
                ## Log the error
                logger.error (f"     \nError connecting to SFTP server: {error}")
                ## Increment the attempt counter
                attemptCounter += 1
                ## If the attempt counter is greater than 3
                if attemptCounter > 3:
                    ## Log the fact that the connection failed
                    logger.error (f"     \nFailed to connect to SFTP server after 3 attempts")
                    ## Break the loop
                    break
                ## Otherwise
                else:
                    ## Log that the connection will be attempted again
                    logger.error (f"     \nAttempting to connect to SFTP server again")
                    ## Wait 5 seconds
                    time.sleep(5)
 
        # Create an SFTP client from the SSH client
        sftp_client = ssh_client.open_sftp()
 
        updatedSlateDataFile_remote_file_path = None
 
        ## Specific the path for the file to be saved to
        if "prof" in p1_slateFile:
            updatedSlateDataFile_remote_file_path = f'./Incoming//Canvas//Prof_progs_canvas_data.csv'
        elif "grad" in p1_slateFile:
            updatedSlateDataFile_remote_file_path = f'./Incoming//Canvas//Graduate_canvas_data.csv'
        else:
            updatedSlateDataFile_remote_file_path = f'./Incoming//Canvas//Undergrad_canvas_data.csv'

        try: ## Irregular try clause, do not comment out in testing
            # Upload the file
            sftp_client.put(updatedSlateFilePathWithName, updatedSlateDataFile_remote_file_path)
            logger.info("File uploaded successfully.")
        finally:
            # Close the SFTP client and SSH connection
            sftp_client.close()
            ssh_client.close()
 
    except Exception as error:
        error_handler (functionName, p1_ErrorInfo = error)

def termGetIncomingStudentsInfo(inputTerm = ""):
    functionName = "Term Get Incoming Student Information"

    try:

        ## Determine and save the term's school year
        targetSchoolYear = None
        if re.search("AF|FA|GF", inputTerm):
            ## Fall terms are the first terms of a new school year so FA20 is part of the 2020-21 school year.
            targetSchoolYear = (century + inputTerm[2:] + "-" + str(int(inputTerm[2:]) + 1))
        elif re.search("SP|GS|AS|SG|SA|SU", inputTerm):
            ## Spring and Summer terms belong in the same school year as the fall terms before them, so SP21 is part of the same 2020-21 school year as FA20.
            targetSchoolYear = (century + str(int(inputTerm[2:]) - 1) + "-" + inputTerm[2:])

        ## Define the incoming School Year input path
        incomingSchoolYearInputPath = f"{baseLocalInputPath}{targetSchoolYear}\\"

        ## Define the fall, spring, or summer term word (fall if fa in input term)
        termWord = "Fall" if "FA" in inputTerm else "Spring" if "SP" in inputTerm else "Summer"
                
        ## Define the generic undergrad target orientation course
        targetUndgOrientation = f"{termWord} {currentYear} - NNU Pre-Launch Orientation"
        targetGradOrientation = targetGradOrientation = f"Graduate & Professional Student Hub"
        
        ## If the input term is in ["FA25", "GF25"] or is =< GS26 or SP26
        if inputTerm in ["SP25", "GS25", "SU25", "SG25"]:

            ## Set the targetUndgOrientation to the old Graduate & Professional Student Hub title
            targetGradOrientation = f"{targetSchoolYear[:5]}{targetSchoolYear[:2]}{targetSchoolYear[5:]}_GPS_Orientation"

        ## Define the term specific output path
        incomingTermInputPath = f"{incomingSchoolYearInputPath}{inputTerm}\\Incoming\\"

        ## If the incomingTermInputPath doesn't already exist, create it
        if not (os.path.exists(incomingTermInputPath)):
            os.makedirs(incomingTermInputPath, mode=0o777, exist_ok=False)

        # Get a list of the files from slate and work through them
        slateFiles = getSlateInfo(inputTerm)
        #slateFiles = os.listdir(incomingTermInputPath)

        ## If there are no files in slate files list
        if not slateFiles:

            ## Log that there are no files in the slate files list
            logger.info ("No files in slate files to process")
            return

        ## Define a list to hold the subsequent threads used to work through the files from slate
        ongoingReportThreads = []

        ## For each file from slate
        for slateFile in slateFiles:
            
            ## Define the get student info report thread
            getStudentInfoThread = None
            
            ## If the target slate file contains graduate students or professional students 
            if "grad" in slateFile or "prof" in slateFile:
                
                ## Target the graduate orientation course/s
                getStudentInfoThread = threading.Thread(target=studentTypeGetIncomingStudentsInfo, args=(targetGradOrientation, slateFile, inputTerm))

            ## If the target slate file contains undergraduate students
            else:

                ## Target the undergraduate orientation course
                getStudentInfoThread = threading.Thread(target=studentTypeGetIncomingStudentsInfo, args=(targetUndgOrientation, slateFile, inputTerm))

            ## Start the get student info report thread
            getStudentInfoThread.start()

            ## Add the get student info report thread to the ongoingReportThreads list
            ongoingReportThreads.append(getStudentInfoThread)

        ## Wait for all of the ongoingReportThreads to finish
        for thread in ongoingReportThreads:
            thread.join()

        ## Define the outgoing term input path
        outgoingTermInputPath = incomingTermInputPath.replace("Incoming", "Outgoing")

        ## Define a verible to hold the names of the ougoing term input files
        outgoingTermInputFiles = os.listdir(outgoingTermInputPath)

        ## For each file in the outgoing term input path
        for slateFile in outgoingTermInputFiles:

            ## If the file is not contained in any of the slate file paths
            if not any(slateFile.replace('_canvas_data', '') in filePath.split('\\')[-1] for filePath in slateFiles):

                ## Delete the file
                os.remove(f"{outgoingTermInputPath}{slateFile}")


    except Exception as error:
        error_handler (functionName, p1_ErrorInfo = error)

if __name__ == "__main__":

    ## Start and download the Canvas report
    termGetIncomingStudentsInfo (inputTerm = input("Enter the desired term in \
four character format (FA20, SU20, SP20): "))

    input("Press enter to exit")
