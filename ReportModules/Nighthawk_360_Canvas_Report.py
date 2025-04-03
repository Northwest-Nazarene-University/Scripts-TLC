# Author: Bryce Miller - brycezmiller@nnu.edu
# Last Updated by: Bryce Miller

## Import Generic Moduels

import os, sys, logging, requests, json, os, shutil, os.path, threading, time
from datetime import datetime
from datetime import date
import pandas as pd

## Set working directory
os.chdir(os.path.dirname(__file__))

## Add Script repository to syspath
sys.path.append(f"{os.getcwd()}\ResourceModules")

# Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = "Nigthhawk 360 Student Report"

scriptPurpose = r"""
Retrieve enrolled student's course level grade and activity information
"""
externalRequirements = r"""
To function properly, this script requires access to NNU's current enrollment list, the the corresponding Canvas enrollment list, Canvas API
and the "{baseExternalInputPath}\\output\\pharos" folder
"""

# Time variables
currentDate = date.today()
currentDatetime = datetime.now()
currentMonth = currentDate.month
currentYear = currentDate.year
lastYear = currentYear - 1
nextYear = currentYear + 1
century = str(currentYear)[:2]
decade = str(currentYear)[2:]

## Set working directory
fileDir = os.path.dirname(__file__)
os.chdir(fileDir)

## The relative path is used to provide a generic way of finding where the Scripts TLC folder has been placed
## This provides a non-script specific manner of finding the vaiours related modules
PFRelativePath = r".\\"

## If the Scripts TLC folder is not in the folder the PFRelative path points to
## look for it in the next parent folder
while "Scripts TLC" not in os.listdir(PFRelativePath):

    PFRelativePath = f"..\\{PFRelativePath}"

## Change the relative path to an absolute path
PFAbsolutePath = f"{os.path.abspath(PFRelativePath)}\\"

## Add the Resource Modules folder to the sys path
sys.path.append(f"{PFAbsolutePath}Scripts TLC\\ResourceModules")

## Import local modules
from Error_Email_API import errorEmailApi
from Get_Canvas_User_Last_Access import termGetCanvasUserLastAccess

## Local Path Variables
baseLogPath = f"{PFAbsolutePath}Logs\\{scriptName}\\"
configPath = f"{PFAbsolutePath}\\Configs TLC\\"
baseLocalInputPath = f"{PFAbsolutePath}Canvas Resources\\"
baseLocalOutputPath = f"{PFAbsolutePath}Canvas Resources\\"

## External Path Variables

## Define a variable to hold the base external input path and output path 
baseExternalInputPath = None ## Where the sis input files are stored
baseExternalOutputPath = None ## Where the output files are stored

## Open Base_External_Paths.json from the config path and get the baseExternalInputPath and baseExternalOutputPath values
with open (f"{configPath}Base_External_Paths.json", "r") as file:
    fileJson = json.load(file)
    baseExternalInputPath = fileJson["baseExternalInputPath"]
    baseExternalOutputPath = fileJson["baseRetentionPharosDataExternalOutputPath"]

## Canvas Instance Url
CoreCanvasAPIUrl = None
## Open the Core_Canvas_Url.txt from the config path
with open (f"{configPath}Core_Canvas_Url.txt", "r") as file:
    CoreCanvasAPIUrl = file.readlines()[0]

## Define the core Canvas enrollment API url
coreEnrollmentApiUrl = f"{CoreCanvasAPIUrl}/accounts/1/enrollments/"

## Define the course Canvas course api url
coreCoursesApiUrl = f"{CoreCanvasAPIUrl}//courses//"

## If the script is run as main the folder with the access token is in the parent directory
canvasAccessToken = ""

## Open and retrieve the Canvas Access Token
with open (fr"{configPath}Canvas_Access_Token.txt", "r") as file:
    canvasAccessToken = file.readlines()[0]

#Primary API call header and payload
header = {'Authorization' : 'Bearer ' + canvasAccessToken}


## Begin logger set up

## If the base log path doesn't already exist, create it
if not (os.path.exists(baseLogPath)):
    os.makedirs(baseLogPath, mode=0o777, exist_ok=False)

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

## This variable enables the error_handler function to only send
## an error email the first time the function triggeres an error
## by tracking what functions have already been recorded as having errors
setOfFunctionsWithErrors = set()

## This function handles function errors
def error_handler (p1_ErrorLocation, p1_ErrorInfo, sendOnce = True):
    functionName = "error_handler"

    ## Log the error
    logger.error (f"     \nA script error occured while running {p1_ErrorLocation}. " +
                     f"Error: {str(p1_ErrorInfo)}")

    ## If the function with the error has not already been processed send an email alert
    if (p1_ErrorLocation not in setOfFunctionsWithErrors):
        errorEmailApi.sendEmailError(p2_ScriptName = scriptName, p2_ScriptPurpose = scriptPurpose, 
                                     p2_ExternalRequirements = externalRequirements, 
                                     p2_ErrorLocation = p1_ErrorLocation, p2_ErrorInfo = p1_ErrorInfo)
        
        ## Add the function name to the set of functions with errors
        setOfFunctionsWithErrors.add(p1_ErrorLocation)
        
        ## Note that an error email was sent
        logger.error (f"     \nError Email Sent")
    
    ## Otherwise log the fact that an error email as already been sent
    else:
        logger.error (f"     \nError email already sent")

## This function recursively parses discussion post replies to search for a particular user's latest reply
def getStuMostRecentGradedDiscussionPostDateRecursive (p2_stuCanvasId, p1_stuLastGradedDiscussionPostDate, p1_post):
    functionName = functionName = "Get a Student's Most Recent Graded Discussion Post Date Recursive"

    try:

        ## Look for replies within the post
        if "replies" in p1_post.keys():

            for reply in p1_post["replies"]:

                if "user_id" in reply.keys():

                    if reply["user_id"] == p2_stuCanvasId:

                        if not p1_stuLastGradedDiscussionPostDate or p1_stuLastGradedDiscussionPostDate < reply["updated_at"]:

                            p1_stuLastGradedDiscussionPostDate = reply["updated_at"]

                getStuMostRecentGradedDiscussionPostDateRecursive (p2_stuCanvasId, p1_stuLastGradedDiscussionPostDate, reply)

    except Exception as error:
        error_handler (functionName, p1_ErrorInfo = error)

## This function returns the user's most recent graded discussion post date
def getStuMostRecentGradedDiscussionPostDate (p1_stuDiscussionListAPIUrl, p1_stuCanvasId):
    functionName = "Get a Student's Most Recent Graded Discussion Post Date"

    try:

        ## Make an analytics api stuDiscussionPostsAPI to get the users activity
        stuDiscussionListObject = None

        ## Define a variable to track the number of attempts to get the api call
        stuDiscussionListApiAttempts = 0

        ## If the api call was not sucessful and the number of secondaryCoreCourseApiAttempts is less than 5
        while (not stuDiscussionListObject 
               or (stuDiscussionListObject.status_code != 200 
                   and stuDiscussionListApiAttempts < 5)
               ):

            try: ## Irregular try clause, do not comment out in testing

                ## If the last api attempt num is greater than 0
                if stuDiscussionListApiAttempts > 0:
                    
                    ## Wait for two seconds if the api call was previously unsucessful
                    time.sleep(2)
                    
                    ## Log that the api call is being tried again
                    logger.info(f"Retrying api call to {p1_stuDiscussionListAPIUrl} for canvas_id: {p1_stuCanvasId}")

                    ## Close the previous api object
                    if stuDiscussionListObject:
                        stuDiscussionListObject.close()
                
                ## try the api call again
                stuDiscussionListObject = requests.get(url=p1_stuDiscussionListAPIUrl, headers = header)

            except Exception as error: ## Irregular except clause, do not comment out in testing
                logger.warning(f"Error: {error} \n Occured when calling {p1_stuDiscussionListAPIUrl} for canvas_id: {p1_stuCanvasId}")

            ## Increment the number of attempts
            stuDiscussionListApiAttempts += 1

        ## If the api call was sucessful
        if stuDiscussionListObject.status_code == 200:

            stuDiscussionList = json.loads(stuDiscussionListObject.text)

            ## Close the api object
            stuDiscussionListObject.close()

            stuLastGradedDiscussionPostDate = None

            for discussion in stuDiscussionList:

                if "assignment" in discussion.keys():

                    discussionCanvasId = discussion["id"]
                                        
                    stuDiscussionViewAPIUrl = f"{p1_stuDiscussionListAPIUrl}//{discussionCanvasId}/view"

                    stuDiscussionViewObject = None

                    ## Define a variable to track the number of attempts to get the api call
                    stuDiscussionViewApiAttempts = 0

                    ## If the api call was not sucessful and the number of secondaryCoreCourseApiAttempts is less than 5
                    while (not stuDiscussionViewObject
                           or (stuDiscussionViewObject.status_code != 200 
                               and stuDiscussionViewApiAttempts < 5)
                           ):

                        try: ## Irregular try clause, do not comment out in testing
                            
                            ## If the last api attempt num is greater than 0
                            if stuDiscussionViewApiAttempts > 0:
                    
                                ## Wait for two seconds if the api call was previously unsucessful
                                time.sleep(2)
                    
                                ## Log that the api call is being tried again
                                logger.info(f"Retrying api call to {stuDiscussionViewAPIUrl} for canvas_id: {p1_stuCanvasId}")

                                ## Close the previous api object
                                if stuDiscussionViewObject:
                                    stuDiscussionViewObject.close()
                    
                            ## try the api call again
                            stuDiscussionViewObject = requests.get(url=stuDiscussionViewAPIUrl, headers = header)
                            
                        except Exception as error: ## Irregular except clause, do not comment out in testing
                            logger.warning(f"Error: {error} \n Occured when calling {stuDiscussionViewAPIUrl} for canvas_id: {p1_stuCanvasId}")
                            
                        ## Increment the number of attempts
                        stuDiscussionViewApiAttempts += 1

                    if stuDiscussionViewObject.status_code == 200:

                        stuDiscussionViewDict = json.loads(stuDiscussionViewObject.text)

                        stuDiscussionViewObject.close

                        for post in stuDiscussionViewDict["view"]:

                            if "user_id" in post.keys():

                                if post["user_id"] == p1_stuCanvasId:

                                    if not stuLastGradedDiscussionPostDate or stuLastGradedDiscussionPostDate < post["updated_at"]:

                                        stuLastGradedDiscussionPostDate = post["updated_at"]

                            getStuMostRecentGradedDiscussionPostDateRecursive (p1_stuCanvasId, stuLastGradedDiscussionPostDate, post)

                    stuDiscussionViewObject.close()

            return stuLastGradedDiscussionPostDate

    except Exception as error:
        error_handler (functionName, p1_ErrorInfo = error)


## This function gets the student's most recent course specific activity and grade data
def getStuCourseData (p2_stuID
                      , p1_stuSisEnrolledCourseIds
                      , p2_stuCoursesData
                      , p1_stuCanvasEnrolledCourseIds
                      , p1_stuCanvasId
                      , targetCourseSisId
                      , p2_combinedUnpublishedCanvasCoursesList
                      ):
    
    functionName = "Get Stu Course Data"

    try:

        ## Look for a match of the CX enrollment within the student's Canvas enrollment list
        stuCourseEnrollmentDataDF = p1_stuCanvasEnrolledCourseIds[(p1_stuCanvasEnrolledCourseIds["course_id"] == targetCourseSisId)
                                                                & (p1_stuCanvasEnrolledCourseIds["user_id"] == p2_stuID)
                                                                ]

        ## Define a variable to hold the parent course ID if applicable
        parentStuEnrolledCourseId = ""

        ## If the course is not part of the student's Canvas enrollments list
        if stuCourseEnrollmentDataDF.empty:

            ## For each of the course enrollments the student has in the Canvas enrolled course ids df
            for secondaryStuCanvasEnrolledCourseId in p1_stuCanvasEnrolledCourseIds["course_id"]:

                ## If no parent id has been found
                if not parentStuEnrolledCourseId:

                    ## Define a api url to retrieve the course's sections
                    secondaryCoreCourseApiUrl = f"{coreCoursesApiUrl}//sis_course_id:{secondaryStuCanvasEnrolledCourseId}//sections"

                    ## Make an api call to retrieve a list of a the sections in the course
                    secondaryCoreCourseApiObject = None

                    ## Define a variable to track the number of attempts to get the api call
                    secondaryCoreCourseApiAttempts = 0

                    ## If the api call was not sucessful and the number of secondaryCoreCourseApiAttempts is less than 5
                    while (not secondaryCoreCourseApiObject
                           or (secondaryCoreCourseApiObject.status_code != 200 
                               and secondaryCoreCourseApiAttempts < 5)
                           ):
                        
                        try: ## Irregular try clause, do not comment out in testing

                            ## If the last api attempt num is greater than 0
                            if secondaryCoreCourseApiAttempts > 0:
                    
                                ## Wait for two seconds if the api call was previously unsucessful
                                time.sleep(2)
                    
                                ## Log that the api call is being tried again
                                logger.info(f"Retrying api call to {secondaryCoreCourseApiUrl} for canvas_id: {p1_stuCanvasId}")

                                ## Close the previous api object
                                if secondaryCoreCourseApiObject:
                                    secondaryCoreCourseApiObject.close()
                
                            ## try the api call again
                            secondaryCoreCourseApiObject = requests.get(url=secondaryCoreCourseApiUrl, headers = header)

                        except Exception as error: ## Irregular except clause, do not comment out in testing
                            logger.warning(f"Error: {error} \n Occured when calling {secondaryCoreCourseApiUrl} for canvas_id: {p1_stuCanvasId}")

                        ## Increment the number of attempts
                        secondaryCoreCourseApiAttempts += 1

                    ## If the api call was sucessful
                    if secondaryCoreCourseApiObject.status_code == 200:    

                        ## Otherwise save the text response of the api object as a dict
                        secondaryCoreCourseApiDict = json.loads(secondaryCoreCourseApiObject.text)

                        ## Close the api object
                        secondaryCoreCourseApiObject.close()

                        ## for each section in the secondary course
                        for section in secondaryCoreCourseApiDict:

                            ## If the target stu sis enrolled course id is in the name of the section
                            if targetCourseSisId in section["name"]:

                                ## Save the secondary student canvas enrolled course id as the parent course id
                                parentStuEnrolledCourseId = secondaryStuCanvasEnrolledCourseId

                                ## Replace the stuCourseEnrollmentDataDF with the studen'ts parent course enrollment data
                                stuCourseEnrollmentDataDF = p1_stuCanvasEnrolledCourseIds[(p1_stuCanvasEnrolledCourseIds["course_id"] == parentStuEnrolledCourseId)
                                                                    & (p1_stuCanvasEnrolledCourseIds["user_id"] == p2_stuID)
                                                                ]

            ## If stuCourseEnrollmentDataDF is still empty
            if stuCourseEnrollmentDataDF.empty:

                ## Do a warning log that the student is not enrolled in the course
                logger.warning(f"Student {p2_stuID} is not enrolled in course {targetCourseSisId}")

                ## Remove the target course from the stu sis enrolled course ids
                del p2_stuCoursesData[targetCourseSisId]

                ## Skip the course
                return "Remove"

        ## Add key value pairs to hold the other neccessary student course data
        p2_stuCoursesData   [targetCourseSisId]["Published"] = "No"
        p2_stuCoursesData   [targetCourseSisId]["Current Grade"] = ""
        p2_stuCoursesData   [targetCourseSisId]["Number of Missing Assignments"] = ""
        p2_stuCoursesData   [targetCourseSisId]["Number of Assignments graded 0"] = ""
        p2_stuCoursesData   [targetCourseSisId]["Last Course Activity Date"] = ""
        p2_stuCoursesData   [targetCourseSisId]["Last Course Participation"] = ""
        p2_stuCoursesData   [targetCourseSisId]["Number of Missing Assignments"] = ""
        p2_stuCoursesData   [targetCourseSisId]["Number of Assignments graded 0"] = ""

        ## Check if the course is in the unpublished course list
        if targetCourseSisId not in p2_combinedUnpublishedCanvasCoursesList:

            ## If not, change the published status to Yes
            p2_stuCoursesData[targetCourseSisId]["Published"] = "Yes"

        ## If there is a parent course ID, see if that is in the unpublished list
        elif parentStuEnrolledCourseId and parentStuEnrolledCourseId not in p2_combinedUnpublishedCanvasCoursesList:

            ## If not, still change the published status to Yes due to the parent course being published
            p2_stuCoursesData[targetCourseSisId]["Published"] = "Yes"

        ## Otherwise
        else:

            ## Skip the course as it is unpublished and no other data will be found
            return "Completed"

        ## Otherwise record the student's enrollment ID for that course in a new sub dictionary of the student ID's dictionary
        p2_stuCoursesData[targetCourseSisId]["canvas_enrollment_id"] = stuCourseEnrollmentDataDF["canvas_enrollment_id"].values[0]

        ## Use the Canvas enrollment id to define the API url for the related canvas enrollment object
        stuCourseEnrollmentApiUrl = f"{coreEnrollmentApiUrl}{p2_stuCoursesData[targetCourseSisId]['canvas_enrollment_id']}"

        ## Make an api call with the enrollment ID to get the relevent Canvas enrollment object
        stuCourseEnrollmentObject = None

        ## Define a variable to track the number of attempts to get the api call
        stuCourseEnrollmentApiAttempts = 0

        ## If the api call was not sucessful and the number of secondaryCoreCourseApiAttempts is less than 5
        while (not stuCourseEnrollmentObject
               or (stuCourseEnrollmentObject.status_code != 200 
                   and stuCourseEnrollmentApiAttempts < 5)
               ):

            try: ## Irregular try clause, do not comment out in testing

                ## If the last api attempt num is greater than 0
                if stuCourseEnrollmentApiAttempts > 0:
                    
                    ## Wait for two seconds if the api call was previously unsucessful
                    time.sleep(2)
                    
                    ## Log that the api call is being tried again
                    logger.info(f"Retrying api call to {stuCourseEnrollmentApiUrl} for canvas_id: {p1_stuCanvasId}")

                    ## Close the previous api object
                    if stuCourseEnrollmentObject:
                        stuCourseEnrollmentObject.close()
                
                ## try the api call again
                stuCourseEnrollmentObject = requests.get(url=stuCourseEnrollmentApiUrl, headers = header)
                
            except Exception as error: ## Irregular except clause, do not comment out in testing
                logger.warning(f"Error: {error} \n Occured when calling {stuCourseEnrollmentApiUrl} for canvas_id: {p1_stuCanvasId}")

            ## Increment the number of attempts
            stuCourseEnrollmentApiAttempts += 1

        ## If the api call ultimately wasn't sucessful
        if stuCourseEnrollmentObject.status_code != 200:

            ## Skip the current student course enrollment
            return "Incomplete"

        ## Convert the json api reponse to a dictionary
        stuCourseEnrollmentDict = json.loads(stuCourseEnrollmentObject.text)

        ## Close the api object
        stuCourseEnrollmentObject.close()

        ## If the student has a current score
        if "grades" in stuCourseEnrollmentDict.keys():
            if stuCourseEnrollmentDict["grades"]:

                ## Get the student's current score
                stuCurrentScore = stuCourseEnrollmentDict["grades"]["current_score"] ## This is a percentage value of the number of points 
                    ## they have recieved out of the current points possible value the "current_score" value is retreived rather than grade because "current_grade" is a letter grade which can vary in meaning between courses

                ## Record the student's listed current score as their current grade
                p2_stuCoursesData[targetCourseSisId]["Current Grade"] = f"{stuCurrentScore}"

        ## Create a placeholder for the student's last canvas activity date, course submission date, their number of missing assignments, and their number of assignments graded 0
        stuLastCourseActivty = ""
        stuLastSubmissionDateTime = ""
        stuNumOfMissedAssignments = 0
        stuNumOfAssignmentsGradedZero = 0

        ## Get the student's last course activty date
        if stuCourseEnrollmentDict["last_activity_at"]:
            stuLastCourseActivty = datetime.strptime(stuCourseEnrollmentDict["last_activity_at"], "%Y-%m-%dT%H:%M:%SZ")

        ## Define a variable for the course course analytics api url
        coreTargetCourseApiUrl = ""

        ## If there is a parent course of the course in question
        if parentStuEnrolledCourseId:
                    
            ## Set the course analytics API url with the parent course sis id
            coreTargetCourseApiUrl = f"{coreCoursesApiUrl}//sis_course_id:{parentStuEnrolledCourseId}//"

        ## Otherwise, set it with the target course sis id
        else:
            coreTargetCourseApiUrl = f"{coreCoursesApiUrl}//sis_course_id:{targetCourseSisId}//"

        ## Define the course analystics user assignments url
        stuAssignmentSubmissionAnalyticsUrl = f"{coreTargetCourseApiUrl}analytics//users//sis_user_id:{p2_stuID}//assignments"

        ## Make an analytics api call to get the users submissions
        stuAssignmentSubmissionAnalyticsObject = None
            
        ## Define a variable to track the number of attempts to get the api call
        stuAssignmentSubmissionAnalyticsApiAttempts = 0

        ## If the api call was not sucessful and the number of secondaryCoreCourseApiAttempts is less than 5
        while (stuAssignmentSubmissionAnalyticsObject == None
               or (stuAssignmentSubmissionAnalyticsObject.status_code != 200 
                   and stuAssignmentSubmissionAnalyticsApiAttempts < 5)
               ):

            try: ## Irregular try clause, do not comment out in testing

                ## If the last api attempt num is greater than 0
                if stuAssignmentSubmissionAnalyticsApiAttempts > 0:
                    
                    ## Wait for two seconds if the api call was previously unsucessful
                    time.sleep(2)
                    
                    ## Log that the api call is being tried again
                    logger.info(f"Retrying api call to {stuAssignmentSubmissionAnalyticsUrl} for canvas_id: {p1_stuCanvasId} \n attempt #{stuAssignmentSubmissionAnalyticsApiAttempts + 1}")

                    ## Close the previous api object
                    if stuAssignmentSubmissionAnalyticsObject:
                        stuAssignmentSubmissionAnalyticsObject.close()
                
                ## try the api call again
                stuAssignmentSubmissionAnalyticsObject = requests.get(url=stuAssignmentSubmissionAnalyticsUrl, headers = header)
                
            except Exception as error: ## Irregular except clause, do not comment out in testing
                logger.warning(f"Error: {error} \n Occured when calling {stuAssignmentSubmissionAnalyticsUrl} for canvas_id: {p1_stuCanvasId}")

            ## Increment the number of attempts
            stuAssignmentSubmissionAnalyticsApiAttempts += 1

        ## If the api call was sucessful
        if stuAssignmentSubmissionAnalyticsObject.status_code == 200:

            ## Convert the json api reponse to a dictionary
            stuAssignmentSubmissionAnalyticsDict = json.loads(stuAssignmentSubmissionAnalyticsObject.text)

            ## Close the api object
            stuAssignmentSubmissionAnalyticsObject.close()

            ## Iterate through the submissions
            for submission in stuAssignmentSubmissionAnalyticsDict:

                if ("submission" not in submission.keys()):
                    continue

                currentSubmissionSubmittedAtDate = ""

                if submission["submission"]["submitted_at"]:
                    currentSubmissionSubmittedAtDate = datetime.strptime(submission["submission"]["submitted_at"], "%Y-%m-%dT%H:%M:%SZ")

                ## If the submission status is missing
                if submission["status"] == "missing":

                    ## Increment the number of missing assignments i
                    stuNumOfMissedAssignments += 1

                ## If a score has been posted and the score given is 0
                if "submission" in submission.keys() and submission["submission"]["score"] == 0:

                    ## Increment the number of assignments graded 0
                    stuNumOfAssignmentsGradedZero += 1

                ## Find the most recent submission date
                if (currentSubmissionSubmittedAtDate) and (not (stuLastSubmissionDateTime) or stuLastSubmissionDateTime < currentSubmissionSubmittedAtDate):
                            
                    ## Record the current submission date as the student's last submission date
                    stuLastSubmissionDateTime = currentSubmissionSubmittedAtDate
                    
        ## Create a variable to hold the converted last course activity date
        ## Converted to month day format instead of the whole date time value
        ## Converted to month day format instead of the whole date time value
        convertedStuLastCourseActivty = ""
                
        ## If the student has a last submission date
        if stuLastSubmissionDateTime:
                    
            ## If there is a last course activity
            if stuLastCourseActivty:
                        
                ## If the last submission date is more recent than the last course activity
                if stuLastSubmissionDateTime >= stuLastCourseActivty:
                    
                    ## Record the last submission date as the last course activity in month day format
                    convertedStuLastCourseActivty = stuLastSubmissionDateTime.strftime("%m-%d") ## The last submisison date is can be more recent than the last activity if they have submitted something in person since they last accessed the course
                
        ## If there is no converted stu last course activity date
        if not convertedStuLastCourseActivty:
                    
            ## If there is a last course activity date
            if stuLastCourseActivty:
                        
                ## Record the last course activity date in month day format
                convertedStuLastCourseActivty = stuLastCourseActivty.strftime("%m-%d")

        stuLastGradedDiscussionPostDateTime = ""
                
        ####################################################################################################

        ## Make an api call to get the student's activity in order to see if the student has a more recent 
        ## participation than their last submission

        ####################################################################################################

        ## Define the course analytics user activity api url
        stuActivityAnalyticsUrl = f"{coreTargetCourseApiUrl}analytics//users//sis_user_id:{p2_stuID}//activity"

        ## Make an analytics api call to get the users activity
        stuActivityAnalyticsObject = None

        ## Define a variable to track the number of attempts to get the api call
        stuActivityAnalyticsApiAttempts = 0

        ## If the api call was not sucessful and the number of secondaryCoreCourseApiAttempts is less than 5
        while (not stuActivityAnalyticsApiAttempts
               or (stuActivityAnalyticsObject.status_code != 200 
                   and stuActivityAnalyticsApiAttempts < 5)
               ):

            try: ## Irregular try clause, do not comment out in testing

                ## If the last api attempt num is greater than 0
                if stuActivityAnalyticsApiAttempts > 0:
                    
                    ## Wait for two seconds if the api call was previously unsucessful
                    time.sleep(2)
                    
                    ## Log that the api call is being tried again
                    logger.info(f"Retrying api call to {stuActivityAnalyticsUrl} for canvas_id: {p1_stuCanvasId}")

                    ## Close the previous api object
                    if stuActivityAnalyticsObject:
                        stuActivityAnalyticsObject.close()
                
                ## try the api call again
                stuActivityAnalyticsObject = requests.get(url=stuActivityAnalyticsUrl, headers = header)

            except Exception as error: ## Irregular except clause, do not comment out in testing
                logger.warning(f"Error: {error} \n Occured when calling {stuActivityAnalyticsUrl} for canvas_id: {p1_stuCanvasId}")

            ## Increment the number of attempts
            stuActivityAnalyticsApiAttempts += 1

        ## If the api call was sucessful
        if stuActivityAnalyticsObject.status_code == 200:

            ## Convert the json api reponse to a dictionary
            stuActivityAnalyticsDict = json.loads(stuActivityAnalyticsObject.text)

            ## Close the api object
            stuActivityAnalyticsObject.close()
                    
            ## Seperate out the participations dicts
            stuParticipationDict = stuActivityAnalyticsDict["participations"]

            ## If the student participation dict isn't emtpy
            if stuParticipationDict:

                ## Seperate the number associated with the last participation and its data
                stuLastParticipationNumber, stuCanvasLastParticipationDate = list(stuParticipationDict[-1].items())[0]

                ## Convert last participation date to datetime
                stuCanvasLastParticipationDate = datetime.strptime(stuCanvasLastParticipationDate, "%Y-%m-%dT%H:%M:%SZ")

                ## If their is no last submission date or if last participation date is more recent than the last submission date
                if not stuLastSubmissionDateTime or stuLastSubmissionDateTime < stuCanvasLastParticipationDate:
                            
                    ###################################################################################

                    ## Make an api call to look through the student's discussion posts to determine whether 
                    ## the list most recent participation was a discussion post to an ungraded discussion.
                    ## Only graded discussions count for NNU participation purposes, so if the most recent 
                    ## participation was in a non-graded discussion find the most recent graded participation
                    ## date.

                    ##################################################################################
                            
                    ## Define the conversations api url
                    stuDiscussionListAPIUrl = f"{coreTargetCourseApiUrl}discussion_topics"

                    stuLastGradedDiscussionPostDate = getStuMostRecentGradedDiscussionPostDate(p1_stuDiscussionListAPIUrl = stuDiscussionListAPIUrl, p1_stuCanvasId = p1_stuCanvasId)

                    if stuLastGradedDiscussionPostDate:
                        stuLastGradedDiscussionPostDateTime = datetime.strptime(stuLastGradedDiscussionPostDate, "%Y-%m-%dT%H:%M:%SZ")

        convertedStuLastParticipationDate = ""
                                        
        ## If there is both a last submission date and a last graded discussion post date
        if stuLastSubmissionDateTime and stuLastGradedDiscussionPostDateTime:

            ## If the last submission date is more recent than the last graded discussion post date
            if stuLastSubmissionDateTime >= stuLastGradedDiscussionPostDateTime:

                ## Record the last submission date as the last participation date
                convertedStuLastParticipationDate = stuLastSubmissionDateTime.strftime("%m-%d")
                        
            ## Otherwise
            else:

                ## Record the last graded discussion post date as the last participation date
                convertedStuLastParticipationDate = stuLastGradedDiscussionPostDateTime.strftime("%m-%d")
                
        ## Otherwise, if there is only a last submission date
        elif stuLastSubmissionDateTime:

            ## Record the last submission date as the last participation date
            convertedStuLastParticipationDate = stuLastSubmissionDateTime.strftime("%m-%d")
                    
        ## If the student's last participation date is more recent than their last course activity date
        if convertedStuLastParticipationDate > convertedStuLastCourseActivty:

            ## Record the last participation date as the last course activity date
            convertedStuLastCourseActivty = convertedStuLastParticipationDate
                
        ## Record the student's course activity date, last participation date, their number of missing assignments, and their number of assignments graded 0     
        p2_stuCoursesData   [targetCourseSisId]   ["Last Course Activity Date"]         = f"{convertedStuLastCourseActivty}"
        p2_stuCoursesData   [targetCourseSisId]   ["Last Course Participation"]         = f"{convertedStuLastParticipationDate}"
        p2_stuCoursesData   [targetCourseSisId]   ["Number of Missing Assignments"]     = f"{stuNumOfMissedAssignments}"
        p2_stuCoursesData   [targetCourseSisId]   ["Number of Assignments graded 0"]    = f"{stuNumOfAssignmentsGradedZero}"
        
        ## If the last course activity date is more recent than the stuCoursesData's last canvas activity date
        if p2_stuCoursesData["Last Canvas Activity"] < convertedStuLastCourseActivty:
            
            ## Update the stuCoursesData's last canvas activity date
            p2_stuCoursesData["Last Canvas Activity"] = convertedStuLastCourseActivty

    except Exception as error:
        error_handler (functionName, p1_ErrorInfo = error)

## This threaded function gets each student's course data related to each of their enrollments for the current term
def getStuCurrentCoursesData (p1_stuID
                              , p1_stuCoursesDataDict
                              , p1_filteredCombinedCanvasEnrollmentsDF
                              , p1_combinedUnpublishedCanvasCoursesList
                              , p1_filteredSisEnrollmentsDF
                              ):
    functionName = "Get Stu Current Course Data"
    
    try:

        ## Retreive the student's SIS course enrollment list
        stuSisEnrolledCourseIds = p1_filteredSisEnrollmentsDF[p1_filteredSisEnrollmentsDF["user_id"] == p1_stuID]["course_id"]

        ## Remove any duplicates from the list
        stuSisEnrolledCourseIds = list(set(stuSisEnrolledCourseIds))

        ## Retrieve the student's Canvas course enrollment list
        stuCanvasEnrolledCourseIds = p1_filteredCombinedCanvasEnrollmentsDF[p1_filteredCombinedCanvasEnrollmentsDF["user_id"] == p1_stuID]

        loopCounter = 0
        
        ## While there is are still course data sets that don't have a published data key and the loop counter is less than 10
        while (
            not all(
                "Published" in stuCourseData
                for stuCourseData in p1_stuCoursesDataDict.values()
                if isinstance(stuCourseData, dict)
            )
            and loopCounter < 10
            ):
            
            loopCounter += 1

            ## Define a list to contain ongoing getStuCourseData threads
            ongoingThreads = []

            ## For each course that the student ID is enrolled in
            for targetStuSisEnrolledCourseId in stuSisEnrolledCourseIds:

                ## If there is no published key in the student's course data dict
                if "Published" not in p1_stuCoursesDataDict[targetStuSisEnrolledCourseId].keys():

                    ## Create a thread to get the student's course data
                    getStuCourseDataThread = threading.Thread(
                        target=getStuCourseData
                        , args = (
                            p1_stuID
                            , stuSisEnrolledCourseIds
                            , p1_stuCoursesDataDict
                            , stuCanvasEnrolledCourseIds
                            , p1_stuCoursesDataDict["stuCanvasId"]
                            , targetStuSisEnrolledCourseId
                            , p1_combinedUnpublishedCanvasCoursesList
                            ),
                        )

                    ## Start the thread
                    getStuCourseDataThread.start()

                    ## Add the thread to the ongoing threads list
                    ongoingThreads.append(getStuCourseDataThread)
            
                    # ## Get the student's course data
                    # getStuCourseData(p2_stuID = p1_stuID
                    #                  , p2_stuCoursesData = p1_stuCoursesDataDict
                    #                  , p1_stuCanvasEnrolledCourseIds = stuCanvasEnrolledCourseIds
                    #                  , p1_stuCanvasId = p1_stuCoursesDataDict['stuCanvasId']
                    #                  , targetCourseSisId = targetStuSisEnrolledCourseId
                    #                  , p2_combinedUnpublishedCanvasCoursesList = p1_combinedUnpublishedCanvasCoursesList
                    #                  )

            ## For each ongoing thread
            for thread in ongoingThreads:
                
                ## Wait for the thread to finish
                thread.join()

        ## Remove any courses that don't have a published key
        #p1_stuCoursesDataDict = {key: value for key, value in p1_stuCoursesDataDict.items() if "Published" in value.keys()}
        p1_stuCoursesDataDict = {key: value for key, value in p1_stuCoursesDataDict.items() if not isinstance(value, dict) or "Published" in value.keys()}

        ## Log that the student's course data retrieval has been completed
        logger.info (f"{p1_stuID} completed")
                                    
                        
    except Exception as error:
        error_handler (functionName, p1_ErrorInfo = error)

## This function retrieves the student's last Canvas access report data point
def retrieveStuLastCanvasAccessReportDataPoint(p2_stuID):
    functionName = "Retrieve Stu Last Canvas Access Report Data Point"
    
    try:
        
        ## Make sure the last Canvas access file is up to date and retrieve it if it isn't, and then load it into a df
        lastCanvasAccessDF = pd.read_csv(termGetCanvasUserLastAccess())
        
        ## Retrieve the users most recent Canvas Activity Date
        stuLastActivityDF = lastCanvasAccessDF[
            lastCanvasAccessDF[
                "user sis id"
                ] == str(
                    p2_stuID
                    )
            ]["last access at"]
        
        ## Convert the dataframe to a list
        stuLastActivityList = stuLastActivityDF.tolist()
        
        ## Create a variable to hold the converted last canvas activity date
        convertedStuLastCanvasActivity = ""
        
        ## If the student doesn't have has any last activity dates
        if not stuLastActivityList or str(stuLastActivityList[0]) == "nan":

            ## Return an empty string
            return ""

        ## Otherwise
        else:
            
            ## Convert the last activity date to a datetime object
            rawStuLastActivity = datetime.strptime(str(stuLastActivityList[0]), "%Y-%m-%dT%H:%M:%S%z")
            
            ## Convert the last activity date to a month day format
            convertedStuLastCanvasActivity = rawStuLastActivity.strftime("%m-%d")

            ## Return the converted last activity date
            return convertedStuLastCanvasActivity
            
                
    except Exception as error:
        error_handler (functionName, p1_ErrorInfo = error)            

## This function retrieves and returns a dict of students' ids and last Canvas access report data points
def retrieveListOfStuLastCanvasAccessReportDataPoints(stuIdsList : list):
    functionName = "Retrieve Stu Last Canvas Access Report Data Point"
    
    try:
    
        ## Make sure the last Canvas access file is up to date and retrieve it if it isn't, and then load it into a df
        lastCanvasAccessDF = pd.read_csv(termGetCanvasUserLastAccess())
    
        ## Initialize a dictionary to hold the last Canvas access data for each student
        lastCanvasAccessData = {}
    
        ## Iterate through the list of student IDs
        for student_id in stuIdsList:
            
            ## Retrieve the user's most recent Canvas Activity Date
            stuLastActivityDF = lastCanvasAccessDF[lastCanvasAccessDF["user sis id"] == str(student_id)]["last access at"]
        
            ## Convert the DataFrame to a list
            stuLastActivityList = stuLastActivityDF.tolist()
        
            ## Initialize the converted last canvas activity date
            convertedStuLastCanvasActivity = ""
        
            ## If the student doesn't have any last activity dates
            if not stuLastActivityList or str(stuLastActivityList[0]) == "nan":
                
                ## Add an empty string to the dictionary
                lastCanvasAccessData[student_id] = ""
                
            ## Otherwise
            else:
                
                ## Convert the last activity date to a datetime object
                rawStuLastActivity = datetime.strptime(str(stuLastActivityList[0]), "%Y-%m-%dT%H:%M:%S%z")
            
                ## Convert the last activity date to a month-day format
                convertedStuLastCanvasActivity = rawStuLastActivity.strftime("%m-%d")
            
                ## Add the converted last activity date to the dictionary
                lastCanvasAccessData[student_id] = convertedStuLastCanvasActivity
    
        ## Return the last Canvas access data
        return lastCanvasAccessData
            
                
    except Exception as error:
        error_handler (functionName, p1_ErrorInfo = error) 

## This threaded function gets each student's courses and adds them as keys with empty dicts to the student's course data dict
def getStuCoursesData (p1_stuID
                       , p1_stuCoursesData
                       , p1_filteredCombinedCanvasEnrollmentsDF
                       , p1_filteredSisEnrollmentsDF
                       ):       
    
    functionName = "Get Stu Courses"
    
    try:
        
        ## Make a df of any canvas user ids that match the student id
        stuCanvasIdDf = p1_filteredCombinedCanvasEnrollmentsDF[
            p1_filteredCombinedCanvasEnrollmentsDF["user_id"].astype(int) == int(p1_stuID)
            ]["canvas_user_id"]
        
        ## try to get the student's canvas id
        if not stuCanvasIdDf.empty:
            p1_stuCoursesData["stuCanvasId"] = stuCanvasIdDf.values[0]
        
        ## If no canvas ID is found
        if not p1_stuCoursesData or not p1_stuCoursesData["stuCanvasId"]:
            return

        ## Retreive the student's SIS course enrollment list
        stuSisEnrolledCourseIds = p1_filteredSisEnrollmentsDF[p1_filteredSisEnrollmentsDF["user_id"] == p1_stuID]["course_id"]

        ## Remove any duplicates from the list
        stuSisEnrolledCourseIds = list(set(stuSisEnrolledCourseIds))

        # ## Retrieve the student's Canvas course enrollment list
        # stuCanvasEnrolledCourseIds = p1_filteredCombinedCanvasEnrollmentsDF[p1_filteredCombinedCanvasEnrollmentsDF["user_id"] == p1_stuID]

        ## For each course that the student ID is enrolled in
        for targetCourseSisId in stuSisEnrolledCourseIds:

            ## If this is a double enrollment or if it is a chapel course
            if targetCourseSisId in p1_stuCoursesData.keys() or "CHPL1000_01" in targetCourseSisId:

                ## Skip it
                return

            ## Create a variable to hold the parent course sis id of crosslisted courses when neccessary
            parentStuEnrolledCourseId = ""

            ## Otherwise create a dict attached to the stu enrolled course id within the student data dict
            p1_stuCoursesData[targetCourseSisId] = {}
            
    except Exception as error:
        error_handler (functionName, p1_ErrorInfo = error)       

## This function takes a list of current NNU enrollments and gets their Canvas enrollment related activity and grade information
def getNighthawk360Data ():
    functionName = "Get Night Hawk 360 Data"

    try:
        ## Determine the current term based off current date\
        ## Determine the target term based off the target month
        currentTermCodes = []
        currentTerms = []
        currentSchoolYear = None

        ## January through May makes the current terms the Spring Terms
        if currentMonth >= 1 and currentMonth <= 5:
            currentTermCodes.append(f"SP{decade}")
            currentTermCodes.append(f"GS{decade}")
            currentTerms.append(f"SP{currentYear}")
            currentTerms.append(f"GS{currentYear}")

        ## June through August makes the current terms the Summer Terms
        elif currentMonth >= 6 and currentMonth <= 8:
            currentTermCodes.append(f"SU{decade}")
            currentTermCodes.append(f"SG{decade}")
            currentTerms.append(f"SU{currentYear}")
            currentTerms.append(f"SG{currentYear}")

        ## September through December (the rest of the months) makes the current terms the Spring Terms
        else:
            currentTermCodes.append(f"FA{decade}")
            currentTermCodes.append(f"GF{decade}")
            currentTerms.append(f"FA{currentYear}")
            currentTerms.append(f"GF{currentYear}")

        ## Deteremine the current schoolyear based off of the current term
        if currentTermCodes[0] == f"FA{decade}":
            ## Fall terms are the first terms of a new school year so FA20 is part of the 2020-21 school year.
            currentSchoolYear = f"{century}{decade}-{int(decade) + 1}"
        else:
            ## Spring and Summer terms belong in the same school year as the fall terms before them, so SP21 is part of the same 2020-21 schooal year as FA20.
            currentSchoolYear = f"{century}{int(decade) - 1}-{decade}"

        ## Save the school year based canvas input path
        schoolYearLocalInputPath = f"{baseLocalInputPath}{currentSchoolYear}\\"

        ## Canvas Enrollment
        
        ## Get the current undergraduate Canvas enrollment file
        undgCanvasEnrollmentsDF = pd.read_csv(f"{schoolYearLocalInputPath}{currentTermCodes[0]}\\{currentTermCodes[0]}_Canvas_Enrollments.csv")

        ## Get the current graduate Canvas enrollment file
        gradCanvasEnrollmentsDF = pd.read_csv(f"{schoolYearLocalInputPath}{currentTermCodes[1]}\\{currentTermCodes[1]}_Canvas_Enrollments.csv")

        ## Combine the undergraduate and graduate enrollment files
        combinedCanvasEnrollmentsDF = pd.concat([undgCanvasEnrollmentsDF, gradCanvasEnrollmentsDF], ignore_index=True)
        
        ## Filter so that only rows that have a student role, are active, and are not in the chapel course are retained
        filteredCombinedCanvasEnrollmentsDF = combinedCanvasEnrollmentsDF[((combinedCanvasEnrollmentsDF['status'] == 'active')
                                                                           | (combinedCanvasEnrollmentsDF['status'] == 'concluded')
                                                                           )
                                                                          & (combinedCanvasEnrollmentsDF['role'] == 'student')
                                                                          & (~combinedCanvasEnrollmentsDF['course_id'].str.contains('CHPL1000_01'))
                                                                          & (combinedCanvasEnrollmentsDF['course_id'].str.contains(currentTerms[0]) 
                                                                             | combinedCanvasEnrollmentsDF['course_id'].str.contains(currentTerms[1])
                                                                             )
                                                                          ]

        ## Get the current undergraduate Canvas unpublished courses file
        undgUnpublishedCanvasCoursesDF = pd.read_csv(f"{schoolYearLocalInputPath}{currentTermCodes[0]}\\{currentTermCodes[0]}_Canvas_Unpublished_Courses.csv")

        ## Get the current graduate Canvas unpublished courses file
        gradUnpublishedCanvasCoursesDF = pd.read_csv(f"{schoolYearLocalInputPath}{currentTermCodes[1]}\\{currentTermCodes[1]}_Canvas_Unpublished_Courses.csv")

        ## Combine the undergraduate and graduate unpublished courses files
        combinedUnpublishedCanvasCoursesDF = pd.concat([undgUnpublishedCanvasCoursesDF, gradUnpublishedCanvasCoursesDF], ignore_index=True)

        ## Make a list of the unpublished courses
        combinedUnpublishedCanvasCoursesList = combinedUnpublishedCanvasCoursesDF["sis id"].tolist()
        
        ## SIS enrollment

        ## Get the current institution enrollment data
        sisEnrollmentsDF = pd.read_csv(f"{baseExternalInputPath}canvas_enroll.csv")
        
        ## Filter the current institution enrollment data to only retain the student enrollments
        filteredSisEnrollmentsDF = sisEnrollmentsDF[((sisEnrollmentsDF['status'] == 'active')
                                                     | (sisEnrollmentsDF['status'] == 'concluded')
                                                     )
                                                    & (sisEnrollmentsDF['role'] == 'student')
                                                    & (~sisEnrollmentsDF['course_id'].str.contains('CHPL1000_01'))
                                                    & (sisEnrollmentsDF['course_id'].str.contains(currentTerms[0])
                                                       | sisEnrollmentsDF['course_id'].str.contains(currentTerms[1])
                                                       )
                                                    ]
        
        ## Determine the unique student ids within the SIS enrollment Dataframe
        uniqueStuIdDF = filteredSisEnrollmentsDF['user_id'].unique()

        ## Create a dict of the students' ids and their last Canvas access report data points
        stuLastCanvasAccessData = retrieveListOfStuLastCanvasAccessReportDataPoints(uniqueStuIdDF.tolist())

        ## Define a dictionary with each unique ID as a key and include the last Canvas access data
        studentDataDict = {stuID: {'Last Canvas Activity': stuLastCanvasAccessData.get(stuID, '')} for stuID in uniqueStuIdDF}

        ## For each unique student id
        for stuID, stuCoursesData in studentDataDict.items():

            getStuCoursesData(stuID
                              , stuCoursesData
                              , filteredCombinedCanvasEnrollmentsDF
                              , filteredSisEnrollmentsDF
                              )

        ## Define a loop counter
        loopCounter = 0

        ## Define a list to hold the enrollment data threads
        ongoingReportThreads = []
        
        ## While the "Published" key is not in every stu courses data dict within each student data dict and the loop counter is less than 100
        ## (Every stu course pair should at least have published data so this acts as a check to see if all the data has been collected)
        while (
            not all(
                "Published" in stuCourseData
                for stuData in studentDataDict.values()
                for stuCourseData in stuData.values()
                if isinstance(stuCourseData, dict)
            )
            and loopCounter < 100
        ):

            if loopCounter == 95:
                
                print (1)

            ## For each student data dict
            # for stuData in studentDataDict.values():
           
            #     ## For each stu course data dict
            #     for stuCourseData in stuData.values():
                
            #         ## If the stu course data is a dict and it is empty
            #         if isinstance(stuCourseData, dict) and not stuCourseData:
                   
            #             ## Delete the stu course data dict
            #             del stuData[stuCourseData]

            ## Increment the loop counter
            loopCounter += 1

            ## Clear the list of any previous ongoing threads
            ongoingReportThreads.clear()
            
            ## Create a variable to hold how many threads have been started
            threadCounter = 0
            
            ## Create a variable to count the number of hundreds of threads that have been completed
            tensCompletedCounter = 0

            # For each unique student id
            for stuID, stuCoursesData in studentDataDict.items():

                #if stuID == 718266:
                
                    ## For each 
                    # If there is not a published key in all of the student's course data dicts
                    if not all(
                        "Published" in stuCourseData
                        for stuCourseData in stuCoursesData.values()
                        if isinstance(stuCourseData, dict)
                    ):
                    
                        ## Define the stuEnrollmentDataThread as None to ensure a brand new thread is created
                        stuEnrollmentDataThread = None

                        # Define the stuEnrollmentDataThread
                        stuEnrollmentDataThread = threading.Thread(
                            target=getStuCurrentCoursesData
                            , args=(
                                stuID,
                                stuCoursesData,
                                filteredCombinedCanvasEnrollmentsDF,
                                combinedUnpublishedCanvasCoursesList,
                                filteredSisEnrollmentsDF,
                            ),
                        )

                        # Start the term related syllabi report thread
                        stuEnrollmentDataThread.start()

                        # Add the term related syllabi report thread to the list of ongoing threads
                        ongoingReportThreads.append(stuEnrollmentDataThread)
                    
                        ## Increment the thread counter
                        threadCounter += 1
                    
                        ## If the thread counter is greater than 100
                        if threadCounter >= 100:
                        
                            ## For each ongoing thread
                            for thread in ongoingReportThreads:
                            
                                ## Wait for the thread to finish
                                thread.join()

                                ## Remove the thread from the ongoing threads list
                                ongoingReportThreads.remove(thread)
                            
                            ## Increment the tens Completed Counter
                            tensCompletedCounter += 10
                                
                            ## Log the number of threads that have been completed
                            logger.info(f"{tensCompletedCounter}0 threads have been completed")
                            
                            ## Reset the thread counter
                            threadCounter = 0

            # Wait until all ongoing threads have completed
            for thread in ongoingReportThreads:
                thread.join()

            ## Log the number of threads that have been completed
            logger.info(f"{tensCompletedCounter * 10 + threadCounter} threads have been completed")    
        
        ## Return the completed student data dict
        return studentDataDict
                    
    except Exception as error:
        error_handler (functionName, p1_ErrorInfo = error)

    ## Return the final product even if there was an error somewhere along the line
    #finally:
        #return finalProduct

## This function contains the start and end of the NightHawk 360 data report
def Nighthawk360CanvasReport ():
    functionName = "Nighthawk 360 Canvas Report"

    try:
        logger.info (f"     \nBeginning the Nighthawk 360 Canvas Report")
        
        ## Get the current institution enrollment data
        enrollmentDataDict = getNighthawk360Data()
        
        # ## Create a list to hold the flattened enrollment data
        # flattenedEnrollmentData = []

        # ## For each student ID and their courses in the enrollment data dict
        # for student_id, courses in enrollmentDataDict.items():
            
        #     ## Save the last canvas activity date from the courses dict
        #     lastCanvasActivity = courses['Last Canvas Activity']
            
        #     ## For each course ID and course data in the student's courses
        #     for course_id, course_data in courses.items():
                
        #         ## If the course data is a dict
        #         if isinstance(course_data, dict):
                    
        #             ## Add the student ID and course ID to the course data dict
        #             flattenedEnrollmentData.append({
        #                 'student_id': student_id,
        #                 'course_id': course_id,
        #                 'Last Canvas Activity': lastCanvasActivity,
        #                 **course_data
        #             })
        
        # ## Convert flattenedEnrollmentData to a DataFrame for comparison
        # currentEnrollmentDataDf = pd.DataFrame(flattenedEnrollmentData)

        # ## Open the current enrollment data activity and submission pipe delineated csv files as dataframes
        # oldEnrollmentDataActivityDf = pd.read_csv(f"{baseLocalOutputPath}Enrollment_Data_Activity.csv", sep="|")
        # oldEnrollmentDataSubmissionsDf = pd.read_csv(f"{baseLocalOutputPath}Enrollment_Data_Submissions.csv", sep="|")
        
        # ## Combine old data into a single DataFrame for comparison
        # oldEnrollmentDataDf = pd.concat([oldEnrollmentDataActivityDf, oldEnrollmentDataSubmissionsDf])

        # ## Rename the columns to match the current data
        # oldEnrollmentDataDf.rename(columns={
        #     'Student ID': 'student_id'
        #     , 'Course Number': 'course_id'
        #     , 'Published': 'published'
        # }, inplace=True)

        # # Find the differences between the old and current data
        # rawMissingEnrollmentsDf = oldEnrollmentDataDf[
        #     ~oldEnrollmentDataDf.apply(
        #         lambda row: any(
        #             (currentEnrollmentDataDf['student_id'] == row['student_id']) 
        #             & (currentEnrollmentDataDf['course_id'] == row['course_id'])
        #             )
        #         , axis=1
        #         )
        #     ]
        
        # ## Open the SIS course feed file from the baseExternalInputPath
        # sisCourseFeedDF = pd.read_csv(f"{baseExternalInputPath}canvas_course.csv")

        # ## Filter out any missing enrollments that are not in the SIS course feed
        # filteredMissingEnrollmentsDf = rawMissingEnrollmentsDf[
        #     rawMissingEnrollmentsDf['course_id'].isin(sisCourseFeedDF['course_id'])
        #     ]
        
        # ## Add the missing enrollments to the relevant student IDs' data points
        # for index, row in filteredMissingEnrollmentsDf.iterrows():
            
        #     ## Define the student ID and course ID
        #     studentId = row['student_id']
        #     courseId = row['course_id']
    
        #     ## Check if the student_id exists in the current data dict
        #     if student_id in enrollmentDataDict.keys():
                
        #         ## Set the published status to "Unenrolled"
        #         row['Published'] = "Student Unenrolled"
                
        #         ## For each key and value in the row
        #         for courseDataPoint, courseDataPointValue in row.items():

        #             ## Add the course data point to the student's course data dict
        #             enrollmentDataDict[studentId][courseId][courseDataPoint] = courseDataPointValue
                        
        #     ## Otherwise  
        #     else:
                
        #         ## Create a new student data dict item
        #         enrollmentDataDict[studentId] = {courseId: row.to_dict()}

        #         ## Set the published status to "Unenrolled"
        #         enrollmentDataDict[studentId][courseId]['Published'] = "Student Unenrolled"

        #         ## Retrieve and record the student's last canvas activity date
        #         enrollmentDataDict[studentId]["Last Canvas Activity"] = retrieveStuLastCanvasAccessReportDataPoint(studentId)

        with (
            open(fr"{baseLocalOutputPath}Enrollment_Data_Activity.csv"
                 , 'w'
                 , newline=''
                 ) 
            as activityCsv
            , open(fr"{baseLocalOutputPath}Enrollment_Data_Submissions.csv"
                   , 'w'
                   , newline=''
                   ) 
            as submissionCsv
            ):

            activityCsv.write("\"Student ID\"")
            activityCsv.write("|\"Course Number\"")
            activityCsv.write("|\"Last Canvas Activity\"") 
            activityCsv.write("|\"Published\"")                           
            activityCsv.write("|\"Last Course Activity\"")                
            activityCsv.write("|\"Last Course Participation\"\n")
            
            submissionCsv.write("\"Student ID\"")                      
            submissionCsv.write("|\"Course Number\"")                   
            submissionCsv.write("|\"Current Grade\"")                   
            submissionCsv.write("|\"Number of Missed Assignments\"")    
            submissionCsv.write("|\"Number of 0 Grades\"\n")        
            
            ## Iterate through the completed enrollment data and save it in dataframe compatible format
            for stuID, dataPoints in enrollmentDataDict.items():
                
                # testList = [132928,
                #             161968,
                # ]

                for dataPoint in dataPoints:
                    ## If the target datapoint is the last course activity (as opposed to a course) 
                    ## or if it it is an empty dict,
                    if (dataPoint == "Last Canvas Activity" 
                        or dataPoint == "stuCanvasId" 
                        or not dataPoint
                        ):
                        
                        ## Skip it
                        continue
                    
                    else:
                        # if stuID in testList:
                        #     print ("hey")

                        ## Create data point variables to hold the student ID and course code
                        formattedStuID                      = "\"" + str(stuID)                                                  + "\""
                        formattedCourseCode                 = "|\"" + dataPoint.replace('_', '-')                                + "\""

                        formattedLastCanvasActivity         = "|\"\""
                        formattedPublished                  = "|\"\""
                        formattedLastCourseActivity         = "|\"\""
                        formattedLastCourseParticipation    = "|\"\"\n"

                        formattedCurrentGrade               = "|\"\""
                        formattedNumberOfMissingAssignments = "|\"\""
                        formattedNumberOf0Grades            = "|\"\"\n"

                        ## try to format the data points for the csv
                        try: ## Irregular try clause, do not comment out in testing
                        
                            formattedStuID                      = "\"" + str(stuID)                                                  + "\""
                            formattedCourseCode                 = "|\"" + dataPoint.replace('_', '-')                                + "\""

                            formattedLastCanvasActivity         = "|\"" + dataPoints             ["Last Canvas Activity"]            + "\""
                            formattedPublished                  = "|\"" + dataPoints [dataPoint] ["Published"]                       + "\""
                            formattedLastCourseActivity         = "|\"" + dataPoints [dataPoint] ["Last Course Activity Date"]       + "\""
                            formattedLastCourseParticipation    = "|\"" + dataPoints [dataPoint] ["Last Course Participation"]       + "\"\n"

                            formattedCurrentGrade               = "|\"" + dataPoints [dataPoint] ["Current Grade"]                   + "\""
                            formattedNumberOfMissingAssignments = "|\"" + dataPoints [dataPoint] ["Number of Missing Assignments"]   + "\""
                            formattedNumberOf0Grades            = "|\"" + dataPoints [dataPoint] ["Number of Assignments graded 0"]  + "\"\n"

                        except Exception as error: ## Irregular except clause, do not comment out in testing

                            ## Log a warning that an error occured while processing the data point
                            logger.warning(f"Error: {error} \n Occured while processing {dataPoint}:{dataPoints[dataPoint]} for Student ID: {str(stuID)}")

                            error_handler (functionName, p1_ErrorInfo = f"{error} \n Occured while processing {dataPoint}:{dataPoints[dataPoint]}")

                        ## Record activity data
                                                  
                        activityCsv.write (formattedStuID)               
                        activityCsv.write (formattedCourseCode)  
                        activityCsv.write (formattedLastCanvasActivity)
                        activityCsv.write (formattedPublished)
                        activityCsv.write (formattedLastCourseActivity)
                        activityCsv.write (formattedLastCourseParticipation)

                        ## Record submission data
                        submissionCsv.write (formattedStuID)
                        submissionCsv.write (formattedCourseCode)
                        submissionCsv.write (formattedCurrentGrade)
                        submissionCsv.write (formattedNumberOfMissingAssignments)
                        submissionCsv.write (formattedNumberOf0Grades)

        ## Copy the converted external csv files to the external output path folder location
        shutil.copy(fr"{baseLocalOutputPath}Enrollment_Data_Activity.csv", fr"{baseExternalOutputPath}Enrollment_Data_Activity.csv")
        shutil.copy(fr"{baseLocalOutputPath}Enrollment_Data_Submissions.csv", fr"{baseExternalOutputPath}Enrollment_Data_Submissions.csv")

        ## Open the newly saved 

        logger.info (f"     \nActivity and Data csvs saved to internal and external paths")



    except Exception as error:
        error_handler (functionName, p1_ErrorInfo = error)

if __name__ == "__main__":

    ## Start the NightHawk 360 data report
    Nighthawk360CanvasReport ()

    input("Press enter to exit")