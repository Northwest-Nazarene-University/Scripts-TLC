# Author: Bryce Miller - brycezmiller@nnu.edu
# Last Updated by: Bryce Miller

## Import Generic Moduels

import traceback, os, sys, logging, requests, json, os, shutil, os.path, threading, time
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
sys.path.append(f"{PFAbsolutePath}Scripts TLC\\ActionModules")

## Import local modules
from Error_Email_API import errorEmailApi
from Get_Canvas_User_Last_Access import termGetCanvasUserLastAccess
from Get_Enrollments import termGetEnrollments
from Get_Unpublished_Courses import termGetUnpublishedCourses
from Make_Api_Call import makeApiCall

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
coreCanvasApiUrl = None
## Open the Core_Canvas_Url.txt from the config path
with open (f"{configPath}Core_Canvas_Url.txt", "r") as file:
    coreCanvasApiUrl = file.readlines()[0]

## Define the core Canvas enrollment API url
coreEnrollmentApiUrl = f"{coreCanvasApiUrl}/accounts/1/enrollments/"

## Define the course Canvas course api url
coreCoursesApiUrl = f"{coreCanvasApiUrl}//courses//"

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

## This variable enables the except function to only send
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
                                     p2_ErrorLocation = p1_ErrorLocation, p2_ErrorInfo = f"{p1_ErrorInfo}: \n\n {traceback.format_exc()}")
        
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
        stuDiscussionListObject = makeApiCall(p1_header = header, p1_apiUrl = p1_stuDiscussionListAPIUrl)

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

                    stuDiscussionViewObject = makeApiCall(p1_header = header, p1_apiUrl = stuDiscussionViewAPIUrl)

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
                    secondaryCoreCourseApiObject = makeApiCall(p1_header = header, p1_apiUrl = secondaryCoreCourseApiUrl)
                                
            ## If stuCourseEnrollmentDataDF is still empty
            if stuCourseEnrollmentDataDF.empty:

                ## Do a warning log that the student is not enrolled in the course
                logger.warning(f"Student {p2_stuID} is not enrolled in course {targetCourseSisId}")

                ## Remove the target course from the stu sis enrolled course ids
                del p2_stuCoursesData[targetCourseSisId]

                ## Skip the course
                return "Remove"

        ## If there is already a Published key in the p2_stuCoursesData dictionary for the target course sis id
        if targetCourseSisId in p2_stuCoursesData.keys() and "Published" in p2_stuCoursesData[targetCourseSisId].keys():

            ## Skip the course as it has already been processed
            return "Completed Unenrolled Entry"

        ## Add key value pairs to hold the other neccessary student course data
        p2_stuCoursesData   [targetCourseSisId]["Published"]                        = "No"
        p2_stuCoursesData   [targetCourseSisId]["Current Grade"]                    = ""
        p2_stuCoursesData   [targetCourseSisId]["Number of Missed Assignments"]    = ""
        p2_stuCoursesData   [targetCourseSisId]["Number of Assignments graded 0"]   = ""
        p2_stuCoursesData   [targetCourseSisId]["Last Course Activity"]        = ""
        p2_stuCoursesData   [targetCourseSisId]["Last Course Participation"]        = ""
        p2_stuCoursesData   [targetCourseSisId]["Number of Missed Assignments"]    = ""
        p2_stuCoursesData   [targetCourseSisId]["Number of Assignments graded 0"]   = ""

        ## If the published key is set to the default value of "No"
        if p2_stuCoursesData[targetCourseSisId]["Published"] == "No":

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

        ## Record whether the enrollment is deleted or not
        enrollmentDeleted = True if stuCourseEnrollmentDataDF["status"].values[0].lower() == "deleted" else False

        ## Use the Canvas enrollment id to define the API url for the related canvas enrollment object
        stuCourseEnrollmentApiUrl = f"{coreEnrollmentApiUrl}{p2_stuCoursesData[targetCourseSisId]['canvas_enrollment_id']}"  

        ## Make an api call with the enrollment ID to get the relevent Canvas enrollment object
        stuCourseEnrollmentObject = None

        ## If enrollmentDeleted is true
        if enrollmentDeleted:

            ## Set the published status to to Unenrolled
            p2_stuCoursesData[targetCourseSisId]["Published"] = "Unenrolled"

            ## Define a parameter payload dict to hold the enrollment data
            stuCourseEnrollmentReenrollmentPayload = {
                "enrollment[user_id]" : f"sis_user_id:{p2_stuID}"
                , "enrollment[type]" : "StudentEnrollment"
                , "enrollment[enrollment_state]" : "active"
            }

            ## Change the course enrollment url
            stuCourseEnrollmentApiUrl = f"{coreCoursesApiUrl}sis_course_id:{targetCourseSisId}//enrollments//"

            ## Make an api call to re-enroll the enrollment
            stuCourseEnrollmentObject = makeApiCall(p1_header = header, p1_apiUrl = stuCourseEnrollmentApiUrl, p1_payload = stuCourseEnrollmentReenrollmentPayload, apiCallType = "post")

        ## Otherwise
        else:
        
            ## Make an api call to get the student's course enrollment data
            stuCourseEnrollmentObject = makeApiCall(p1_header = header, p1_apiUrl = stuCourseEnrollmentApiUrl)

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

        ## Create a placeholder for the student's last canvas activity date, course submission date, their number of missed assignments, and their number of assignments graded 0
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

        ## If the users's enrollment was not deleted

        ## Define the course analystics user assignments url
        stuAssignmentSubmissionAnalyticsUrl = f"{coreTargetCourseApiUrl}analytics//users//sis_user_id:{p2_stuID}//assignments"

        ## Make an analytics api call to get the users submissions
        stuAssignmentSubmissionAnalyticsObject = makeApiCall(p1_header = header, p1_apiUrl = stuAssignmentSubmissionAnalyticsUrl)

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

                ## If the submission status is missed
                if submission["status"] == "missed":

                    ## Increment the number of missed assignments i
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
        stuActivityAnalyticsObject = makeApiCall(p1_header = header, p1_apiUrl = stuActivityAnalyticsUrl)

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
                
        ## Record the student's course activity date, last participation date, their number of missed assignments, and their number of assignments graded 0     
        p2_stuCoursesData   [targetCourseSisId]   ["Last Course Activity"]         = f"{convertedStuLastCourseActivty}"
        p2_stuCoursesData   [targetCourseSisId]   ["Last Course Participation"]         = f"{convertedStuLastParticipationDate}"
        p2_stuCoursesData   [targetCourseSisId]   ["Number of Missed Assignments"]     = f"{stuNumOfMissedAssignments}"
        p2_stuCoursesData   [targetCourseSisId]   ["Number of Assignments graded 0"]    = f"{stuNumOfAssignmentsGradedZero}"
        
        ## If the last course activity date is more recent than the stuCoursesData's last canvas activity date
        if p2_stuCoursesData["Last Canvas Activity"] < convertedStuLastCourseActivty:
            
            ## Update the stuCoursesData's last canvas activity date
            p2_stuCoursesData["Last Canvas Activity"] = convertedStuLastCourseActivty

        ## If the enrollment was originally deleted, but has been reactivated
        if enrollmentDeleted:

            ## Create the deletion api url by adding the enrollment id to the end of the stuCourseEnrollmentApiUrl
            stuCourseEnrollmentDeletionApiUrl = f"{stuCourseEnrollmentApiUrl}/{p2_stuCoursesData[targetCourseSisId]['canvas_enrollment_id']}"

            ## Defeine the parameter to delete the enrollment
            stuCourseEnrollmentDeleteParams = {
                "task": "delete"
            }
            
            ## Make a delete enrollment api call to remove the reactivated enrollment
            enrollmentDeletionApiOjbect = makeApiCall(p1_header = header, p1_apiUrl = stuCourseEnrollmentDeletionApiUrl, p1_payload = stuCourseEnrollmentDeleteParams, apiCallType = "delete")

            ## Define a deletion attempt variable
            enrollmentDeletionAttempt = 1

            ## If the enrollment deletion api call was not successful
            while enrollmentDeletionApiOjbect.status_code != 200 and enrollmentDeletionAttempt != 5:

                ## Sleep 3 seconds
                time.sleep(3)

                ## Log a warning that the enrollment deletion failed
                logger.warning(f"Enrollment deletion failed for {p2_stuCoursesData[targetCourseSisId]['canvas_enrollment_id']} in course {targetCourseSisId} for student {p2_stuID}")

                #try to remove the reactiviated enrollment again
                enrollmentDeletionApiOjbect = makeApiCall(p1_header = header, p1_apiUrl = stuCourseEnrollmentDeletionApiUrl, p1_payload = stuCourseEnrollmentDeleteParams, apiCallType = "delete")

                ## Increment the attempt number
                enrollmentDeletionAttempt += 1

            ## If the status was unsucessful despite 5 attempts
            if enrollmentDeletionApiOjbect.status_code != 200:

                ## Log the attempt as a warning
                logger.warning(f"Enrollment deletion failed for {p2_stuCoursesData[targetCourseSisId]['canvas_enrollment_id']} in course {targetCourseSisId} for student {p2_stuID}. Status Code: {enrollmentDeletionApiOjbect.status_code}")

                ## Call the error handler function to alert the lms admin
                error_handler (functionName, p1_ErrorInfo = f"Enrollment deletion failed for {p2_stuCoursesData[targetCourseSisId]['canvas_enrollment_id']} in course {targetCourseSisId} for student {p2_stuID}. Status Code: {enrollmentDeletionApiOjbect.status_code}")

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

        # if p1_stuID == 25725:
        #     print (1)
        
        ## Make a df of any canvas user ids that match the student id
        stuCanvasIdDf = p1_filteredCombinedCanvasEnrollmentsDF[
            p1_filteredCombinedCanvasEnrollmentsDF["user_id"].astype(int) == int(p1_stuID)
            ]["canvas_user_id"]
        
        ## Attempt to get the student's canvas id
        if not stuCanvasIdDf.empty:
            p1_stuCoursesData["stuCanvasId"] = stuCanvasIdDf.values[0]
        
        ## If no canvas ID is found
        if (
            not p1_stuCoursesData 
            or "stuCanvasId" not in p1_stuCoursesData.keys() 
            or not p1_stuCoursesData["stuCanvasId"]
            ):

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
                continue

            ## Create a variable to hold the parent course sis id of crosslisted courses when neccessary
            parentStuEnrolledCourseId = ""

            ## Otherwise create a dict attached to the stu enrolled course id within the student data dict
            p1_stuCoursesData[targetCourseSisId] = {}
            
    except Exception as error:
        error_handler (functionName, p1_ErrorInfo = error)       

## This function takes a list of current NNU enrollments and gets their Canvas enrollment related activity and grade information
def getNighthawk360Data (p1_oldEnrollmentDataDf):
    functionName = "Get Night Hawk 360 Data"

    try:
        ## Determine the current term based off current date\
        ## Determine the target term based off the target month
        currentTermCodes = []
        currentTerms = []
        currentSchoolYear = None

        ## January through May makes the current terms the Spring Terms
        if currentMonth >= 1 and currentMonth <= 5:

            ## Using the current year and decade, define the current terms and add them as tuples to the current terms list
            currentTermCodes.append((f"SP{decade}", f"GS{decade}"))
            currentTerms.append((f"SP{currentYear}", f"GS{currentYear}"))

        ## December has some spring courses so it is also a spring term
        if currentMonth == 12:

            ## Using the current year and decade, define the current terms and add them as tuples to the current terms list
            currentTermCodes.append((f"SP{str(int(decade) + 1)}", f"GS{str(int(decade) + 1)}"))
            currentTerms.append((f"SP{str(int(currentYear) + 1)}", f"GS{str(int(currentYear) + 1)}"))

        ## May through August makes the current terms the Summer Terms
        if currentMonth >= 5 and currentMonth <= 8:

            ## Using the current year and decade, define the current terms and add them as tuples to the current terms list
            currentTermCodes.append((f"SU{decade}", f"SG{decade}"))
            currentTerms.append((f"SU{currentYear}", f"SG{currentYear}"))

        ## August through December (the rest of the months) makes the current terms the Spring Terms
        if currentMonth >= 8 and currentMonth <= 12:

            ## Using the current year and decade, define the current terms and add them as tuples to the current terms list
            currentTermCodes.append((f"FA{decade}", f"GF{decade}"))
            currentTerms.append((f"FA{currentYear}", f"GF{currentYear}"))

        ## Define a complete student enrollment data dict
        completeStudentEnrollmentDataDict = {}

        ## For each term pair in the current terms
        for termCodePair, termPair in zip(currentTermCodes, currentTerms):

            ## Deteremine the current schoolyear based off of the current term
            if termCodePair[0] == f"FA{decade}":
                ## Fall terms are the first terms of a new school year so FA20 is part of the 2020-21 school year.
                currentSchoolYear = f"{century}{decade}-{int(decade) + 1}"
            else:
                ## Spring and Summer terms belong in the same school year as the fall terms before them, so SP21 is part of the same 2020-21 schooal year as FA20.
                currentSchoolYear = f"{century}{int(decade) - 1}-{decade}"

            ## Save the school year based canvas input path
            schoolYearLocalInputPath = f"{baseLocalInputPath}{currentSchoolYear}\\"
        
            ## Get the current undergraduate Canvas enrollment file
            undgCanvasEnrollmentsDF = pd.read_csv(termGetEnrollments(termCodePair[0]))

            ## Get the current graduate Canvas enrollment file
            gradCanvasEnrollmentsDF = pd.read_csv(termGetEnrollments(termCodePair[1]))

            ## Combine the undergraduate and graduate enrollment files
            combinedCanvasEnrollmentsDF = pd.concat([undgCanvasEnrollmentsDF, gradCanvasEnrollmentsDF], ignore_index=True)
        
            ## Filter so that only rows that have a student role and that are not in the chapel course are retained
            filteredCombinedCanvasEnrollmentsDF = combinedCanvasEnrollmentsDF[(combinedCanvasEnrollmentsDF['role'] == 'student')
                                                                              & (~combinedCanvasEnrollmentsDF['course_id'].str.contains('CHPL1000_01'))
                                                                              & (combinedCanvasEnrollmentsDF['course_id'].str.contains(termPair[0]) 
                                                                                 | combinedCanvasEnrollmentsDF['course_id'].str.contains(termPair[1])
                                                                                 )
                                                                              ]

            ## Get the current undergraduate Canvas unpublished courses file
            undgUnpublishedCanvasCoursesDF = pd.read_csv(termGetUnpublishedCourses(termCodePair[0]))

            ## Get the current graduate Canvas unpublished courses file
            gradUnpublishedCanvasCoursesDF = pd.read_csv(termGetUnpublishedCourses(termCodePair[1]))

            ## Combine the undergraduate and graduate unpublished courses files
            combinedUnpublishedCanvasCoursesDF = pd.concat([undgUnpublishedCanvasCoursesDF, gradUnpublishedCanvasCoursesDF], ignore_index=True)

            ## Make a list of the unpublished courses
            combinedUnpublishedCanvasCoursesList = combinedUnpublishedCanvasCoursesDF["sis id"].tolist()
        
            ## SIS enrollment

            ## Get the current institution enrollment data
            sisEnrollmentsDF = pd.read_csv(f"{baseExternalInputPath}canvas_enroll.csv")
        
            ## Filter the current institution enrollment data to only retain the student enrollments
            partiallyFilteredSisEnrollmentsDF = sisEnrollmentsDF[(sisEnrollmentsDF['role'] == 'student')
                                                        & (~sisEnrollmentsDF['course_id'].str.contains('CHPL1000_01'))
                                                        & (sisEnrollmentsDF['course_id'].str.contains(termPair[0])
                                                           | sisEnrollmentsDF['course_id'].str.contains(termPair[1])
                                                           )
                                                        ]

            ## Filter out duplicate course id + user id pairs from the SIS enrollments dataframe
            filteredSisEnrollmentsDF = partiallyFilteredSisEnrollmentsDF.drop_duplicates(subset=['course_id', 'user_id'])
        
            ## Determine the unique student ids within the SIS enrollment Dataframe
            uniqueStuIdDF = filteredSisEnrollmentsDF['user_id'].unique()

            ## Create a dict of the students' ids and their last Canvas access report data points
            stuLastCanvasAccessData = retrieveListOfStuLastCanvasAccessReportDataPoints(uniqueStuIdDF.tolist())

            ## Define a dictionary with each unique ID as a key and include the last Canvas access data
            studentDataDict = {stuID: {'Last Canvas Activity': stuLastCanvasAccessData.get(stuID, '')} for stuID in uniqueStuIdDF}

            ## Extract the sis enrollment data for deleted enrollments
            deletedSisEnrollments = filteredSisEnrollmentsDF[filteredSisEnrollmentsDF['status'] == 'deleted'].copy()

            ## Replace underscores with dashes in the course_id column of the deleted sis enrollments
            deletedSisEnrollments['course_id'] = deletedSisEnrollments['course_id'].str.replace('_', '-')

            ## Create a unEnrolledStudentData to hold the unenrolled student data
            unEnrolledStudentData = pd.DataFrame()

            ## If the p1_oldEnrollmentDataDf is not empty
            if not p1_oldEnrollmentDataDf.empty:

                ## Change all '-' to '_' in the course number column of the old enrollment data and deletedSisEnrollments
                p1_oldEnrollmentDataDf['Course Number'] = p1_oldEnrollmentDataDf['Course Number'].str.replace('-', '_')
                deletedSisEnrollments['course_id'] = deletedSisEnrollments['course_id'].str.replace('-', '_')

                ## Retain the old enrollment data user ids for only the user id and course ids that are in the deleted sis enrollments
                unEnrolledStudentData = p1_oldEnrollmentDataDf.merge(
                    deletedSisEnrollments[['user_id', 'course_id']],
                    left_on=['Student ID', 'Course Number'],
                    right_on=['user_id', 'course_id'],
                    how='inner'
                ).drop(columns=['user_id', 'course_id'])

                ## Set all nan to ""
                unEnrolledStudentData.fillna("", inplace=True)

                ## For each student id in the unEnrolledStudentData
                for stuID in unEnrolledStudentData['Student ID'].astype(int).unique():

                    # if (stuID == 718522):
                    #     print (1)

                    ## Retreive the data associated with the student's deleted enrollments
                    stuDataDf = unEnrolledStudentData[unEnrolledStudentData['Student ID'] == stuID]

                    ## For each course id in the student's data
                    for courseId in stuDataDf['Course Number'].unique():
                    
                        ## Add the course id as a key to the student's data dict
                        studentDataDict[stuID][courseId] = {}

                        ## Seperate out the course data for the course id
                        courseData = stuDataDf[stuDataDf['Course Number'] == courseId].iloc[0]

                        ## For each column in the course data
                        for column in stuDataDf.columns:

                            ## If the column is the published column
                            if column == "Published":

                                ## Add the key but set the value to "Unenrolled"
                                studentDataDict[stuID][courseId][column] = "Unenrolled"

                            ## Otherwise, if the column is not the published column
                            else:

                                ## Add the column as a key and the value as the value to the student's course data dict
                                studentDataDict[stuID][courseId][column] = courseData[column]

            ## For each unique student id
            for stuID, stuCoursesData in studentDataDict.items():

                ## If the published key is not in the student's course data dict
                if "Published" not in stuCoursesData.keys():

                    ## Retrieve the student's courses data
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

                    #if stuID == 718522:
                
                        ## For each 
                        # If there is not a published key in all of the student's course data dicts
                        if not all(
                            "Published" in stuCourseData
                            for stuCourseData in stuCoursesData.values()
                            if isinstance(stuCourseData, dict)
                        ):
                    
                            ## Define the stuEnrollmentDataThread as None to ensure a brand new thread is created
                            stuEnrollmentDataThread = None

                            ## If there is not a published value for the key
                            if "Published" not in stuCoursesData.keys() or not stuCoursesData["Published"]:

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

                            ## Else
                            else:
                                print ("already handled")

                # Wait until all ongoing threads have completed
                for thread in ongoingReportThreads:
                    thread.join()

                ## Log the number of threads that have been completed
                logger.info(f"{tensCompletedCounter * 10 + threadCounter} threads have been completed")    

            ## If the completeStudentEnrollmentDataDict is empty
            if not completeStudentEnrollmentDataDict:

                ## Set it to the student data dict
                completeStudentEnrollmentDataDict = studentDataDict

            ## Otherwise
            else:

                ## For each student id and their courses in the student data dict
                for stuID, stuCoursesData in studentDataDict.items():

                    ## If the student id is not in the complete student enrollment data dict
                    if stuID not in completeStudentEnrollmentDataDict.keys():

                        ## Add the student id and their courses to the complete student enrollment data dict
                        completeStudentEnrollmentDataDict[stuID] = stuCoursesData

                    ## Otherwise
                    else:

                        ## Update the keys and values of the student id's courses in the complete student enrollment data dict
                        completeStudentEnrollmentDataDict[stuID].update(stuCoursesData)


        
        ## Return the completed student data dict
        return completeStudentEnrollmentDataDict
                    
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

        ## Define a variable to contain the oldEnrollmentDataDf
        oldEnrollmentDataDf = pd.DataFrame()

        ## If previous versions of the enrollment data exist
        if os.path.exists(f"{baseExternalOutputPath}Enrollment_Data_Activity.csv") and os.path.exists(f"{baseExternalOutputPath}Enrollment_Data_Submissions.csv"):

            ## Open the current enrollment data activity and submission pipe delineated csv files as dataframes
            oldEnrollmentDataActivityDf = pd.read_csv(f"{baseExternalOutputPath}Enrollment_Data_Activity.csv", sep="|")
            oldEnrollmentDataSubmissionsDf = pd.read_csv(f"{baseExternalOutputPath}Enrollment_Data_Submissions.csv", sep="|")
        
            ## Combine old data into a single DataFrame for comparison
            oldEnrollmentDataDf = pd.merge(oldEnrollmentDataActivityDf, oldEnrollmentDataSubmissionsDf, on=["Student ID", "Course Number"], how="outer")

        ## Get the current institution enrollment data
        enrollmentDataDict = getNighthawk360Data(oldEnrollmentDataDf)

        ## Open the local enrollment data activity and submission pipe delineated csv files as dataframes
        #currentEnrollmentDataActivityDf = pd.read_csv(f"{baseLocalOutputPath}Enrollment_Data_Activity.csv", sep="|")
        #currentEnrollmentDataSubmissionsDf = pd.read_csv(f"{baseLocalOutputPath}Enrollment_Data_Submissions.csv", sep="|")

        ## Combine the two using the Student ID and Course Number as the join keys
        #currentEnrollmentDataDf = pd.merge(currentEnrollmentDataActivityDf, currentEnrollmentDataSubmissionsDf, on=["Student ID", "Course Number"], how="outer")
        
        # ## Recreate the enrollmentDataDict from the merged DataFrame
        # enrollmentDataDict = {}

        # for _, row in currentEnrollmentDataDf.iterrows():
        #     student_id = row['Student ID']
        #     course_number = row['Course Number']

        #     ## Initialize the student entry if not already present
        #     if student_id not in enrollmentDataDict:
                
        #         enrollmentDataDict[student_id] = {}

        #     ## Create the course-specific data dictionary
        #     course_data = {
        #         "Last Canvas Activity": row.get("Last Canvas Activity", ""),
        #         "Published": row.get("Published", ""),
        #         "Last Course Activity Date": row.get("Last Course Activity", ""),
        #         "Last Course Participation": row.get("Last Course Participation", ""),
        #         "Current Grade": row.get("Current Grade", ""),
        #         "Number of Missed Assignments": row.get("Number of Missed Assignments", ""),
        #         "Number of Assignments graded 0": row.get("Number of Assignments graded 0", "")
        #         }

        #     ## Assign the course data to the appropriate course number
        #     enrollmentDataDict[student_id][course_number] = course_data

        # ## If previous versions of the enrollment data exist
        # if os.path.exists(f"{baseExternalOutputPath}Enrollment_Data_Activity.csv") and os.path.exists(f"{baseExternalOutputPath}Enrollment_Data_Submissions.csv"):

        #     ## Open the current enrollment data activity and submission pipe delineated csv files as dataframes
        #     oldEnrollmentDataActivityDf = pd.read_csv(f"{baseExternalOutputPath}Enrollment_Data_Activity.csv", sep="|")
        #     oldEnrollmentDataSubmissionsDf = pd.read_csv(f"{baseExternalOutputPath}Enrollment_Data_Submissions.csv", sep="|")
        
        #     ## Combine old data into a single DataFrame for comparison
        #     oldEnrollmentDataDf = pd.merge(oldEnrollmentDataActivityDf, oldEnrollmentDataSubmissionsDf, on=["Student ID", "Course Number"], how="outer")

        #     ## Rename the columns to match the current data
        #     # oldEnrollmentDataDf.rename(columns={
        #     #     'Student ID': 'student_id'
        #     #     , 'Course Number': 'course_id'
        #     #     , 'Published': 'published'
        #     # }, inplace=True)

        #     # Find the differences between the old and current data
        #     # rawMissedEnrollmentsDf = oldEnrollmentDataDf[
        #     #     ~oldEnrollmentDataDf.apply(
        #     #         lambda row: any(
        #     #             (currentEnrollmentDataDf['student_id'] == row['student_id']) 
        #     #             & (currentEnrollmentDataDf['course_id'] == row['course_id'])
        #     #             )
        #     #         , axis=1
        #     #         )
        #     #     ]

        #     # Find the differences between the old and current data
        #     rawMissedEnrollmentsDf = oldEnrollmentDataDf[
        #         ~oldEnrollmentDataDf.apply(
        #             lambda row: (
        #                 row['Student ID'] in enrollmentDataDict and
        #                 row['Course Number'] in enrollmentDataDict[row['Student ID']]
        #             ),
        #             axis=1
        #         )
        #     ]

        
        #     ## Open the SIS course feed file from the baseExternalInputPath
        #     sisCourseFeedDF = pd.read_csv(f"{baseExternalInputPath}canvas_course.csv")

        #     ## Replace all _ with - in the sisCourseFeedDF
        #     sisCourseFeedDF['course_id'] = sisCourseFeedDF['course_id'].str.replace('-', '_')

        #     ## Filter out any missed enrollments that are not in the SIS course feed
        #     filteredMissedEnrollmentsDf = rawMissedEnrollmentsDf[
        #         rawMissedEnrollmentsDf['Course Number'].isin(sisCourseFeedDF['course_id'])
        #         ]

        #     ## Replace any instances of nan with ""
        #     filteredMissedEnrollmentsDf.fillna("", inplace=True)
        
        #     ## Add the missed enrollments to the relevant student IDs' data points
        #     for index, row in filteredMissedEnrollmentsDf.iterrows():
            
        #         ## Define the student ID and course ID
        #         studentId = row['Student ID']
        #         courseId = row['Course Number']
        #         lastCanvasActivity = enrollmentDataDict[studentId]["Last Canvas Activity"] if studentId in enrollmentDataDict else retrieveStuLastCanvasAccessReportDataPoint(studentId)
    
        #         ## Check if the student_id exists in the current data dict
        #         if studentId in enrollmentDataDict:
                
        #             ## Set the published status to "Unenrolled"
        #             row['Published'] = "Student Un-enrolled"
                
        #             ## For each key and value in the row
        #             for courseDataPoint, courseDataPointValue in row.items():

        #                 ## If the courseid is a key within the student's course data dict
        #                 if courseId in enrollmentDataDict[studentId]:

        #                     ## Add the course data point to the student's course data dict
        #                     enrollmentDataDict[studentId][courseId][courseDataPoint] = courseDataPointValue

        #                 ## Otherwise
        #                 elif courseDataPoint not in ["Student ID", "Course Number"]:

        #                     ## Create a new course data dict item
        #                     enrollmentDataDict[studentId][courseId] = {courseDataPoint: courseDataPointValue}

        #                     ## If the course data point is the last canvas activity date and the row's last Canvas activity
        #                     ## date is more recent than the student's last canvas activity date
        #                     if (
        #                         courseDataPoint == "Last Canvas Activity"
        #                         and lastCanvasActivity < courseDataPointValue
        #                     ):

        #                         ## Set the last canvas activity date to the student's last canvas activity date
        #                         enrollmentDataDict[studentId][courseId][courseDataPoint] = row[courseDataPoint]

        #                         ## Also set the last canvas activity date to the student's last canvas activity date
        #                         enrollmentDataDict[studentId]["Last Canvas Activity"] = row[courseDataPoint]
        #                         lastCanvasActivity = row[courseDataPoint]
                        
        #         ## Otherwise  
        #         else:
                
        #             ## Create a new student data dict item
        #             enrollmentDataDict[studentId] = {courseId: row.to_dict()}

        #             ## Set the published status to "Unenrolled"
        #             enrollmentDataDict[studentId][courseId]['Published'] = "Student Un-enrolled"

        #             ## Record the student's last canvas activity date and set it to both the course and student data dicts
        #             enrollmentDataDict[studentId][courseId]["Last Canvas Activity"] = lastCanvasActivity
        #             enrollmentDataDict[studentId]["Last Canvas Activity"] = lastCanvasActivity

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
            submissionCsv.write("|\"Number of Assignments graded 0\"\n")        
            
            ## Iterate through the completed enrollment data and save it in dataframe compatible format
            for stuID, dataPoints in enrollmentDataDict.items():

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
                        formattedNumberOfMissedAssignments = "|\"\""
                        formattedNumberOf0Grades            = "|\"\"\n"

                        ## Attempt to format the data points for the csv
                        try: ## Irregular try clause, do not comment out in testing
                        
                            formattedStuID                      = "\""  + str(stuID)                                                        + "\""
                            formattedCourseCode                 = "|\"" + dataPoint.replace('_', '-')                                       + "\""

                            formattedLastCanvasActivity         = "|\"" + str(dataPoints             ["Last Canvas Activity"])              + "\""
                            formattedPublished                  = "|\"" + dataPoints [dataPoint]     ["Published"]                          + "\""
                            formattedLastCourseActivity         = "|\"" + str(dataPoints [dataPoint] ["Last Course Activity"])              + "\""
                            formattedLastCourseParticipation    = "|\"" + str(dataPoints [dataPoint] ["Last Course Participation"])         + "\"\n"

                            formattedCurrentGrade               = "|\"" + str(dataPoints [dataPoint] ["Current Grade"])                     + "\""
                            formattedNumberOfMissedAssignments  = "|\"" + str(dataPoints [dataPoint] ["Number of Missed Assignments"])      + "\""
                            formattedNumberOf0Grades            = "|\"" + str(dataPoints [dataPoint] ["Number of Assignments graded 0"])    + "\"\n"

                        except Exception as error: ## Irregular except clause, do not comment out in testing

                             ## Log a warning that an error occured while processing the data point
                             logger.warning(f"Error: {error} \n Occured while processing {dataPoint}:{dataPoints[dataPoint]} for Student ID: {str(stuID)}")

                             error_handler ("functionName", p1_ErrorInfo = f"{error} \n Occured while processing {dataPoint}:{dataPoints[dataPoint]}")

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
                        submissionCsv.write (formattedNumberOfMissedAssignments)
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