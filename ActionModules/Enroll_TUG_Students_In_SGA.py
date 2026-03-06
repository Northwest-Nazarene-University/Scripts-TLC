## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

## Import necessary modules
import os, sys, threading, time, pandas as pd
from datetime import datetime

## Import necessary functions from local modules
## Add Script repository to syspath
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

## New resource modules
try:
    from Local_Setup import LocalSetup
    from TLC_Common import makeApiCall, isFileRecent
    from Canvas_Report import CanvasReport
    from Error_Email import errorEmail
except ImportError:
    from ResourceModules.Local_Setup import LocalSetup
    from ResourceModules.TLC_Common import makeApiCall, isFileRecent
    from ResourceModules.Canvas_Report import CanvasReport
    from ResourceModules.Error_Email import errorEmail

## Create the localsetup variable
localSetup = LocalSetup(datetime.now(), __file__)  ## sets cwd, paths, logs, date parts

## Import configs
from Common_Configs import coreCanvasApiUrl, canvasAccessToken, gradTermsWordsToCodesDict

## Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = os.path.basename(__file__).replace(".py", "")

scriptPurpose = r"""
This script reads a CSV file containing Canvas enrollment IDs and changes the role for each enrollment using the Canvas API.
"""
externalRequirements = r"""
To function properly, this script requires a valid URL, and a CSV file named "Target_Canvas_Enrollment_Ids.csv" located in the Canvas Resources directory.
"""

## Setup the error handler
errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)

## This function deletes an enrollment given its Canvas enrollment ID
def deleteEnrollment(p3_courseId, p1_enrollmentId):
    functionName = "deleteEnrollment"
    try:

        ## Define the API URL for deleting the enrollment
        deleteEnrollmentUrl = f"{coreCanvasApiUrl}courses/{p3_courseId}/enrollments/{p1_enrollmentId}"

        ## Define the API URL for deleting the enrollment
        response, _ = makeApiCall(localSetup, p1_apiUrl=deleteEnrollmentUrl, p1_apiCallType="delete")

        ## Make the API call to delete the enrollment
        if response.status_code == 200:
            localSetup.logger.info(f"Successfully deleted enrollment with ID: {p1_enrollmentId}")
        else:
            localSetup.logger.warning(f"Failed to delete enrollment with ID: {p1_enrollmentId}. Status code: {response.status_code}")

    except Exception as Error:
        errorHandler.sendError(functionName, Error)

## This function enrolls a user with a new role given the Canvas user ID, course ID, role ID, and base role type
def reEnrollUser(p1_userId, p2_courseId, p3_roleId, p4_baseRoleType):
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
        response, _ = makeApiCall(localSetup, p1_apiUrl=reEnrollUrl, p1_payload=payload, p1_apiCallType="post")

        ## Make the API call to enroll the user
        if response.status_code == 200:
            localSetup.logger.info(f"Successfully enrolled user with ID: {p1_userId} in course with ID: {p2_courseId} with role ID: {p3_roleId}")
        else:
            localSetup.logger.warning(f"Failed to enroll user with ID: {p1_userId} in course with ID: {p2_courseId}. Status code: {response.status_code}")

    except Exception as Error:
        errorHandler.sendError(functionName, Error)

## This function deletes the enrollment and enrolls the user with the new role
def deleteAndReenroll(p1_enrollmentId, p1_userId, p2_courseId, p3_roleId, p4_baseRoleType):
    reEnrollUser(p1_userId, p2_courseId, p3_roleId, p4_baseRoleType)
    deleteEnrollment(p2_courseId, p1_enrollmentId)

## This function reads the CSV file, deletes the enrollment, and enrolls the user with the new role
def enrollTugStudentsInSga(inputTerm):

    functionName = "enrollTugStudentsInSga"

    try:

        ## Determine and save the term's school year
        termName = localSetup._determineTermName(inputTerm[:2])
        startYear, endYear = localSetup._getSchoolYearRange(termName, int(str(localSetup.dateDict["century"]) + inputTerm[2:4]))
        termYear = localSetup._getYearForTerm(termName, startYear, endYear)

        
        ## Get TUG students from Canvas
        tugStudentsDf = CanvasReport.getTugStudentsDf(localSetup, inputTerm)

        ## Get SGA course info
        coursesDf = CanvasReport.getCoursesDf(localSetup, "Default Term")
        targetOrientationCanvasCourseId = coursesDf.loc[coursesDf['short_name'] == "SGA", 'canvas_course_id'].values[0]
         
        ## Define the SGA course's base api url
        SGACourseCoreApiUrl = f"{coreCanvasApiUrl}courses/{targetOrientationCanvasCourseId}"

        ## Define the SGA courses users api url
        SGACourseUsersApiUrl = f"{SGACourseCoreApiUrl}/users"

        ## Define the payload to get the course's students
        SGACourseUserPayload = {"enrollment_type[]":["student"], "include[]": "enrollments", "per_page": 100}

        ## Make the API call to get the course's details
        SGACourseEnrollmentObjectOrObjectList, _ = makeApiCall(localSetup, p1_apiUrl = SGACourseUsersApiUrl, p1_payload = SGACourseUserPayload)

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
            if studentId.isdigit() and str(studentId) not in tugStudentsDf['user_id'].astype(str).values:
                
                ## Create the deletion api url by adding the enrollment id to the end of the stuCourseEnrollmentApiUrl
                stuCourseEnrollmentDeletionApiUrl = f"{coreCanvasApiUrl}courses/{targetOrientationCanvasCourseId}/enrollments/{enrollmentID}"

                ## Defeine the parameter to delete the enrollment
                stuCourseEnrollmentDeleteParams = {
                    "task": "delete"
                }

                ## Make a delete enrollment api call to remove the reactivated enrollment
                enrollmentDeletionApiOjbect, _ = makeApiCall(localSetup, p1_apiUrl = stuCourseEnrollmentDeletionApiUrl, p1_payload = stuCourseEnrollmentDeleteParams, p1_apiCallType = "delete")

                ## Define a deletion attempt variable
                enrollmentDeletionAttempt = 1

                ## If the enrollment deletion api call was not successful
                while enrollmentDeletionApiOjbect.status_code != 200 and enrollmentDeletionAttempt != 5:

                    ## Sleep 3 seconds
                    time.sleep(3)

                    ## Log a warning that the enrollment deletion failed
                    localSetup.logger.warning(f"Enrollment deletion failed in the SGA course for student {studentId}")

                    ## try to remove the reactiviated enrollment again
                    enrollmentDeletionApiOjbect, _ = makeApiCall(localSetup, p1_apiUrl = stuCourseEnrollmentDeletionApiUrl, p1_payload = stuCourseEnrollmentDeleteParams, p1_apiCallType = "delete")

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
                reEnrollApiObject, _ = makeApiCall(localSetup, p1_apiUrl=SGACourseUsersApiUrl, p1_payload=reEnrollPayload, p1_apiCallType="post")

                ## If the enrollment was successful
                if reEnrollApiObject.status_code == 200:
                    localSetup.logger.info(f"Successfully enrolled student {studentRow['user_id']} in the SGA course")
                else:
                    localSetup.logger.warning(f"Failed to enroll student {studentRow['user_id']} in the SGA course. Status code: {reEnrollApiObject.status_code}")

        

    except Exception as Error:
        errorHandler.sendError(functionName, Error)

if __name__ == "__main__":
    ## Set working directory
    os.chdir(os.path.dirname(__file__))
    
    ## Change the role for the listed enrollments
    enrollTugStudentsInSga(inputTerm = input("Enter the desired term in \
four character format (FA20, SU20, SP20): "))

    ## Wait for user input to exit
    input("Press enter to exit")
