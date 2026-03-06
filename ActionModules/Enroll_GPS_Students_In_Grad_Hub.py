## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

## Import necessary modules
import os, sys, pandas as pd, threading, time
from datetime import datetime, date

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
To function properly, this script requires a valid access header and URL, and a CSV file named "Target_Canvas_Enrollment_Ids.csv" located in the Canvas Resources directory.
"""

## Setup the error handler
errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)

## This function deletes an enrollment given its Canvas enrollment ID
def deleteEnrollment(p3_courseId, p1_enrollmentId):
    functionName = "deleteEnrollment"
    try:

        ## Define the API URL for deleting the enrollment
        ##deleteEnrollmentUrl = f"{coreCanvasApiUrl}courses/{p3_courseId}/enrollments/{p1_enrollmentId}"
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
def enrollGPSStudentsInGrad_Hub(inputTerm):

    functionName = "enrollGPSStudentsInGrad_Hub"

    try:

        ## Get the term prefix
        termPrefix = inputTerm[:2]
        termYear = int(str(localSetup.dateDict['century']) + inputTerm[2:4])
        
        ## Define the grad term
        termName = localSetup._determineTermName(termPrefix)

        ## Get the grad term
        gradTermPrefix = gradTermsWordsToCodesDict[termName]
        gradTerm = gradTermPrefix + str(termYear)[2:4]

        ## Read the input term's GPS student csv into a df
        GPSStudentsDf = CanvasReport.getGpsStudentsDf(localSetup, gradTerm)

        ## Drop student rows without user_ids
        GPSStudentsDf = GPSStudentsDf.dropna(subset=['user_id'])

        ## Set studentRow['user_id'] to int
        GPSStudentsDf['user_id'] = GPSStudentsDf['user_id'].astype(int)

        ## Retrieve (and update if neccessary) the term relavent canvas courses file path
        ##Grad_HubCourseTermLocationDf = pd.read_csv(termGetCourses("All"))
        Grad_HubCourseTermLocationDf = CanvasReport.getCoursesDf(localSetup, "Default Term")

        ## Find the "canvas_course_id" for the Graduate & Professional Student Hub course by looking for the target Graduate & Professional Student Hub sis id in the course short name
        targetOrientationCanvasCourseId = Grad_HubCourseTermLocationDf.loc[Grad_HubCourseTermLocationDf['short_name'] == "Graduate & Professional Student Hub", 'canvas_course_id'].values[0]
         
        ## Define the Graduate & Professional Student Hub course's base api url
        Grad_HubCourseCoreApiUrl = f"{coreCanvasApiUrl}courses/{targetOrientationCanvasCourseId}"

        ## Define the Graduate & Professional Student Hub courses users api url
        Grad_HubCourseUsersApiUrl = f"{Grad_HubCourseCoreApiUrl}/users"

        ## Define the payload to get the course's students
        Grad_HubCourseUserPayload = {"enrollment_type[]":["student"], "include[]": "enrollments", "per_page": 100}

        ## Make the API call to get the course's details
        Grad_HubCourseEnrollmentObjectOrObjectList, _ = makeApiCall(localSetup, p1_apiUrl = Grad_HubCourseUsersApiUrl, p1_payload = Grad_HubCourseUserPayload)

        ## Make a list to hold the target orientation students
        targetCourseEnrolledStudentsDict = {}
        
        ## Make List to hold all threads
        actionThreads = []

        ## If the Grad_HubCourseEnrollmentObjectOrObjectList is a list
        if isinstance(Grad_HubCourseEnrollmentObjectOrObjectList, list):

            ## For each json api object in the course's enrollment objects list
            for enrollmentsObject in Grad_HubCourseEnrollmentObjectOrObjectList:
                
                ## For each student within the text (dict) of the object
                for studentObject in enrollmentsObject.json():

                    ## Add the student's sis_user_id and the target student's Graduate & Professional Student Hub enrollment id to the targetCourseEnrolledStudentsDict
                    targetCourseEnrolledStudentsDict[studentObject["sis_user_id"]] = studentObject['enrollments'][0]["id"]
        
        ## If the Grad_HubCourseEnrollmentObjectOrObjectList is not a list, There was just one object returned
        else:
            
             ## For each student within the text (dict) of the object
                for studentObject in Grad_HubCourseEnrollmentObjectOrObjectList.json():

                    ## Define a variable to hold the student's enrollment id
                    targetStudentsGrad_HubEnrollmentId = None

                    ## For each enrollment in the student object's enrollments list
                    for enrollment in studentObject["enrollments"]:

                        ## If the course id of the enrollment matches the target orientation course id
                        if enrollment["course_id"] == targetOrientationCanvasCourseId:

                            ## Set the target student's Graduate & Professional Student Hub enrollment id to the enrollment's id
                            targetStudentsGrad_HubEnrollmentId = enrollment["id"]

                    ## Add the student's sis_user_id and the target student's Graduate & Professional Student Hub enrollment id to the targetCourseEnrolledStudentsDict
                    targetCourseEnrolledStudentsDict[studentObject["sis_user_id"]] = targetStudentsGrad_HubEnrollmentId

        ## For each student in the targetCourseEnrolledStudentsDict
        for studentId, enrollmentID in targetCourseEnrolledStudentsDict.items():

            ## If the student is not in the GPSStudentsDf
            if studentId.isdigit() and int(studentId) not in GPSStudentsDf['user_id'].values:
                
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
                    localSetup.logger.warning(f"Enrollment deletion failed in the Graduate & Professional Student Hub course for student {studentId}")

                    ## try to remove the reactiviated enrollment again
                    enrollmentDeletionApiOjbect, _ = makeApiCall(localSetup, p1_apiUrl = stuCourseEnrollmentDeletionApiUrl, p1_payload = stuCourseEnrollmentDeleteParams, p1_apiCallType = "delete")

                    ## Increment the attempt number
                    enrollmentDeletionAttempt += 1

        ## Define the Graduate & Professional Student Hub courses's enrollment API URL
        Grad_HubCourseUsersApiUrl = f"{Grad_HubCourseCoreApiUrl}/enrollments"

        ## For each student in the GPSStudentsDf
        for index, studentRow in GPSStudentsDf.iterrows():

            ## Define the payload to enroll the student in the Graduate & Professional Student Hub course
            reEnrollPayload = {
                "enrollment[user_id]": studentRow['canvas_user_id'],
                "enrollment[type]": "StudentEnrollment",
                "enrollment[enrollment_state]": "active"
            }

            ## If the student is not already enrolled in the Graduate & Professional Student Hub course
            if str(studentRow['user_id']) not in targetCourseEnrolledStudentsDict.keys():

                ## Make a post api call to enroll the student in the Graduate & Professional Student Hub course
                reEnrollApiObject, _ = makeApiCall(localSetup, p1_apiUrl=Grad_HubCourseUsersApiUrl, p1_payload=reEnrollPayload, p1_apiCallType="post")

                ## If the enrollment was successful
                if reEnrollApiObject.status_code == 200:
                    localSetup.logger.info(f"Successfully enrolled student {studentRow['user_id']} in the Graduate & Professional Student Hub course")
                else:
                    localSetup.logger.warning(f"Failed to enroll student {studentRow['user_id']} in the Graduate & Professional Student Hub course. Status code: {reEnrollApiObject.status_code}")

        

    except Exception as Error:
        errorHandler.sendError(functionName, Error)

if __name__ == "__main__":
    ## Set working directory
    os.chdir(os.path.dirname(__file__))
    
    ## Change the role for the listed enrollments
    enrollGPSStudentsInGrad_Hub(inputTerm = input("Enter the desired term in \
four character format (FA20, SU20, SP20): "))

    ## Wait for user input to exit
    input("Press enter to exit")
