## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

## Import Generic Modules
import os, sys, pandas as pd
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

## Add the resource modules path
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

## Try direct imports if run as main, else relative for package usage
try:
    from Local_Setup import LocalSetup
    from TLC_Common import makeApiCall
    from Error_Email import errorEmail
    from Common_Configs import coreCanvasApiUrl, canvasAccessToken
except ImportError:  ## When imported as a package/module
    from .Local_Setup import LocalSetup
    from .TLC_Common import makeApiCall
    from .Error_Email import errorEmail
    from .Common_Configs import coreCanvasApiUrl, canvasAccessToken

## Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = os.path.basename(__file__).replace(".py", "")
scriptPurpose = r"""
This script reads a CSV file containing Canvas enrollment IDs and changes the role for each enrollment using the Canvas API.
"""
externalRequirements = r"""
To function properly, this script requires:
- Valid Canvas API configuration in Common_Configs (coreCanvasApiUrl, canvasAccessToken).
- A CSV file named "Target_Canvas_Enrollment_Ids.csv" located in the Canvas internal
  resources directory (LocalSetup.getInternalResourcePaths("Canvas")) with columns:
    - canvas_enrollment_id
    - canvas_user_id
    - canvas_course_id
    - role_id
    - base_role_type
"""

## This function deletes an enrollment given its Canvas enrollment ID
def deleteEnrollment(
    localSetup: LocalSetup,
    errorHandler: errorEmail,
    courseId: str,
    enrollmentId: str,
) -> None:
    functionName = "deleteEnrollment"
    try:
        deleteEnrollmentUrl = f"{coreCanvasApiUrl}courses/{courseId}/enrollments/{enrollmentId}"
        response, _ = makeApiCall(
            localSetup=localSetup,
            p1_apiUrl=deleteEnrollmentUrl,
            p1_apiCallType="delete",
        )

        if response.status_code == 200:
            localSetup.logInfoThreadSafe(f"Successfully deleted enrollment with ID: {enrollmentId}")
        else:
            localSetup.logWarningThreadSafe(f"Failed to delete enrollment with ID: {enrollmentId}. Status code: {response.status_code}")

    except Exception as Error:
        errorHandler.sendError(functionName, Error)

## This function re-enrolls a user with a new role given the Canvas user ID, course ID, role ID, and base role type
def reEnrollUser(
    localSetup: LocalSetup,
    errorHandler: errorEmail,
    userId: str,
    courseId: str,
    roleId: str,
    baseRoleType: str,
) -> None:
    functionName = "reEnrollUser"
    try:
        reEnrollUrl = f"{coreCanvasApiUrl}courses/{courseId}/enrollments"
        payload = {
            "enrollment[user_id]": userId,
            "enrollment[type]": baseRoleType,
            "enrollment[role_id]": roleId,
            "enrollment[enrollment_state]": "active",
        }
        response, _ = makeApiCall(
            localSetup=localSetup,
            p1_apiUrl=reEnrollUrl,
            p1_payload=payload,
            p1_apiCallType="post",
        )

        if response.status_code == 200:
            localSetup.logInfoThreadSafe(f"Successfully re-enrolled user {userId} in course {courseId} with role {roleId}")
        else:
            localSetup.logWarningThreadSafe(f"Failed to re-enroll user {userId} in course {courseId}. Status code: {response.status_code}")

    except Exception as Error:
        errorHandler.sendError(functionName, Error)

## This function re-enrolls the user with the new role then deletes the old enrollment
def deleteAndReenroll(
    localSetup: LocalSetup,
    errorHandler: errorEmail,
    enrollmentId: str,
    userId: str,
    courseId: str,
    roleId: str,
    baseRoleType: str,
) -> None:
    reEnrollUser(localSetup, errorHandler, userId, courseId, roleId, baseRoleType)
    deleteEnrollment(localSetup, errorHandler, courseId, enrollmentId)

## This function reads the CSV file, deletes the enrollment, and re-enrolls the user with the new role
def changeListedEnrollmentsRole(localSetup: LocalSetup, errorHandler: errorEmail) -> None:
    functionName = "changeListedEnrollmentsRole"
    try:
        canvasResourcePath = localSetup.getInternalResourcePaths("Canvas")
        targetEnrollmentsCsvFilePath = os.path.join(canvasResourcePath, "Target_Canvas_Enrollment_Ids.csv")

        ## Read the CSV file using pandas
        rawTargetEnrollmentsDf = pd.read_csv(targetEnrollmentsCsvFilePath)

        ## Retain only rows that have a value in canvas_enrollment_id
        targetEnrollmentsDf = rawTargetEnrollmentsDf[rawTargetEnrollmentsDf["canvas_enrollment_id"].notna()]

        ## Process each enrollment in a thread pool
        MAX_WORKERS = 25
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
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

                ## Submit the task to the thread pool
                executor.submit(deleteAndReenroll, localSetup, errorHandler, enrollmentId, userId, courseId, roleId, baseRoleType)

        localSetup.logInfoThreadSafe(f"{functionName} completed. Processed {len(targetEnrollmentsDf)} enrollments.")

    except Exception as Error:
        errorHandler.sendError(functionName, Error)

if __name__ == "__main__":
    ## Initialize shared LocalSetup (paths, logging)
    localSetup = LocalSetup(datetime.now(), __file__)

    ## Setup the error handler (sends one email per function error)
    errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)

    changeListedEnrollmentsRole(localSetup, errorHandler)

    input("Press enter to exit")
