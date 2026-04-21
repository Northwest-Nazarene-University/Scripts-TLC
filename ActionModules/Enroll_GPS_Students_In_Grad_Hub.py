## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

## Import necessary modules
import os, sys
from datetime import datetime

## Add Script repository to syspath
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

## Resource module imports
try:
    from Local_Setup import LocalSetup
    from TLC_Common import makeApiCall, isFileRecent
    from Canvas_Report import CanvasReport
    from Error_Email import errorEmail
    from TLC_Action import deleteEnrollment, enrollUser
except ImportError:
    from ResourceModules.Local_Setup import LocalSetup
    from ResourceModules.TLC_Common import makeApiCall, isFileRecent
    from ResourceModules.Canvas_Report import CanvasReport
    from ResourceModules.Error_Email import errorEmail
    from ResourceModules.TLC_Action import deleteEnrollment, enrollUser

## Module-level setup (required because this module is imported and called by the orchestrator)
localSetup = LocalSetup(datetime.now(), __file__)  ## sets cwd, paths, logs, date parts

## Import configs
from Common_Configs import coreCanvasApiUrl, canvasAccessToken, gradTermsWordsToCodesDict

## Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = os.path.basename(__file__).replace(".py", "")

scriptPurpose = r"""
Auto-enrolls all current GPS (Graduate & Professional Studies) students in the
Graduate & Professional Student Hub course and removes any students who are no
longer GPS students for the given term.
"""
externalRequirements = r"""
To function properly, this script requires a valid Canvas API URL and the Canvas courses
and enrollment reports to be populated via CanvasReport.
"""

## Setup the error handler
errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)


## Sync GPS student enrollment in the Grad Hub course for the given term
def enrollGPSStudentsInGrad_Hub(inputTerm):
    functionName = "enrollGPSStudentsInGrad_Hub"
    try:
        ## Step 1: Determine the grad term code from the given input term
        termPrefix = inputTerm[:2]
        termYear   = int(str(localSetup.dateDict["century"]) + inputTerm[2:4])
        termName   = localSetup._determineTermName(termPrefix)
        gradTermPrefix = gradTermsWordsToCodesDict[termName]
        gradTerm = gradTermPrefix + str(termYear)[2:4]

        ## Step 2: Retrieve GPS students and the Grad Hub course ID
        gpStudentsDf = CanvasReport.getGpsStudentsDf(localSetup, gradTerm)
        gpStudentsDf = gpStudentsDf.dropna(subset=["user_id"])
        gpStudentsDf["user_id"] = gpStudentsDf["user_id"].astype(int)

        coursesDf = CanvasReport.getCoursesDf(localSetup, "Default Term")
        targetCourseId = coursesDf.loc[
            coursesDf["short_name"] == "Graduate & Professional Student Hub",
            "canvas_course_id",
        ].values[0]

        gradHubCoreApiUrl  = f"{coreCanvasApiUrl}courses/{targetCourseId}"
        gradHubUsersApiUrl = f"{gradHubCoreApiUrl}/users"
        gradHubUsersPayload = {
            "enrollment_type[]": ["student"],
            "include[]": "enrollments",
            "per_page": 100,
        }

        ## Step 3: Build a dict of students currently enrolled in the Grad Hub course
        ## {sis_user_id: enrollment_id}
        gradHubEnrollmentResponse, _ = makeApiCall(
            localSetup, p1_apiUrl=gradHubUsersApiUrl, p1_payload=gradHubUsersPayload
        )
        currentlyEnrolled = {}
        for studentObj in gradHubEnrollmentResponse.json():
            currentlyEnrolled[studentObj["sis_user_id"]] = studentObj["enrollments"][0]["id"]

        ## Step 4: Remove students who are enrolled but are no longer in the GPS list
        for studentId, enrollmentId in currentlyEnrolled.items():
            if studentId.isdigit() and int(studentId) not in gpStudentsDf["user_id"].values:
                deleteEnrollment(localSetup, errorHandler, str(targetCourseId), str(enrollmentId))

        ## Step 5: Enroll students who are in the GPS list but not yet enrolled in Grad Hub
        gradHubEnrollmentsApiUrl = f"{gradHubCoreApiUrl}/enrollments"
        for _, studentRow in gpStudentsDf.iterrows():
            if str(studentRow["user_id"]) not in currentlyEnrolled:
                enrollUser(
                    localSetup, errorHandler,
                    str(targetCourseId), str(studentRow["canvas_user_id"]),
                    "StudentEnrollment",
                )

    except Exception as Error:
        errorHandler.sendError(functionName, Error)


if __name__ == "__main__":
    ## Set working directory
    os.chdir(os.path.dirname(__file__))

    ## Prompt for the target term and run
    enrollGPSStudentsInGrad_Hub(
        inputTerm=input("Enter the desired term in four character format (GF20, GS20): ")
    )

    input("Press enter to exit")
