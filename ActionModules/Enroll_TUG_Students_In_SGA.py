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
Auto-enrolls all current TUG (Traditional Undergraduate) students in the SGA course
and removes any students who are no longer TUG students for the given term.
"""
externalRequirements = r"""
To function properly, this script requires a valid Canvas API URL and the Canvas courses
and enrollment reports to be populated via CanvasReport.
"""

## Setup the error handler
errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)


## Sync TUG student enrollment in the SGA course for the given term
def enrollTugStudentsInSga(inputTerm):
    functionName = "enrollTugStudentsInSga"
    try:
        ## Step 1: Determine school-year variables for the term
        termName = localSetup._determineTermName(inputTerm[:2])
        startYear, endYear = localSetup._getSchoolYearRange(
            termName, int(str(localSetup.dateDict["century"]) + inputTerm[2:4])
        )

        ## Step 2: Retrieve the current TUG students and the SGA course ID
        tugStudentsDf = CanvasReport.getTugStudentsDf(localSetup, inputTerm)
        coursesDf = CanvasReport.getCoursesDf(localSetup, "Default Term")
        targetCourseId = coursesDf.loc[
            coursesDf["short_name"] == "SGA", "canvas_course_id"
        ].values[0]

        sgaCoreApiUrl   = f"{coreCanvasApiUrl}courses/{targetCourseId}"
        sgaUsersApiUrl  = f"{sgaCoreApiUrl}/users"
        sgaUsersPayload = {
            "enrollment_type[]": ["student"],
            "include[]": "enrollments",
            "per_page": 100,
        }

        ## Step 3: Build a dict of students currently enrolled in the SGA course
        ## {sis_user_id: enrollment_id}
        sgaEnrollmentResponse, _ = makeApiCall(
            localSetup, p1_apiUrl=sgaUsersApiUrl, p1_payload=sgaUsersPayload
        )
        currentlyEnrolled = {}
        for studentObj in sgaEnrollmentResponse.json():
            currentlyEnrolled[studentObj["sis_user_id"]] = studentObj["enrollments"][0]["id"]

        ## Step 4: Remove students who are enrolled but are no longer in the TUG list
        for studentId, enrollmentId in currentlyEnrolled.items():
            if studentId.isdigit() and str(studentId) not in tugStudentsDf["user_id"].astype(str).values:
                deleteEnrollment(localSetup, errorHandler, str(targetCourseId), str(enrollmentId))

        ## Step 5: Enroll students who are in the TUG list but not yet enrolled in SGA
        sgaEnrollmentsApiUrl = f"{sgaCoreApiUrl}/enrollments"
        for _, studentRow in tugStudentsDf.iterrows():
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
    enrollTugStudentsInSga(
        inputTerm=input("Enter the desired term in four character format (FA20, SU20, SP20): ")
    )

    input("Press enter to exit")
