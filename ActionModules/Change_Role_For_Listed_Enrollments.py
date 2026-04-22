## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

## Import Generic Modules
import os, sys, pandas as pd
from datetime import datetime

## Add the resource modules path
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

## Try direct imports if run as main, else relative for package usage
try:
    from Local_Setup import LocalSetup
    from Error_Email import errorEmail
    from TLC_Action import deleteEnrollment, enrollUser
    from TLC_Common import readTargetCsv, runThreadedRows
    from Common_Configs import coreCanvasApiUrl, canvasAccessToken
except ImportError:  ## When imported as a package/module
    from .Local_Setup import LocalSetup
    from .Error_Email import errorEmail
    from .TLC_Action import deleteEnrollment, enrollUser
    from .TLC_Common import readTargetCsv, runThreadedRows
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

## Read the CSV and change the role for each listed enrollment
def changeListedEnrollmentsRole(localSetup: LocalSetup, errorHandler: errorEmail) -> None:
    functionName = "changeListedEnrollmentsRole"
    try:
        ## Step 1: Load and validate the target CSV
        csvPath = os.path.join(
            localSetup.getInternalResourcePaths("Canvas"), "Target_Canvas_Enrollment_Ids.csv"
        )
        df = readTargetCsv(
            localSetup, errorHandler, csvPath,
            requiredColumns=["canvas_enrollment_id", "canvas_user_id", "canvas_course_id",
                             "role_id", "base_role_type"],
        )
        if df.empty:
            return

        ## Step 2: Define the per-row worker — re-enroll with new role then drop the old enrollment
        def _worker(row):
            enrollmentId = str(row["canvas_enrollment_id"]).replace(".0", "")
            userId       = str(row["canvas_user_id"]).replace(".0", "")
            courseId     = str(row["canvas_course_id"]).replace(".0", "")
            roleId       = str(row["role_id"]).replace(".0", "")
            baseRoleType = str(row["base_role_type"])
            enrollUser(localSetup, errorHandler, courseId, userId, baseRoleType, roleId=roleId)
            deleteEnrollment(localSetup, errorHandler, courseId, enrollmentId)

        ## Step 3: Process all rows concurrently
        runThreadedRows(df, _worker)
        localSetup.logInfoThreadSafe(f"{functionName} completed. Processed {len(df)} enrollments.")

    except Exception as Error:
        errorHandler.sendError(functionName, Error)

if __name__ == "__main__":
    ## Initialize shared LocalSetup (paths, logging)
    localSetup = LocalSetup(datetime.now(), __file__)

    ## Setup the error handler (sends one email per function error)
    errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)

    changeListedEnrollmentsRole(localSetup, errorHandler)

    input("Press enter to exit")

