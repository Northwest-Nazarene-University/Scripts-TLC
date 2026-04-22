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
    from TLC_Action import updateCourseField
    from TLC_Common import readTargetCsv, runThreadedRows
    from Common_Configs import coreCanvasApiUrl, canvasAccessToken, scriptLibrary
except ImportError:  ## When imported as a package/module
    from .Local_Setup import LocalSetup
    from .Error_Email import errorEmail
    from .TLC_Action import updateCourseField
    from .TLC_Common import readTargetCsv, runThreadedRows
    from .Common_Configs import coreCanvasApiUrl, canvasAccessToken, scriptLibrary

## Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = os.path.basename(__file__).replace(".py", "")
scriptPurpose = r"""
This script reads a CSV file containing Canvas course IDs and changes the account
for each course using the Canvas API.
"""
externalRequirements = r"""
To function properly, this script requires:
- Valid Canvas API configuration in Common_Configs (coreCanvasApiUrl, canvasAccessToken).
- A CSV file named "Target_Canvas_Course_Ids.csv" located in the Canvas internal
  resources directory (LocalSetup.getInternalResourcePaths("Canvas")) with columns:
    - canvas_course_id
    - canvas_account_id
"""

## Read the CSV and change the Canvas account for each listed course
def changeListedCoursesAccount(
    localSetup: LocalSetup,
    errorHandler: errorEmail,
    csvFileName: str = "Target_Canvas_Course_Ids.csv",
) -> None:
    functionName = "changeListedCoursesAccount"
    try:
        ## Step 1: Load and validate the target CSV
        csvPath = os.path.join(localSetup.getInternalResourcePaths("Canvas"), csvFileName)
        df = readTargetCsv(
            localSetup, errorHandler, csvPath,
            requiredColumns=["canvas_course_id", "canvas_account_id"],
        )
        if df.empty:
            return

        ## Step 2: Define the per-row worker
        def _worker(row):
            courseId  = str(row["canvas_course_id"]).replace(".0", "")
            accountId = str(row["canvas_account_id"]).replace(".0", "")
            if courseId and accountId:
                updateCourseField(localSetup, errorHandler, courseId, "account_id", accountId)
            else:
                localSetup.logWarningThreadSafe(
                    f"Skipping row with courseId='{courseId}' accountId='{accountId}'"
                )

        ## Step 3: Process all rows concurrently
        runThreadedRows(df, _worker)
        localSetup.logInfoThreadSafe(f"{functionName} completed. Processed {len(df)} courses.")

    except Exception as Error:
        errorHandler.sendError(functionName, Error)

if __name__ == "__main__":
    ## Initialize shared LocalSetup (paths, logging)
    localSetup = LocalSetup(datetime.now(), __file__)

    ## Setup the error handler (sends one email per function error)
    errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)

    localSetup.logger.info(f"Starting script: {scriptName} | Purpose: {scriptPurpose.strip()}")

    changeListedCoursesAccount(localSetup, errorHandler)

    localSetup.logger.info(f"Script {scriptName} completed.")
    input("Press Enter to exit...")

