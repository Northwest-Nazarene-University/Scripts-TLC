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
    from Common_Configs import coreCanvasApiUrl, canvasAccessToken
except ImportError:  ## When imported as a package/module
    from .Local_Setup import LocalSetup
    from .Error_Email import errorEmail
    from .TLC_Action import updateCourseField
    from .TLC_Common import readTargetCsv, runThreadedRows
    from .Common_Configs import coreCanvasApiUrl, canvasAccessToken

## Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = os.path.basename(__file__).replace(".py", "")
scriptPurpose = r"""
This script reads a CSV file containing Canvas course IDs and grading standard IDs
and updates the grading standard for each listed course using the Canvas Courses API
("Update a course" - course[grading_standard_id]).
"""
externalRequirements = r"""
To function properly, this script requires:
- Valid Canvas API configuration in Common_Configs (coreCanvasApiUrl, canvasAccessToken).
- A CSV file located in the Canvas internal resources directory
  (LocalSetup.getInternalResourcePaths("Canvas")) with columns:
    - canvas_course_id
    - grading_standard_id  (or grading_scheme_id)
"""

## Read the CSV and update the grading standard for each listed course
def changeListedCoursesGradingStandard(
    localSetup: LocalSetup,
    errorHandler: errorEmail,
    csvFileName: str = "Target_Canvas_Course_Ids.csv",
) -> None:
    functionName = "changeListedCoursesGradingStandard"
    try:
        ## Step 1: Load and validate the target CSV
        csvPath = os.path.join(localSetup.getInternalResourcePaths("Canvas"), csvFileName)
        df = readTargetCsv(
            localSetup, errorHandler, csvPath,
            requiredColumns=["canvas_course_id"],
        )
        if df.empty:
            return

        ## Step 2: Resolve which column holds the grading standard ID
        ## (Canvas API uses "grading_standard_id"; older CSVs may use "grading_scheme_id")
        if "grading_standard_id" in df.columns:
            gradingCol = "grading_standard_id"
        elif "grading_scheme_id" in df.columns:
            gradingCol = "grading_scheme_id"
        else:
            raise KeyError(
                "Expected column 'grading_standard_id' or 'grading_scheme_id' not found in CSV."
            )

        ## Step 3: Define the per-row worker
        def _worker(row):
            courseId          = str(row["canvas_course_id"]).replace(".0", "")
            gradingStandardId = str(row[gradingCol]).replace(".0", "")
            if not courseId or not gradingStandardId:
                localSetup.logWarningThreadSafe(
                    f"Skipping row with courseId='{courseId}' {gradingCol}='{gradingStandardId}'"
                )
                return
            ## The API expects an integer for grading_standard_id
            updateCourseField(
                localSetup, errorHandler, courseId,
                "grading_standard_id", int(gradingStandardId)
            )

        ## Step 4: Process all rows concurrently
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

    changeListedCoursesGradingStandard(localSetup, errorHandler)

    localSetup.logger.info(f"Script {scriptName} completed.")
    input("Press Enter to exit...")
