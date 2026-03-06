
# Author: Bryce Miller - brycezmiller@nnu.edu
# Last Updated by: Bryce Miller (refactored to update grading standards via Courses API)

import os, threading, time, pandas as pd, sys
from datetime import datetime

## Ensure ResourceModules are importable when running from ActionModules
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

# Try direct imports if run as main, else relative for package usage
try:
    from Local_Setup import LocalSetup
    from TLC_Common import makeApiCall
    from Error_Email import errorEmail
    from Common_Configs import coreCanvasApiUrl, canvasAccessToken
except ImportError:  # When imported as a package/module
    from .Local_Setup import LocalSetup
    from .TLC_Common import makeApiCall
    from .Error_Email import errorEmail
    from .Common_Configs import coreCanvasApiUrl, canvasAccessToken

## Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = "Change_Grading_Standard_For_Listed_Courses"
scriptPurpose = r"""
This script reads a CSV file containing Canvas course IDs and grading standard IDs
and updates the grading standard for each listed course using the Canvas Courses API
("Update a course" - course[grading_standard_id]).
"""
externalRequirements = r"""
To function properly, this script requires:
- Valid Canvas API configuration in Common_Configs (coreCanvasApiUrl, canvasAccessToken).
- A CSV file located in the Canvas internal resources directory
  (LocalSetup.getInternalResourcePaths("Canvas")) with:
    - canvas_course_id
    - grading_standard_id  (or grading_scheme_id)
"""

## Change the grading standard for the given course ID
def changeCourseGradingStandard(
    localSetup: LocalSetup,
    errorHandler: errorEmail,
    header: dict,
    courseId: str,
    gradingStandardId: str,
) -> None:
    """
    Change the grading standard for a Canvas course given its course ID and
    target grading standard ID.

    Uses the Courses "Update a course" endpoint:
    PUT /api/v1/courses/:id
    with course[grading_standard_id].
    """
    functionName = "changeCourseGradingStandard"
    try:
        updateCourseUrl = f"{coreCanvasApiUrl}courses/{courseId}"
        # Build payload using nested course object, matching Rails-style parameters
        payload = {
            "course": {
                "grading_standard_id": int(gradingStandardId)
            }
        }

        response, _ = makeApiCall(
            localSetup,
            p1_apiUrl=updateCourseUrl,
            p1_payload=payload,
            p1_apiCallType="put"
        )

        # makeApiCall may return a Response or a list; handle the primary case.
        statusCode = getattr(response, "status_code", None)

        if statusCode == 200:
            localSetup.logger.info(
                f"Successfully changed grading_standard_id for course with ID: "
                f"{courseId} to {gradingStandardId}"
            )
        else:
            localSetup.logger.warning(
                f"Failed to change grading_standard_id for course with ID: {courseId}. "
                f"Status code: {statusCode}"
            )

    except Exception as Error:
        errorHandler.sendError(functionName, Error)


## Change grading standards for listed courses from CSV
def changeListedCoursesGradingStandard(
    localSetup: LocalSetup,
    errorHandler: errorEmail,
    csvFileName: str = "Target_Canvas_Course_Ids.csv",
    threadSleep: float = 0.1,
) -> None:
    """
    Read a CSV file of course/grading standard IDs and change each course's grading standard.

    Expected columns in CSV:
      - canvas_course_id
      - grading_standard_id  (preferred)
        or grading_scheme_id (alias, will be treated as grading_standard_id)
    """
    functionName = "changeListedCoursesGradingStandard"
    try:
        # Canvas internal resources root
        canvasResourcePath = localSetup.getInternalResourcePaths("Canvas")
        targetCoursesCsvFilePath = os.path.join(canvasResourcePath, csvFileName)

        localSetup.logger.info(
            f"Starting {functionName}. Input file: {targetCoursesCsvFilePath}"
        )

        if not os.path.exists(targetCoursesCsvFilePath):
            raise FileNotFoundError(
                f"Target courses CSV not found: {targetCoursesCsvFilePath}"
            )

        header = {"Authorization": f"Bearer {canvasAccessToken}"}

        # Thread tracking
        ongoingThreads = []

        # Load CSV
        rawTargetCoursesDf = pd.read_csv(targetCoursesCsvFilePath)

        # Keep rows that have a value in canvas_course_id
        if "canvas_course_id" not in rawTargetCoursesDf.columns:
            raise KeyError(
                "Expected column 'canvas_course_id' not found in CSV."
            )

        # Support either grading_standard_id (Canvas term) or grading_scheme_id (your wording)
        gradingColumnName = None
        if "grading_standard_id" in rawTargetCoursesDf.columns:
            gradingColumnName = "grading_standard_id"
        elif "grading_scheme_id" in rawTargetCoursesDf.columns:
            gradingColumnName = "grading_scheme_id"
        else:
            raise KeyError(
                "Expected column 'grading_standard_id' or 'grading_scheme_id' "
                "not found in CSV."
            )

        targetCoursesDf = rawTargetCoursesDf[
            rawTargetCoursesDf["canvas_course_id"].notna()
        ]

        localSetup.logger.info(
            f"Found {len(targetCoursesDf)} target course rows with non-null canvas_course_id."
        )

        # Iterate and spawn threads
        for _, row in targetCoursesDf.iterrows():
            courseId = str(row["canvas_course_id"]).replace(".0", "")
            gradingStandardId = str(row[gradingColumnName]).replace(".0", "")

            # Skip if either is empty
            if not courseId or not gradingStandardId:
                localSetup.logger.warning(
                    f"Skipping row with courseId='{courseId}' "
                    f"{gradingColumnName}='{gradingStandardId}'"
                )
                continue

            changeGradingThread = threading.Thread(
                target=changeCourseGradingStandard,
                args=(localSetup, errorHandler, header, courseId, gradingStandardId),
                name=f"change_grading_standard_{courseId}",
                daemon=True,
            )
            changeGradingThread.start()
            ongoingThreads.append(changeGradingThread)

            # Sleep for a short time to avoid overloading the server
            time.sleep(threadSleep)

        # Check if all ongoing change grading standard threads have completed
        for thread in ongoingThreads:
            thread.join()

        localSetup.logger.info(
            f"{functionName} completed. Processed {len(targetCoursesDf)} courses."
        )

    except Exception as Error:
        errorHandler.sendError(functionName, Error)

if __name__ == "__main__":
    ## Initialize the local setup object and error handler
    localSetup = LocalSetup(datetime.now(), __file__)
    errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)

    localSetup.logger.info(
        f"Starting script: {scriptName} | Purpose: {scriptPurpose.strip()}"
    )

    # Change the grading standard for the listed courses
    changeListedCoursesGradingStandard(localSetup, errorHandler)

    localSetup.logger.info(f"Script {scriptName} completed.")
    input("Press Enter to exit...")
