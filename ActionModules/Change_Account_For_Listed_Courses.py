## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller (refactored to use common TLC modules)

## Import Generic Moduels

import os, sys, threading, time, pandas as pd
from datetime import datetime

## Add the resource modules path
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

# Try direct imports if run as main, else relative for package usage
try:
    from Local_Setup import LocalSetup
    from TLC_Common import makeApiCall
    from Error_Email import errorEmail
    from Common_Configs import coreCanvasApiUrl, canvasAccessToken, scriptLibrary
except ImportError:  # When imported as a package/module
    from .Local_Setup import LocalSetup
    from .TLC_Common import makeApiCall
    from .Error_Email import errorEmail
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
  resources directory (LocalSetup.getInternalResourcePaths("Canvas")).
"""

## Set the account for the given course ID
def changeCourseAccount(
    localSetup: LocalSetup,
    errorHandler: errorEmail,
    courseId: str,
    accountId: str,
) -> None:
    """
    Change the account for a Canvas course given its course ID and target account ID.
    Uses TLC_Common.makeApiCall with retry behavior.
    """
    functionName = "changeCourseAccount"
    try:
        changeAccountUrl = f"{coreCanvasApiUrl}courses/{courseId}"
        payload = {"course": {"account_id": accountId}}

        response, _ = makeApiCall(
            localSetup=localSetup,
            p1_apiUrl=changeAccountUrl,
            p1_payload=payload,
            p1_apiCallType="put",
        )

        ## Get the status code
        statusCode = getattr(response, "status_code", None)

        if statusCode == 200:
            localSetup.logger.info(
                f"Successfully changed account for course with ID: {courseId} "
                f"to account_id: {accountId}"
            )
        else:
            localSetup.logger.warning(
                f"Failed to change account for course with ID: {courseId}. "
                f"Status code: {statusCode}"
            )

    except Exception as Error:
        errorHandler.sendError(functionName, Error)


def changeListedCoursesAccount(
    localSetup: LocalSetup,
    errorHandler: errorEmail,
    csvFileName: str = "Target_Canvas_Course_Ids.csv",
    threadSleep: float = 0.1,
) -> None:
    """
    Read a CSV file of course/account IDs and change each course's account.

    Expected columns in CSV:
      - canvas_course_id
      - canvas_account_id
    """
    functionName = "changeListedCoursesAccount"
    try:
        ## Canvas internal resources root
        canvasResourcePath = localSetup.getInternalResourcePaths("Canvas")
        targetCoursesCsvFilePath = os.path.join(canvasResourcePath, csvFileName)

        ## Log start
        localSetup.logger.info(
            f"Starting {functionName}. Input file: {targetCoursesCsvFilePath}"
        )

        ## Verify CSV file exists
        if not os.path.exists(targetCoursesCsvFilePath):
            raise FileNotFoundError(
                f"Target courses CSV not found: {targetCoursesCsvFilePath}"
            )

        ## Thread tracking
        ongoingThreads = []

        ## Load CSV
        rawTargetCoursesDf = pd.read_csv(targetCoursesCsvFilePath)

        ## Filter out rows with null canvas_course_id
        targetCoursesDf = rawTargetCoursesDf[
            rawTargetCoursesDf["canvas_course_id"].notna()
        ]

        ## Iterate and spawn threads
        for _, row in targetCoursesDf.iterrows():
            courseId = str(row["canvas_course_id"]).replace(".0", "")
            accountId = str(row["canvas_account_id"]).replace(".0", "")

            ## Skip if either is empty
            if not courseId or not accountId:
                localSetup.logger.warning(
                    f"Skipping row with courseId='{courseId}' accountId='{accountId}'"
                )
                continue

            ## Start thread to change account and append it to the tracking list
            changeAccountThread = threading.Thread(
                target=changeCourseAccount,
                args=(localSetup, errorHandler, courseId, accountId),
                name=f"change_course_{courseId}",
            )
            changeAccountThread.start()
            ongoingThreads.append(changeAccountThread)

            # Throttle slightly to avoid hammering the API
            time.sleep(threadSleep)

        # Wait for all threads to complete
        for thread in ongoingThreads:
            thread.join()

        ## Log completion
        localSetup.logger.info(
            f"{functionName} completed. Processed {len(targetCoursesDf)} courses."
        )

    except Exception as Error:
        errorHandler.sendError(functionName, Error)

if __name__ == "__main__":
    # Initialize shared LocalSetup (paths, logging)
    localSetup = LocalSetup(datetime.now(), __file__)

    # Setup the error handler (sends one email per function error)
    errorHandler = errorEmail(
        scriptName,
        scriptPurpose,
        externalRequirements,
        localSetup,
    )

    localSetup.logger.info(
        f"Starting script: {scriptName} | Purpose: {scriptPurpose.strip()}"
    )

    changeListedCoursesAccount(localSetup, errorHandler)

    localSetup.logger.info(f"Script {scriptName} completed.")
    input("Press Enter to exit...")
