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
This script reads a CSV file containing Canvas course IDs and changes the term for each course using the Canvas API.
"""
externalRequirements = r"""
To function properly, this script requires:
- Valid Canvas API configuration in Common_Configs (coreCanvasApiUrl, canvasAccessToken).
- A CSV file named "Target_Canvas_Course_Ids.csv" located in the Canvas internal
  resources directory (LocalSetup.getInternalResourcePaths("Canvas")) with columns:
    - canvas_course_id
    - canvas_term_id
"""

## This function sets the term for a course given its Canvas course ID and term ID
def setCourseTerm(
    localSetup: LocalSetup,
    errorHandler: errorEmail,
    courseId: str,
    termId: str,
) -> None:
    functionName = "setCourseTerm"
    try:
        setTermUrl = f"{coreCanvasApiUrl}courses/{courseId}"
        payload = {"course": {"enrollment_term_id": termId}}
        response, _ = makeApiCall(
            localSetup=localSetup,
            p1_apiUrl=setTermUrl,
            p1_payload=payload,
            p1_apiCallType="put",
        )

        if response.status_code == 200:
            localSetup.logInfoThreadSafe(f"Successfully set term for course with ID: {courseId}")
        else:
            localSetup.logWarningThreadSafe(f"Failed to set term for course with ID: {courseId}. Status code: {response.status_code}")

    except Exception as Error:
        errorHandler.sendError(functionName, Error)

## This function reads the CSV file and sets the term for the listed courses
def setListedCoursesTerm(localSetup: LocalSetup, errorHandler: errorEmail) -> None:
    functionName = "setListedCoursesTerm"
    try:
        canvasResourcePath = localSetup.getInternalResourcePaths("Canvas")
        targetCoursesCsvFilePath = os.path.join(canvasResourcePath, "Target_Canvas_Course_Ids.csv")

        ## Read the CSV file using pandas
        rawTargetCoursesDf = pd.read_csv(targetCoursesCsvFilePath)

        ## Retain only rows that have a value in canvas_course_id
        targetCoursesDf = rawTargetCoursesDf[rawTargetCoursesDf["canvas_course_id"].notna()]

        ## Process each course in a thread pool
        MAX_WORKERS = 25
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for index, row in targetCoursesDf.iterrows():

                ## Get the course id from the row
                courseId = str(row["canvas_course_id"]).replace('.0', '')

                ## Get the term id from the row
                termId = str(row["canvas_term_id"]).replace('.0', '')

                ## Submit the task to the thread pool
                executor.submit(setCourseTerm, localSetup, errorHandler, courseId, termId)

        localSetup.logInfoThreadSafe(f"{functionName} completed. Processed {len(targetCoursesDf)} courses.")

    except Exception as Error:
        errorHandler.sendError(functionName, Error)

if __name__ == "__main__":
    ## Initialize shared LocalSetup (paths, logging)
    localSetup = LocalSetup(datetime.now(), __file__)

    ## Setup the error handler (sends one email per function error)
    errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)

    setListedCoursesTerm(localSetup, errorHandler)

    input("Press enter to exit")
