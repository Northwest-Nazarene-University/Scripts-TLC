## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

## Import Generic Modules
import os, sys, threading, pandas as pd
from datetime import datetime

## Add the resource modules path
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

## Try direct imports if run as main, else relative for package usage
try:
    from Local_Setup import LocalSetup
    from TLC_Common import makeApiCall, runThreadedRows
    from Error_Email import errorEmail
    from Common_Configs import coreCanvasApiUrl, canvasAccessToken
except ImportError:  ## When imported as a package/module
    from .Local_Setup import LocalSetup
    from .TLC_Common import makeApiCall, runThreadedRows
    from .Error_Email import errorEmail
    from .Common_Configs import coreCanvasApiUrl, canvasAccessToken

## Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = os.path.basename(__file__).replace(".py", "")
scriptPurpose = r"""
This script reads a CSV file of Canvas courses and, for each course, identifies discussion
topics that do not allow threaded replies and records them. It produces a report CSV of all
unthreaded discussions found across the listed courses.
"""
externalRequirements = r"""
To function properly, this script requires:
- Valid Canvas API configuration in Common_Configs (coreCanvasApiUrl, canvasAccessToken).
- A CSV file named "Target_Canvas_Courses.csv" located in the Canvas internal resources
  directory (LocalSetup.getInternalResourcePaths("Canvas")) with columns:
    - canvas_course_id
    - course_id
"""

_sharedDataLock = threading.Lock()

## This function makes an API call to Canvas to collect unthreaded discussion topics for a course
def allowThreadedReplies(
    localSetup: LocalSetup,
    errorHandler: errorEmail,
    row,
    canvasCourseUnthreadedDiscussions: dict,
) -> None:
    functionName = "allowThreadedReplies"

    try:
        ## Define the course variables
        canvasCourseId = int(row['canvas_course_id'])
        sisCourseID = row['course_id']

        ## Define the payload that will be sent to each course discussion topics end point
        discussionTopicsPayload = {"per_page": 100}

        ## Make a url to get the course's discussions
        courseDiscussionTopicsApiUrl = f"{coreCanvasApiUrl}courses/{canvasCourseId}/discussion_topics"

        ## Make the API call with retry/rate-limit handling
        courseDiscussionTopicsResponse, _ = makeApiCall(
            localSetup=localSetup,
            p1_apiUrl=courseDiscussionTopicsApiUrl,
            p1_payload=courseDiscussionTopicsPayload,
        )

        ## If the API status code is anything other than 200 it is an error, log it and skip
        if courseDiscussionTopicsResponse.status_code != 200:
            localSetup.logErrorThreadSafe(f"\nCourse Error: {courseDiscussionTopicsResponse.status_code}")
            localSetup.logErrorThreadSafe(courseDiscussionTopicsApiUrl)
            return

        ## Get the course object as a json
        courseDiscussionTopicsDict = courseDiscussionTopicsResponse.json()

        ## If the course object is empty log and return
        if not courseDiscussionTopicsDict:
            localSetup.logInfoThreadSafe(f"\nNo discussion topics for course: {canvasCourseId}")
        else:
            ## For each discussion topic
            for topic in courseDiscussionTopicsDict:

                ## If the discussion isn't already threaded, record it
                if topic['discussion_type'] not in ["threaded", "side_comment"]:
                    discussionTitle = topic['title']
                    discussionUrl = topic['html_url']

                    with _sharedDataLock:
                        canvasCourseUnthreadedDiscussions["canvas_sis_id"].append(sisCourseID)
                        canvasCourseUnthreadedDiscussions["canvas_course_id"].append(canvasCourseId)
                        canvasCourseUnthreadedDiscussions["discussion title"].append(discussionTitle)
                        canvasCourseUnthreadedDiscussions["discussion url"].append(discussionUrl)

        localSetup.logInfoThreadSafe(f"Course {canvasCourseId} processed")

    except Exception as Error:
        errorHandler.sendError(functionName, Error)

## This function reads the CSV file and collects unthreaded discussion topics across all listed courses
def allowThreadedDiscussions(localSetup: LocalSetup, errorHandler: errorEmail) -> None:
    functionName = "allowThreadedDiscussions"

    try:
        canvasResourcePath = localSetup.getInternalResourcePaths("Canvas")

        ## Open the relevant Target_Canvas_Courses.csv as a df
        canvasCourses = pd.read_csv(os.path.join(canvasResourcePath, "Target_Canvas_Courses.csv"))

        ## Remove any rows that are all blank
        canvasCourses.dropna(how="all", inplace=True)

        ## Create a dict with canvas_sis_id, canvas_course_id, discussion title, and discussion url
        canvasCourseUnthreadedDiscussions = {
            "canvas_sis_id": [],
            "canvas_course_id": [],
            "discussion title": [],
            "discussion url": [],
        }

        ## Process each course concurrently; skip rows where canvas_course_id is absent
        def _worker(row):
            if pd.isna(row['canvas_course_id']):
                return
            allowThreadedReplies(localSetup, errorHandler, row, canvasCourseUnthreadedDiscussions)

        runThreadedRows(canvasCourses, _worker)

        ## Create a df from the canvasCourseUnthreadedDiscussions dict and save to CSV
        canvasCourseUnthreadedDiscussionsDF = pd.DataFrame(canvasCourseUnthreadedDiscussions)
        canvasCourseUnthreadedDiscussionsDF.to_csv(
            os.path.join(canvasResourcePath, "Canvas_Course_Unthreaded_Discussions.csv"),
            index=False,
        )

        localSetup.logInfoThreadSafe(f"{functionName} completed. Processed {len(canvasCourses)} courses.")

    except Exception as Error:
        errorHandler.sendError(functionName, Error)

if __name__ == "__main__":
    ## Initialize shared LocalSetup (paths, logging)
    localSetup = LocalSetup(datetime.now(), __file__)

    ## Setup the error handler (sends one email per function error)
    errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)

    allowThreadedDiscussions(localSetup, errorHandler)

    input("Press enter to exit")
