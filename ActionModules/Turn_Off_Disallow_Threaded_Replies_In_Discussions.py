## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

## Import Generic Moduels
from __future__ import print_function
import traceback, os, sys, logging, requests, os, os.path, time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from datetime import date
import pandas as pd

## Set working directory
os.chdir(os.path.dirname(__file__))

## Add Script repository to syspath
sys.path.append(f"{os.getcwd()}\ResourceModules")

## Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = "Turn off disallow threaded discussions"

## Script file identifier
scriptRequirementMissingFolderIdentifier = "Missing_Syllabi"

scriptPurpose = r"""
The Outcome Exporter script is to copy the most recent relative outcome/s into the c ourses that need them.
"""
externalRequirements = r"""
To function properly this script requires a spreadsheet of the most recent outcomes and the courses they are assigned to.
"""

## Date Variables
currentDateTime = datetime.now()
currentMonth = currentDateTime.month
currentYear = currentDateTime.year
century = str(currentYear)[:2]
decade = str(currentYear)[2:]

## Time variables
currentDateTime = date.today()
current_year = currentDateTime.year
lastYear = current_year - 1
nextYear = current_year + 1
century = str(current_year)[:2]
decade = str(current_year)[2:]

## Set working directory
fileDir = os.path.dirname(__file__)
os.chdir(fileDir)

## The relative path is used to provide a generic way of finding where the Scripts TLC folder has been placed
## This provides a non-script specific manner of finding the vaiours related modules
PFRelativePath = r".\\"

## If the Scripts TLC folder is not in the folder the PFRelative path points to
## look for it in the next parent folder
while "Scripts TLC" not in os.listdir(PFRelativePath):

    PFRelativePath = f"..\\{PFRelativePath}"

## Change the relative path to an absolute path
absolutePath = f"{os.path.abspath(PFRelativePath)}\\"

## Add Input Modules to the sys path
sys.path.append(f"{absolutePath}Scripts TLC\\ResourceModules")

## Import local modules
from Error_Email import errorEmail

## Local Path Variables
baseLogPath = f"{absolutePath}Logs\\{scriptName}\\"
configPath = f"{absolutePath}\\Configs TLC\\"
baseLocalInputPath = f"{absolutePath}Canvas Resources\\"

## Canvas Instance Url
coreCanvasApiUrl = ""
## Open the Core_Canvas_Url.txt from the config path
with open (f"{configPath}Core_Canvas_Url.txt", "r") as file:
    coreCanvasApiUrl = file.readlines()[0]

## If the script is run as main the folder with the access token is in the parent directory
canvasAccessToken = ""

## Open and retrieve the Canvas Access Token
with open (fr"{configPath}Canvas_Access_Token.txt", "r") as file:
    canvasAccessToken = file.readlines()[0]

## Begin localSetup.logger set up

## If the base log path doesn't already exist, create it
if not (os.path.exists(baseLogPath)):
    os.makedirs(baseLogPath, mode=0o777, exist_ok=False)

## Log configurations
logger = logging.getLogger(__name__)
rootFormat = ("%(asctime)s %(levelname)s %(message)s")
FORMAT = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
logging.basicConfig(format=rootFormat, filemode = "a", level=logging.INFO)

## Local setup shim for compatibility with shared modules
class _LoggerShim:
    def __init__(self, p_logger):
        self.logger = p_logger
localSetup = _LoggerShim(logger)

## Info Log Handler
infoLogFile = f"{baseLogPath}\\Info Log.txt"
logInfo = logging.FileHandler(infoLogFile, mode = 'a')
logInfo.setLevel(logging.INFO)
logInfo.setFormatter(FORMAT)
localSetup.logger.addHandler(logInfo)

## Warning Log handler
warningLogFile = f"{baseLogPath}\\Warning Log.txt"
logWarning = logging.FileHandler(warningLogFile, mode = 'a')
logWarning.setLevel(logging.WARNING)
logWarning.setFormatter(FORMAT)
localSetup.logger.addHandler(logWarning)

## Error Log handler
errorLogFile = f"{baseLogPath}\\Error Log.txt"
logError = logging.FileHandler(errorLogFile, mode = 'a')
logError.setLevel(logging.ERROR)
logError.setFormatter(FORMAT)
localSetup.logger.addHandler(logError)

## Setup the error handler
errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)

## This function processes the rows of the CSV file and sends on the relavent data to process_course
def addOutcomeToCourse (row, p2_inputTerm, p1_header, p1_outcomeCourseDict):
    functionName = "Add Outcome/s to courses"

    try:
        
        ## If the row's course_sis_id is empty skip it
        if pd.isna(row['Course_sis_id']):
            return

        ## For each row in our CSV file pull the course sis id column and outcome column names
        ## Sample sess values: FA2022_PHIL2030_01
        ## Sample outcome value: GE_CF4_V1.0
        targetCourseSisId = row['Course_sis_id']
        outcomeKeys = [col for col in row.keys() if "Outcome" in col and "Area" not in col]
            
        ## Log the start of the process
        localSetup.logger.info("\n     Course:" + targetCourseSisId)

        ## Create the URL the API call will be made to
        course_API_url = coreCanvasApiUrl + "courses/sis_course_id:" + targetCourseSisId + "/course_copy"
        
        ## For each outcome in the row
        for outcome in outcomeKeys:
            
            ## If the outcome is empty skip it
            if pd.isna(row[outcome]):
                continue
            
            ## Get the canvas course id from the outcomeCourseDict
            canvasCourseId = p1_outcomeCourseDict[row[outcome]]

            ## Create the API Payload from the outcome sis id
            payload = {'source_course': canvasCourseId, 'only[]': ['outcomes']}
                
            ## Make the API call and save the result as course_object
            course_object = requests.post(course_API_url, headers = p1_header, params = payload)
                
            ## If the API status code is anything other than 200 it is an error, so log it and skip
            if (course_object.status_code != 200):
                localSetup.logger.error("\nCourse Error: " + str(course_object.status_code))
                localSetup.logger.error(course_API_url)
                localSetup.logger.error(course_object.url)
            else:
                ## Successfully made the API call
                localSetup.logger.info("\nOutcome copy successful for : " + targetCourseSisId)

    except Exception as Error:
        errorHandler.sendError(functionName, Error)

## This function makes a makes an api call to Canvas to set a course's discussion topic to allow threaded replies
def allowThreadedReplies (p1_row, p1_header, p1_canvasCourseUnthreadedDiscussions):
    
    functionName = "allowThreadedReplies"

    try:

            ## Define the course vaables
            canvasCourseId = int(p1_row['canvas_course_id'])
            sisCourseID = p1_row['course_id']

            ## Define the payload that will be sent to each course discussion topics end point
            discussionTopicsPayload = {"per_page": 100}
        
            ## Define the payload that will be sent to each course discussion end point
            discussionTopicPayload = {"discussion_type": "threaded"}
            
            ## Make a url to get the the courses's discussions
            courseDiscussionTopicsApiUrl = coreCanvasApiUrl + "courses/" + str(canvasCourseId) + "/discussion_topics"

            ## Make the API call
            courseDiscussionTopicsObject = requests.get(courseDiscussionTopicsApiUrl, headers = p1_header, params = discussionTopicsPayload)

            ## If the api status code is 403
            if (courseDiscussionTopicsObject.status_code == 403):

                ## So long as the API status code is 403, wait 2 seconds and try again
                while (courseDiscussionTopicsObject.status_code == 403):
                    
                    ## Log that the course has been rate limited
                    localSetup.logger.warning("\nRate limited for course: " + str(canvasCourseId))
                    
                    ## Wait 2 seconds
                    time.sleep(5) 

                    ## Make the API call again
                    courseDiscussionTopicsObject = requests.get(courseDiscussionTopicsApiUrl, headers = p1_header, params = discussionTopicsPayload)

            ## If the API status code is anything other than 200 it is an error, so log it and skip
            if (courseDiscussionTopicsObject.status_code != 200):
                localSetup.logger.error("\nCourse Error: " + str(courseDiscussionTopicsObject.status_code))
                localSetup.logger.error(courseDiscussionTopicsApiUrl)
                localSetup.logger.error(courseDiscussionTopicsObject.url)

            ## Otherwise
            else:
                
                ## Get the course object as a json
                courseDiscussionTopicsDict = courseDiscussionTopicsObject.json()

                ## If the course object is empty
                if not courseDiscussionTopicsDict:
                    
                    ## Log that the course has no discussion topics
                    localSetup.logger.info("\nNo discussion topics for course: " + str(canvasCourseId))
                
                ## Otherwise
                else:

                    ## For each discussion topic
                    for topic in courseDiscussionTopicsDict:

                        ## if the discussion isn't already threaded
                        if topic['discussion_type'] not in ["threaded", "side_comment"]:

                            ## Get the discussion title and url
                            discussionTitle = topic['title']
                            discussionUrl = topic['html_url']

                            ## Add the course's information to the canvasCourseUnthreadedDiscussions dict
                            p1_canvasCourseUnthreadedDiscussions["canvas_sis_id"].append(sisCourseID)
                            p1_canvasCourseUnthreadedDiscussions["canvas_course_id"].append(canvasCourseId)
                            p1_canvasCourseUnthreadedDiscussions["discussion title"].append(discussionTitle)
                            p1_canvasCourseUnthreadedDiscussions["discussion url"].append(discussionUrl)

            localSetup.logger.info (f"Course {canvasCourseId} processed")
                            
    except Exception as Error:
        errorHandler.sendError(functionName, Error)

## This function opens the CSV file, the save locations json file, sends the information on, and closes both files
def allowThreadedDiscussions():
    functionName = "outcome_exporter"
    
    try:

        ## Define the API Call header using the retreived Canvas Token
        header = {'Authorization' : f"Bearer {canvasAccessToken}"}

        ## Open the relevant Active_GE_Course.csv as a df
        canvasCourses = pd.read_csv(f"{baseLocalInputPath}Target_Canvas_Courses.csv")

        ## Remove any rows that area all blank
        canvasCourses.dropna(how = "all", inplace = True)

        ## Create a dict with canvas_sis_id, canvas_course_id, discussion title, and discussion url, each with an empty list as the value
        canvasCourseUnthreadedDiscussions = {"canvas_sis_id": []
                                                , "canvas_course_id": []
                                                , "discussion title": []
                                                , "discussion url": []
                                                }
        

        ## Process each course in a thread pool
        MAX_WORKERS = 25
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

            ## For each row in the Canvas Courses DF
            for index, row in canvasCourses.iterrows():

                ## If the row's course_sis_id is empty skip it
                if pd.isna(row['canvas_course_id']):
                    continue

                ##if row["course_id"] == "FA2024_ACCT2060_01":

                ## Submit the task to the thread pool
                executor.submit(allowThreadedReplies, row, header, canvasCourseUnthreadedDiscussions)

        ## Create a df from the canvasCourseUnthreadedDiscussions dict
        canvasCourseUnthreadedDiscussionsDF = pd.DataFrame(canvasCourseUnthreadedDiscussions)
        
        ## Save the df to a csv
        canvasCourseUnthreadedDiscussionsDF.to_csv(f"{baseLocalInputPath}Canvas_Course_Unthreaded_Discussions.csv", index = False)
     
    except Exception as Error:
        errorHandler.sendError(functionName, Error)

if __name__ == "__main__":

    ## Start and download the Canvas reportz
    allowThreadedDiscussions ()

    input("Press enter to exit")