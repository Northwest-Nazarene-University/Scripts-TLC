# Author: Bryce Miller - brycezmiller@nnu.edu
# Last Updated by: Bryce Miller

from datetime import datetime
from Download_File import downloadFile
import requests, time, json, os, logging, sys, re, pandas as pd

# Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = "Get_GPS_Students"

scriptPurpose = r"""
This script (Get_GPS_Students) uses the Canvas API to run a users report and download the .csv result.
It requires the following paramters: the path the .csv should be saved to (p1_SavePath), the API 
header (p1_header).
"""
externalRequirements = r"""
To function properly this script requires either no provided parameters (in which case it runs on 
the current term) or a term code in the FA20 format.
"""

## Date Variables
currentDate = datetime.now()
currentMonth = currentDate.month
currentYear = currentDate.year
century = str(currentYear)[:2]
decade = str(currentYear)[2:]

## Relative Path (this changes depending on the working directory of the main script)
PFRelativePath = r".\\"

## If the Canvas directory is not in the folder the relative path points to
## find the Canvas directory and set the relative path to its parent folder
while "Scripts TLC" not in os.listdir(PFRelativePath):

    PFRelativePath = f"..\\{PFRelativePath}"

## Change the relative path to an absolute path
PFAbsolutePath = f"{os.path.abspath(PFRelativePath)}\\"

## Add Input Modules to the sys path
sys.path.append(f"{PFAbsolutePath}Scripts TLC\\ResourceModules")
sys.path.append(f"{PFAbsolutePath}Scripts TLC\\ActionModules")

## Import local modules
from Error_Email_API import errorEmailApi
from Make_Api_Call import makeApiCall
from Get_Courses import termGetCourses
from Get_Enrollments import termGetEnrollments
from Get_Users import getUsers

## Local Path Variables
baseLogPath = f"{PFAbsolutePath}Logs\\{scriptName}\\"
outputPath = f"{PFAbsolutePath}Canvas Resources\\"
configPath = f"{PFAbsolutePath}Configs TLC\\"

## If the base log path doesn't already exist, create it
if not (os.path.exists(baseLogPath)):
    os.makedirs(baseLogPath, mode=0o777, exist_ok=False)

if not (os.path.exists(outputPath)):
    os.makedirs(outputPath, mode=0o777, exist_ok=False)

## Final length of relative Path
relPathLen = len(PFRelativePath)

## Canvas Instance Url
coreCanvasApiUrl = None
## Open the Core_Canvas_Url.txt from the config path
with open (f"{configPath}Core_Canvas_Url.txt", "r") as file:
    coreCanvasApiUrl = file.readlines()[0]

## If the script is run as main the folder with the access token is in the parent directory
canvasAccessToken = ""

## Open and retrieve the Canvas Access Token
with open (f"{configPath}\Canvas_Access_Token.txt", "r") as file:
    canvasAccessToken = file.readlines()[0]

## External Paths

## Define a variable to hold the base external input path which is where the sis input files are stored
baseExternalInputPath = None 
## Open Base_External_Paths.json from the config path and get the baseExternalInputPath value
with open (f"{configPath}Base_External_Paths.json", "r") as file:
    fileJson = json.load(file)
    baseExternalInputPath = fileJson["baseExternalInputPath"]

## Log configurations
logger = logging.getLogger(__name__)
rootFormat = ("%(asctime)s %(levelname)s %(message)s")
FORMAT = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
logging.basicConfig(format=FORMAT, filemode = "a", level=logging.INFO)

## Info Log Handler
infoLogFile = f"{baseLogPath}\\Info Log.txt"
logInfo = logging.FileHandler(infoLogFile, mode = 'a')
logInfo.setLevel(logging.INFO)
logInfo.setFormatter(FORMAT)
logger.addHandler(logInfo)

## Warning Log handler
warningLogFile = f"{baseLogPath}\\Warning Log.txt"
logWarning = logging.FileHandler(warningLogFile, mode = 'a')
logWarning.setLevel(logging.WARNING)
logWarning.setFormatter(FORMAT)
logger.addHandler(logWarning)

## Error Log handler
errorLogFile = f"{baseLogPath}\\Error Log.txt"
logError = logging.FileHandler(errorLogFile, mode = 'a')
logError.setLevel(logging.ERROR)
logError.setFormatter(FORMAT)
logger.addHandler(logError)

## The variable below holds a set of the functions that have had errors. This enables the except function to only send
## an error email the first time the function triggeres an error
setOfFunctionsWithErrors = set()

## This function handles function errors
def error_handler (p1_ErrorLocation, p1_ErrorInfo, sendOnce = True):
    functionName = "error_handler"
    logger.error (f"     \nA script error occured while running {p1_ErrorLocation}. " +
                     f"Error: {str(p1_ErrorInfo)}")
    ## If the function with the error has not already been processed send an email alert
    if (p1_ErrorLocation not in setOfFunctionsWithErrors):
        errorEmailApi.sendEmailError(p2_ScriptName = scriptName, p2_ScriptPurpose = scriptPurpose, 
                                     p2_ExternalRequirements = externalRequirements, 
                                     p2_ErrorLocation = p1_ErrorLocation, p2_ErrorInfo = f"{p1_ErrorInfo}: \n\n {traceback.format_exc()}")
        setOfFunctionsWithErrors.add(p1_ErrorLocation)
        ## Note that an error email was sent
        logger.error (f"     \nError Email Sent")
    ## Otherwise log the fact that an error email as already been sent
    else:
        logger.error (f"     \nError email already sent")

## This function uses the user provided term to determines which Canvas school users to pull users from
## with the Canvas Reports API and creates a .csv file of the users in those Canvas school users
def createGPSStudentsCsv(p1_inputTerm, attempt = 0):
    functionName = "createUsersCSV"
    try:
        logger.info (f"     \nStarting GPS Student report")

        ## Determine and save the term's school year
        schoolYear = None
        if re.search("AF|FA|GF", p1_inputTerm):
            ## Fall terms are the first terms of a new school year so FA20 is part of the 2020-21 school year.
            schoolYear = (century + p1_inputTerm[2:] + "-" + str(int(p1_inputTerm[2:]) + 1))
        elif re.search("SP|GS|AS|SG|SA|SU", p1_inputTerm):
            ## Spring and Summer terms belong in the same school year as the fall terms before them, so SP21 is part of the same 2020-21 school year as FA20.
            schoolYear = (century + str(int(p1_inputTerm[2:]) - 1) + "-" + p1_inputTerm[2:])

        ## Create the school year relavent output patha
        schoolYearOutputPath = f"{outputPath}\\{schoolYear}\\" if p1_inputTerm != "All" else f"{outputPath}"
        
        ## Define the term specific output path
        termOutputPath = f"{schoolYearOutputPath}{p1_inputTerm}\\" if p1_inputTerm != "All" else f"{outputPath}"
        
        ## If the user did not provide a term, set the target path to the default
        targetDestination = f"{termOutputPath}{p1_inputTerm}_GPS_Students.csv"
        
        ## If the target file exists
        if os.path.exists(targetDestination):
            
            ## Get its last moddifed timestamp
            targetFileTimestamp = os.path.getmtime(targetDestination)

            ## Convert the timestamp to datetime
            targetFileDateTime = datetime.fromtimestamp(targetFileTimestamp)

            ## Subtract the file's datetime from the current datetime
            targetFileHoursOld = int((currentDate - targetFileDateTime).total_seconds() // 3600)

            ## If it has been less than hour or more since the target was updated
            if targetFileHoursOld < 3.5:

                ## logger.info that the file is up to date and return
                logger.info (f"     \nTarget {targetDestination} CSV is up to date")
                return targetDestination
        
        ## Read (and update if neccessary) the canvas courses csv file for the term as a pandas df
        rawCanvasCourses = pd.read_csv(termGetCourses(inputTerm = p1_inputTerm))

        ## Filter the sis courses to only include GPS courses by exluding all rows that have "G_" or "_APP"
        ## in the account_id column and have TRUE in the created_by_sis column
        GPSCanvasCourses = rawCanvasCourses[rawCanvasCourses["account_id"].str.contains("G_|_APP")
                                   & (rawCanvasCourses["created_by_sis"] == True)
                                   ]


        ## Read (and update if neccessary) the canvas enrollment csv file for the term as a pandas df
        rawCanvasEnrollments = pd.read_csv(termGetEnrollments(inputTerm = p1_inputTerm))

        ## Filter the sis enrollments to only include those with that are enrolled in a GPSSisCourse course that have created by_sis set to TRUE
        GPSCanvasEnrollments = rawCanvasEnrollments[rawCanvasEnrollments["canvas_course_id"].isin(GPSCanvasCourses["canvas_course_id"])
                                                    & (rawCanvasEnrollments["created_by_sis"] == True)
                                                    & (rawCanvasEnrollments["role"] == "student")
                                           ]

        ## Read (and update if neccessary) the canvas users csv file for the term as a pandas df
        rawCanvasUsers = pd.read_csv(getUsers(inputTerm = p1_inputTerm))

        ## Filter the sis users to only include those whose ids are in the unique user_id column of the GPSCanvasEnrollments df
        GPSCanvasUsers = rawCanvasUsers[rawCanvasUsers["canvas_user_id"].isin(GPSCanvasEnrollments["canvas_user_id"].unique())]

        ## Save the filtered sis users to a csv file in the outputPath
        GPSCanvasUsers.to_csv(targetDestination, index=False)

        logger.info (f"     \nGPS Student report created successfully at {outputPath}GPS_Students.csv")

        ## Return the path to the created csv file
        return targetDestination



    except Exception as error:
        error_handler (functionName, p1_ErrorInfo = error)

def termGetGPSStudents(inputTerm = ""):
    functionName = "Get_GPS_Students"

    ## Start and download the Canvas report
    targetDestiniation = createGPSStudentsCsv(p1_inputTerm = inputTerm)

    ## Return the target destination
    return targetDestiniation

if __name__ == "__main__":

    ## Start the GPS student report creation process
    termGetGPSStudents (inputTerm = input("Enter the desired term in \
four character format (FA20, SU20, SP20): "))

    input("Press enter to exit")