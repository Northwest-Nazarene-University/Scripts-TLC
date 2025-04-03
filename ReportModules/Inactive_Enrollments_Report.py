# Author: Bryce Miller - brycezmiller@nnu.edu
# Last Updated by: Bryce Miller


from datetime import datetime
import paramiko, os, logging, sys, requests, json, re, threading, time
import pandas as pd #External Download from https://pypi.org/project/pandas/

# Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = "Conclude Inactive Student's Enrollments"

scriptPurpose = r"""
This script (Inactive_Enrollments_Report) makes a special enrollments call to get all
enrollments from every term and turns it into a Pandas Dataframe. It then retrieves the list of 
students currently being imported to Canvas from the institution'a SIS and saves it as a Dataframe as
well. It then removes any enrollments from enrollment dataframe whose id's match the userlist, as well
as all teacher enrollments, and saves whats left in sis import csv format with conclude set to true.

Thus, the enrollments of all non-active students are concluded, ensuring they don't get global 
announcement notifications.
"""
externalRequirements = r"""
To function properly this script requires access to the institution's Canvas and a list of active 
students.
"""

## Date Variables
currentDate = datetime.now()
currentMonth = currentDate.month
currentYear = currentDate.year
century = str(currentYear)[:2]
decade = str(currentYear)[2:]
currentTerms = []
nearFutureTerms = []
currentAndNearFutureTerms = []
currentSchoolYear = None

## January through May is the Spring Term
if currentMonth >= 1 and currentMonth <= 5:
    ## Current semester's three active terms
    currentTerms.append(f"SP{str(currentYear)}")
    currentTerms.append(f"GS{str(currentYear)}")
    currentTerms.append(f"AS{str(currentYear)}")
    ## Next semester's six active terms
    nearFutureTerms.append(f"SU{str(currentYear)}")
    nearFutureTerms.append(f"SG{str(currentYear)}")
    nearFutureTerms.append(f"SA{str(currentYear)}")
    nearFutureTerms.append(f"FA{str(currentYear)}")
    nearFutureTerms.append(f"GF{str(currentYear)}")
    nearFutureTerms.append(f"AF{str(currentYear)}")

## June through August is the Summer Term
elif currentMonth >= 6 and currentMonth <= 8:
    ## Current semester's three active terms
    currentTerms.append(f"SU{str(currentYear)}")
    currentTerms.append(f"SG{str(currentYear)}")
    currentTerms.append(f"SA{str(currentYear)}")
    ## Next semester's six active terms
    nearFutureTerms.append(f"FA{str(currentYear)}")
    nearFutureTerms.append(f"GF{str(currentYear)}")
    nearFutureTerms.append(f"AF{str(currentYear)}")
    nearFutureTerms.append(f"SP{str(currentYear+1)}")
    nearFutureTerms.append(f"GS{str(currentYear+1)}")
    nearFutureTerms.append(f"AS{str(currentYear+1)}")

## The other months (September through December) is the Fall Term
else:
    ## Current semester's three active terms
    currentTerms.append(f"FA{str(currentYear)}")
    currentTerms.append(f"GF{str(currentYear)}")
    currentTerms.append(f"AF{str(currentYear)}")
    ## Next semester's six active terms
    nearFutureTerms.append(f"SP{str(currentYear+1)}")
    nearFutureTerms.append(f"GS{str(currentYear+1)}")
    nearFutureTerms.append(f"AS{str(currentYear+1)}")
    nearFutureTerms.append(f"SU{str(currentYear+1)}")
    nearFutureTerms.append(f"SG{str(currentYear+1)}")
    nearFutureTerms.append(f"SA{str(currentYear+1)}")

currentAndNearFutureTerms.extend(currentTerms)
currentAndNearFutureTerms.extend(nearFutureTerms)

## Define the current school year by whether it is before or during/after september
if f"FA{str(currentYear)}" in currentTerms:
    ## Fall terms are the first terms of a new school year so FA20 is part of the 2020-21 school year.
    currentSchoolYear = f"{currentYear}-{currentYear + 1}"
else:
    ## Spring and Summer terms belong in the same school year as the fall terms before them, so SP21 is part of the same 2020-21 school year as FA20.
    currentSchoolYear = f"{currentYear - 1}-{currentYear}"

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

## Import local modules
from Error_Email_API import errorEmailApi
from Get_Enrollments import termGetEnrollments

## Local Path Variables
baseLogPath = f"{PFAbsolutePath}Logs\\{scriptName}\\"
baseLocalInputPath = f"{PFAbsolutePath}Canvas Resources\\{str(currentYear)}\\"  ## This is only the base path as the real path requires the requested term
baseLocalInputPathWithoutYear = f"{PFAbsolutePath}Canvas Resources\\"
outputPath = f"{PFAbsolutePath}Canvas Resources\\"
configPath = f"{PFAbsolutePath}Configs TLC\\"

## External Path Variables

## Define a variable to hold the base external input path which is where the sis input files are stored
baseExternalInputPath = None 
## Open Base_External_Paths.json from the config path and get the baseExternalInputPath value
with open (f"{configPath}Base_External_Paths.json", "r") as file:
    fileJson = json.load(file)
    baseExternalInputPath = fileJson["baseExternalInputPath"]

## If the base log path doesn't already exist, create it
if not (os.path.exists(baseLogPath)):
    os.makedirs(baseLogPath, mode=0o777, exist_ok=False)

## Final length of relative Path
relPathLen = len(PFRelativePath)

## Canvas Instance Url
CoreCanvasAPIUrl = None
## Open the Core_Canvas_Url.txt from the config path
with open (f"{configPath}Core_Canvas_Url.txt", "r") as file:
    CoreCanvasAPIUrl = file.readlines()[0]

## If the script is run as main the folder with the access token is in the parent directory
canvasAccessToken = ""

## Open and retrieve the Canvas Access Token
with open (f"{configPath}\Canvas_Access_Token.txt", "r") as file:
    canvasAccessToken = file.readlines()[0]

## Log configurations
logger = logging.getLogger(__name__)
rootFormat = ("%(asctime)s %(levelname)s %(message)s")
FORMAT = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
logging.basicConfig(format=rootFormat, filemode = "a", level=logging.INFO)

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

## The variable below holds a set of the functions that have had errors. This enables the error_handler function to only send
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
                                     p2_ErrorLocation = p1_ErrorLocation, p2_ErrorInfo = p1_ErrorInfo)
        setOfFunctionsWithErrors.add(p1_ErrorLocation)
        ## Note that an error email was sent
        logger.error (f"     \nError Email Sent")
    ## Otherwise log the fact that an error email as already been sent
    else:
        logger.error (f"     \nError email already sent")

def concludeEnrollments():
    functionName = "Conclude Enrollments"

    try:
        ## Define the header for all subsequent canvas api calls
        header = {'Authorization' : 'Bearer ' + canvasAccessToken}

        ## Open the instance level enrollment file
        enrollmentsDF = pd.read_csv(baseLocalInputPathWithoutYear + "Canvas_Enrollments.csv", dtype={"user_id": str,})

        ## Open the current sis to Canvas user file
        activeUsersDf = pd.read_csv(rf"{baseExternalInputPath}canvas_user.csv", dtype={"user_id": str,})

        ## Create a list of active user sis ids
        activeUsersSisIDList = []
        activeUsersSisIDList.extend(["697670", "509993", "60947", "405720", "389669","706696", "520094"])
        for index, row in activeUsersDf.iterrows():
            activeUsersSisIDList.append(str(row['user_id']))

        ## Save a list of the uncessary columnns in the enrollmentsDF
        enrollmentCsvColumnsToDrop = ["canvas_associated_user_id", "associated_user_id", "created_by_sis", "base_role_type", "limit_section_privileges"]
        
        ## Parse the enrollmentsDF and create an inactiveEnrollmentsDF that only has the records whose id's are not among the activeUsersIDList list
        moddedEnrollmentsDF = enrollmentsDF.drop(columns = enrollmentCsvColumnsToDrop, inplace=False)

        # Remove enrollment records where Status is anything other than "active"
        moddedEnrollmentsDF = moddedEnrollmentsDF[moddedEnrollmentsDF["status"] == "active"]
        
        ## Create a list to hold the indexes of active user enrollments and those that have blank course ids
        inactiveCourseIndexList = []

        ## Create a dict to hold the canvas course ids and canvas user ids of enrollments with blank course sis ids
        blankCourseIDInactiveEnrollmentsList = []

        ## Define the current orientation course names to count their enrollments as active
        activeUndgOrientation = None
        if "SU" in currentTerms[0]:
            ## Summer does not have an active undg orientation so use the coming Fall orientation
            activeUndgOrientation = f"{nearFutureTerms[0][:2]}{currentYear}_TUG_Orientation"
        else:
            activeUndgOrientation = f"{currentTerms[0][:2]}{currentYear}_TUG_Orientation"
        ## The grad orientation is set by school year
        activeGradOrientation = f"{currentSchoolYear}_GPS_Orientation"
        undgTechOrientation = "NNU_IT_TECH_ORIENTATION"
        gradTechOrientation = "NNU_IT_Tech_Orientation_For_GPS_Students"

        ## Iterate through the enrollmentDF 
        for index, row in moddedEnrollmentsDF.iterrows():

            ## Save the row specific variables
            userID = row["user_id"]
            courseID = str(row["course_id"])
            enrollmentID = row["canvas_enrollment_id"]
            canvasCourseID = row["canvas_course_id"]

            ## Save a variable to track if the enrollment is in a current or future course
            currentOrFutureCourse = False

            for term in currentAndNearFutureTerms:
                if term in courseID:
                    currentOrFutureCourse = True
                    break

            if not currentOrFutureCourse:
                ## Check if the row's user id is in the active userSIS id List or if the target course is an active orientation
                ActiveUserEnrollment = userID in activeUsersSisIDList
                activeOrientationEnrollment = (
                    activeUndgOrientation in courseID
                    or activeGradOrientation in courseID
                    or undgTechOrientation in courseID
                    or gradTechOrientation in courseID
                )

                if not (ActiveUserEnrollment or activeOrientationEnrollment):
                
                    ## If the user is not active, check if the course_id is blank
                    if pd.isna(row["course_id"]) or pd.isna(row["user_id"]):

                        ##  If the course_id is blank, create a list with the canvas course id and user id, and append that list to the blank Course ID Inactive Enrollments List
                        blankCourseIDInactiveEnrollment = [canvasCourseID, enrollmentID]
                        blankCourseIDInactiveEnrollmentsList.append(blankCourseIDInactiveEnrollment)
                
                    else:

                        ##  If the course_id is not blank, add the index to the inactiveCourseIndexList
                        inactiveCourseIndexList.append(index)
        
        ## Create a inactive enrollment DF by dropping the enrollments for active users
        inactiveEnrollmentsDF = moddedEnrollmentsDF.loc[inactiveCourseIndexList]

        ## Set the status of all inactive enrollments to completed (concluded)
        inactiveEnrollmentsDF["status"] = "Completed"

        ## Save the inactive Enrollments DF as a csv file
        inactiveEnrollmentsDF.to_csv(f"{outputPath}Inactive_Enrollments.csv", index=False)

        ## Save the conclude enrollment SIS upload API URL
        concludeEnrollmentSisUploadApiUrl = f"{CoreCanvasAPIUrl}accounts/1/sis_imports"
        
        ##Save the conclude enrollment SIS upload payload
        concludeEnrollmentSisUploadPayload = {"attachment":f"{outputPath}Inactive_Enrollments.csv", "extension":"csv"}

        ## Make an API call to conclude the enrollment
        concludeEnrollmentSisUploadSectionObject = requests.post(concludeEnrollmentSisUploadApiUrl, headers = header)

        for enrollment in blankCourseIDInactiveEnrollmentsList:
            ## Save the specific course and user id
            canvasCourseId = enrollment[0]
            canvasEnrollmentId = enrollment[1]

            ## Save the conclude enrollment API URL
            concludeEnrollmentApiUrl = f"{CoreCanvasAPIUrl}courses/{canvasCourseId}/enrollments/{canvasEnrollmentId}"

            ## Make an API call to conclude the enrollment
            concludeEnrollmentSectionObject = requests.delete(concludeEnrollmentApiUrl, headers = header)





    except Exception as error:
        error_handler (functionName, p1_ErrorInfo = error)

if __name__ == "__main__":

    ## Start and download the Canvas report
    concludeEnrollments ()

    input("Press enter to exit")

else:
    ## If an argument was given use that as the input term
    
    if len(sys.argv) == 1:

        ## Start and download the Canvas report
        concludeEnrollments(inputTerm = "")
