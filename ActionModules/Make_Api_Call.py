# Author: Bryce Miller - brycezmiller@nnu.edu
# Last Updated by: Bryce Miller

## Import Generic Moduels
from __future__ import print_function
import traceback, os, sys, logging, requests, os, os.path, time, subprocess
from datetime import datetime
from datetime import date

## Set working directory
os.chdir(os.path.dirname(__file__))

## Add Script repository to syspath
sys.path.append(f"{os.getcwd()}\ResourceModules")

# Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = "Make an Api Call and return the result"

# Script file identifier
scriptRequirementMissingFolderIdentifier = "Missing_Syllabi"

scriptPurpose = r"""
The Make Api call script is designed to make an api call to the Canvas LMS and return the results of the call. It is also set to try the call multiple times, pausing in between, in case the the call experiences a temporary falure.
"""
externalRequirements = r"""
This script requires a valid access header and url.
"""

## Date Variables
currentDate = datetime.now()
currentMonth = currentDate.month
currentYear = currentDate.year
century = str(currentYear)[:2]
decade = str(currentYear)[2:]

# Time variables
currentDate = date.today()
current_year = currentDate.year
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
PFAbsolutePath = f"{os.path.abspath(PFRelativePath)}\\"

## Add Input Modules to the sys path
sys.path.append(f"{PFAbsolutePath}Scripts TLC\\ResourceModules")

## Import local modules
from Error_Email_API import errorEmailApi

## Local Path Variables
baseLogPath = f"{PFAbsolutePath}Logs\\{scriptName}\\"
configPath = f"{PFAbsolutePath}\\Configs TLC\\"
baseLocalInputPath = f"{PFAbsolutePath}Canvas Resources\\"

## Canvas Instance Url
CoreCanvasAPIUrl = None
## Open the Core_Canvas_Url.txt from the config path
with open (f"{configPath}Core_Canvas_Url.txt", "r") as file:
    CoreCanvasAPIUrl = file.readlines()[0]

## If the script is run as main the folder with the access token is in the parent directory
canvasAccessToken = ""

## Open and retrieve the Canvas Access Token
with open (fr"{configPath}Canvas_Access_Token.txt", "r") as file:
    canvasAccessToken = file.readlines()[0]

## Begin logger set up

## If the base log path doesn't already exist, create it
if not (os.path.exists(baseLogPath)):
    os.makedirs(baseLogPath, mode=0o777, exist_ok=False)

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

## This variable enables the except function to only send
## an error email the first time the function triggeres an error
## by tracking what functions have already been recorded as having errors
setOfFunctionsWithErrors = set()

## This function handles function errors
def error_handler (p1_ErrorLocation, p1_ErrorInfo, sendOnce = True):
    functionName = "except"

    ## Log the error
    logger.error (f"     \nA script error occured while running {p1_ErrorLocation}. " +
                     f"Error: {str(p1_ErrorInfo)}")

    ## If the function with the error has not already been processed send an email alert
    if (p1_ErrorLocation not in setOfFunctionsWithErrors):
        errorEmailApi.sendEmailError(p2_ScriptName = scriptName, p2_ScriptPurpose = scriptPurpose, 
                                     p2_ExternalRequirements = externalRequirements, 
                                     p2_ErrorLocation = p1_ErrorLocation, p2_ErrorInfo = f"{p1_ErrorInfo}: \n\n {traceback.format_exc()}")
        
        ## Add the function name to the set of functions with errors
        setOfFunctionsWithErrors.add(p1_ErrorLocation)
        
        ## Note that an error email was sent
        logger.error (f"     \nError Email Sent")
    
    ## Otherwise log the fact that an error email as already been sent
    else:
        logger.error (f"     \nError email already sent")

## This function takes a api header and url and returns the json object of the api call, recursively calling itself in a seperate instance up to 5 times if the call fails
def makeApiCall (p1_header = {'Authorization' : f"Bearer {canvasAccessToken}"}
                 , p1_apiUrl = CoreCanvasAPIUrl
                 , p1_payload = {}
                 , apiCallType = "get"
                 , outcomeViewApiAttempts = 0
                 ):
    functionName = "Make API Call"

    try:

        ## Make a variable to hold the outcome api object
        apiObject = None

        ## Make a variable to hold a list of outcome api objects if there is more than one
        apiObjectList = None
                    
        ## If the api call was not sucessful and the number of outcomeViewApiAttempts is less than 5
        try: ## Irregular try clause, do not comment out in testing
                                    
            ## If the number of attempts is greater than 0
            if apiObject:
                                
                ## Close the previous api object
                apiObject.close()
                
                ## Wait for 2 seconds
                time.sleep(2)
                    
            ## If the api call type is a get
            if apiCallType.lower() == "get":
                    
                ## If there is a p1_payload
                if p1_payload:

                    ## Add per page 100 to the payload if it isn't already there
                    if "per_page" not in p1_payload.keys():
                        p1_payload["per_page"] = 100

                    apiObject = requests.get(
                        url=p1_apiUrl
                        , headers = p1_header
                        , params = p1_payload
                        )
                else:
                    apiObject = requests.get(
                        url=p1_apiUrl
                        , headers = p1_header
                        , params = {"per_page": 100}
                        )
                        
            ## If the api call type is a post
            elif apiCallType.lower() == "post":
                    
                ## If there is a p1_payload
                if p1_payload:
                    apiObject = requests.post(
                        url=p1_apiUrl
                        , headers = p1_header
                        , params = p1_payload
                        )
                else:
                    apiObject = requests.post(
                        url=p1_apiUrl
                        , headers = p1_header
                        )   
                        
            ## If the api call type is a put
            elif apiCallType.lower() == "put":

                ## If there is a p1_payload
                if p1_payload:
                    apiObject = requests.put(url=p1_apiUrl, headers=p1_header, json=p1_payload)

                ## If there is no payload
                else:
                    apiObject = requests.put(url=p1_apiUrl, headers=p1_header)

            ## If the api call type is a delete
            elif apiCallType.lower() == "delete":

                ## If there is a p1_payload
                if p1_payload:
                    apiObject = requests.delete(
                        url=p1_apiUrl
                        , headers = p1_header
                        , params = p1_payload
                        )

                ## If there is no payload
                else:
                    apiObject = requests.delete(
                        url=p1_apiUrl
                        , headers = p1_header
                        )   
                
            ## If there is a next page and the current page has content
            if hasattr(apiObject, 'links') and 'next' in getattr(apiObject, 'links', {}):
                    
                ## Add the current page to the api object list
                apiObjectList = [apiObject]
                    
                ## Recursively call the function to get the next page
                nextPage = makeApiCall(p1_header, apiObject.links["next"]["url"], p1_payload = p1_payload)
                    
                ## If the now called next page has content
                if isinstance(nextPage, list) or nextPage.json():
                    
                    ## Add the next page object to the api object list
                    apiObjectList.append(nextPage) if not isinstance(nextPage, list) else apiObjectList.extend(nextPage)
                                        
        except Exception as error: ## Irregular except clause, do not comment out in testing
            logger.warning(f"Error: {error} \n Occured when calling {p1_apiUrl}")


                            
        ## Increment the number of attempts
        outcomeViewApiAttempts += 1

        ## If the api object is not successful and the number of attempts is less than 5
        if (
            (not apiObject
                or not apiObject.status_code == 200
                ) 
            and outcomeViewApiAttempts > 5
            ):

            ## Run the script as a subprocess
            subprocessRunResult = subprocess.run([
                "python"
                , os.path.abspath(__file__)
                , p1_header
                , p1_apiUrl
                , p1_payload
                , apiCallType
                , outcomeViewApiAttempts
                ]
                , capture_output = True
                , text = True
                )

            ## Get the apiObjectList or apiObject from the subprocess run result
            apiObjectList = subprocessRunResult.stdout if subprocessRunResult.stdout else apiObjectList
            
        ## Return the api object list if there is one, otherwise return the api object
        return apiObjectList if apiObjectList else apiObject

    except Exception as error:
        error_handler (functionName, error)

## If the script is run as main, run makeApiCall as a subprocess call
if __name__ == "__main__":

    ## Get the needed variables from the command line
    apiHeader = sys.argv[1]
    apiUrl = sys.argv[2]
    apiPayload = sys.argv[3]
    apiCallType = sys.argv[4]
    outcomeViewApiAttempts = sys.argv[5]

    ## Call the makeApiCall function
    makeApiCall (p1_header = apiHeader
                 , p1_apiUrl = apiUrl
                 , p1_payload = apiPayload
                 , apiCallType = apiCallType
                 , outcomeViewApiAttempts = outcomeViewApiAttempts
                 )