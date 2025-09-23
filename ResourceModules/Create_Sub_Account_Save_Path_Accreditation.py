# Author: Bryce Miller - brycezmiller@nnu.edu
# Last Updated by: Bryce Miller

## Import Generic Moduels

import traceback, os, sys, logging, os, os.path
import pandas as pd

## Import local modules
from Error_Email_API import errorEmailApi

## Set working directory
fileDir = os.path.dirname(__file__)
os.chdir(fileDir)

# Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = "Create Sub Account Save Path"

scriptPurpose = r"""
Receieve a Canvas sub account id create an incomplete department save path by combining the base path with the name of the given sub account and its parent accounts.
This incomplete path can then be added to the core path of the script using this function.
"""
externalRequirements = r"""
To function properly, this script requires a Canvas_Accounts.csv which is created by the Get_Accounts.py script
"""

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
from Download_File import downloadFile


## Local Path Variables
baseLogPath = f"{PFAbsolutePath}Logs\\{scriptName}\\"
baseLocalInputPath = f"{PFAbsolutePath}Canvas Resources\\"

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
    functionName = "error_handler"

    ## Log the error
    logger.error (f"A script error occured while running {p1_ErrorLocation}. " +
                     f"Error: {str(p1_ErrorInfo)}")

    ## If the function with the error has not already been processed send an email alert
    if (p1_ErrorLocation not in setOfFunctionsWithErrors):
        errorEmailApi.sendEmailError(p2_ScriptName = scriptName, p2_ScriptPurpose = scriptPurpose, 
                                     p2_ExternalRequirements = externalRequirements, 
                                     p2_ErrorLocation = p1_ErrorLocation, p2_ErrorInfo = f"{p1_ErrorInfo}: \n\n {traceback.format_exc()}")
        
        ## Add the function name to the set of functions with errors
        setOfFunctionsWithErrors.add(p1_ErrorLocation)
        
        ## Note that an error email was sent
        logger.error (f"Error Email Sent")
    
    ## Otherwise log the fact that an error email as already been sent
    else:
        logger.error (f"Error email already sent")


## This function determines the save path for a given account ID
def determineDepartmentSavePath(courseAccountId):
    functionName = "Determine Department Save Path"

    try:
        logger.info (f"Determining save path for {courseAccountId}")
        ## Add the courseAccountId as a key to p2_departmentSavePaths with the value being an empty string
        accountSavePath = ""

        ## Read the accounts csv into a pandas dataframe
        departmentSavePathsDF = pd.read_csv(f"{baseLocalInputPath}Canvas_Accounts.csv")

        ## Find the iloc associated with the courseAccountId is in the departmentSavePathsDF
        accountRow = departmentSavePathsDF.index.get_loc\
            (departmentSavePathsDF[departmentSavePathsDF["canvas_account_id"] == courseAccountId].index[0])

        ## Find and set the account name using the iloc
        accountName = departmentSavePathsDF["name"][accountRow]

        ## Add the account name to the relavent department save path
        accountSavePath = f"{accountName}\\"

        ## Add the parent account names to the path until the root account (denoted by "") is reached
        targetAccountParentID = departmentSavePathsDF["canvas_parent_id"][accountRow]

        while pd.notna(targetAccountParentID):
            ## Find the iloc associated with the parrent account name using the iloc
            parentAccountRow = departmentSavePathsDF.index.get_loc\
            (departmentSavePathsDF[departmentSavePathsDF["canvas_account_id"] == targetAccountParentID].index[0])

            ## Get the targetAccountParent's name
            targetAccountName = departmentSavePathsDF["name"][parentAccountRow]

            ## Save the current iteration of the path
            currentPath = accountSavePath

            ## Add the targetAccountParent's name to the current path
            accountSavePath = f"{targetAccountName}\\{currentPath}"

            ## Reset the targetAccountParentID to the parent of the current account
            targetAccountParentID = departmentSavePathsDF["canvas_parent_id"][parentAccountRow]

        if len(accountSavePath.rsplit("\\")) > 3:
            departmentName = accountSavePath.rsplit("\\")[1]

            accountSavePath = accountSavePath\
                .replace(f" {departmentName}", "")\
                .replace("College of Arts &\\", "College of Arts & Humanities\\")

        else:
            collegeName = accountSavePath.rsplit("\\")[0].replace("College of ", "")

        if "_" in accountSavePath \
            and "Undergraduate_" not in accountSavePath \
            and "Graduate_" not in accountSavePath:

            stringWithUnderscore = None

            if "Graduate " in accountSavePath:
                stringWithUnderscore = accountSavePath\
                    .split("Graduate ")[1]\
                    .split("_")[0]
            else:
                stringWithUnderscore = accountSavePath\
                    .split("Undergraduate ")[1]\
                    .split("_")[0]
            
            accountSavePath = accountSavePath\
                .replace(f"Undergraduate {stringWithUnderscore}_", "Undergraduate_")\
                .replace(f"Graduate {stringWithUnderscore}_", "Graduate_")\
                .replace(f"{stringWithUnderscore}_", "")

            
        return accountSavePath

    except Exception as error:
        error_handler (functionName, error)
        return ""