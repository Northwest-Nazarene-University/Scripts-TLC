# Author: Bryce Miller - brycezmiller@nnu.edu
# Last Updated by: Bryce Miller

# Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = "Download_File"

ScriptPurpose = r"""
This class contains the method needed to download a file using a download url and save it at a given location
"""
externalRequirements = r"""
None
"""

import requests, logging, os, time

## Set working directory
fileDir = os.path.dirname(__file__)
os.chdir(fileDir)

## Relative Path (this changes depending on the working directory of the main script)
PFRelativePath = r".\\"

## If the Canvas directory is not in the folder the relative path points to
## find the Canvas directory and set the relative path to its parent folder
while "Scripts TLC" not in os.listdir(PFRelativePath):

    PFRelativePath = f"..\\{PFRelativePath}"

## Change the relative path to an absolute path
PFAbsolutePath = f"{os.path.abspath(PFRelativePath)}\\"

## Local Path Variables
baseLogPath = f"{PFAbsolutePath}Logs\\{scriptName}\\"

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

# This function makes a get call to a file url location, downloads and saves it
def downloadFile(file_link, inputFilePathwithName, mode):
    functionName = "download_file"

    ## Define path and file name variables to allow for the file name to be shortened if it is too long
    finalFilePathwithName = None
    finalfilePathwithoutName = None

    ## If the fileName is going to be longer than 256 characters, appreviate it. 
    if len(inputFilePathwithName) > 255:

        ## Get the filename from the provided path with name
        fileName = inputFilePathwithName.split("\\")[-1]

        ## Separate the name from the extension
        fileNameWithoutExt = fileName.split(".")[0]

        ## Determine the number of characters that need to be removed
        numOfCharToRemove = len(inputFilePathwithName) - 255

        ## Determine the cut off point for the name using the number of numOfCharToRemove
        fileNameCutOffPoint = len(fileNameWithoutExt) - numOfCharToRemove

        ## Create the new shortened filename
        newFileNameWithoutExt = fileNameWithoutExt[:fileNameCutOffPoint]

        ## Add the extension to the new filename
        newFileName = newFileNameWithoutExt + "." + fileName.split(".")[-1]

        ## Seperate the given path from the origianl name
        inputFilePathwithoutName = inputFilePathwithName.rsplit("\\", maxsplit=1)[0]

        ## Change the given path with name to have the same path but with the appreviated name
        finalFilePathwithName = inputFilePathwithoutName + "\\" + newFileName

    ## Otherwise
    else:

        ## Keep the input filename
        finalFilePathwithName = inputFilePathwithName

    ## Seperate the file path from the final filepath with name
    finalfilePathwithoutName = finalFilePathwithName.rsplit("\\", maxsplit=1)[0]
        
    ## If the filepath doesn't already exist
    if not (os.path.exists(finalfilePathwithoutName)):

        ## Create it
        os.makedirs(finalfilePathwithoutName, mode=0o777, exist_ok=False)

    ## Define a download attempt number and a variable to track if the download was successful
    attemptNum = 1
    completed = False

    ## try: to download the file at least 5 times
    while not completed and attemptNum <= 5:

        try: ## Irregular try clause, do not comment out in testing
            ## Make a get request to get the file
            the_file = requests.get(file_link, stream=True, allow_redirects=True)

            ## Download it chunk by chunk
            if (mode == "w"):
                with open(finalFilePathwithName, 'wb') as f:
                    for chunk in the_file.iter_content(1024 * 1024 * 2):  # 2 MiB chunks
                            f.write(chunk)
            if (mode == "a"):
                with open(finalFilePathwithName, 'ab') as f:
                    for chunk in the_file.iter_content(1024 * 1024 * 2):  # 2 MiB chunks
                            f.write(chunk)

            ## Note that the download was successful
            completed = True
                
        ## Upon error log the error and try: again after 5 seconds
        except Exception as error: ## Irregular except clause, do not comment out in testing
            logging.error (f"     \nError: {str(error)}")
            logging.error (f"     \ntrying again")
            time.sleep(5)
          
        ## Increment the attmept number
        attemptNum += 1