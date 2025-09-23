## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller


from Error_Email_API import errorEmailApi
from datetime import datetime
import traceback, paramiko, os, logging, json, re, time ## External Installation: paramiko: https://www.paramiko.org/installing.html, 

## Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = "Get Slate Info"

scriptPurpose = r"""
This script (Get_Slate_Info) connects to the NNU's slate SFTP server to retrieve the csv of incoming
students for the coming semester.
"""
externalRequirements = r"""
To function properly this script requires access to the SFTP server
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

## Local Path Variables
baseLogPath = f"{PFAbsolutePath}Logs\\{scriptName}\\"
rawOutputPath = f"{PFAbsolutePath}Slate Resources\\"
configPath = f"{PFAbsolutePath}\\Configs TLC\\"

## If the base log path doesn't already exist, create it
if not (os.path.exists(baseLogPath)):
    os.makedirs(baseLogPath, mode=0o777, exist_ok=False)

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


## This function calls the GE Council's Outcome course code list Google Sheet and saves it as a csv
def getSlateInfo (p1_inputTerm):
    functionName = "Get Slate Info"

    try:

        ## Define a veriable to hold the slate creds json file
        slateCreds = None

        ## Define the number of attempts made this run to connect to the SFTP server
        attempt = 0
        ## Define the maximum number of retries to connect to the SFTP server before giving up
        retries = 5

        ## Open the slate creds json file from the configPath
        with open(f"{configPath}Slate_Creds.json", "r") as file:

            ## Load the json file
            slateCreds = json.load(file)

        ## Define the slate creds
        ASHost = slateCreds["ASHost"]
        ASPort = slateCreds["ASPort"]
        ASUsername = slateCreds["ASUsername"]
        ASPassword = slateCreds["ASPassword"]
        ASPublicKeyPath = f"{configPath}Slate_Public_Key.txt"

        ## Create an SSH client
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        #try to connect to the SFTP server
        while attempt < retries:
            try:
                ssh_client.connect(hostname=ASHost
                                   , port=ASPort
                                   , username=ASUsername
                                   , password=ASPassword
                                   , key_filename=ASPublicKeyPath
                                   , banner_timeout=60
                                   )

                ## Create an SFTP client from the SSH client
                sftp_client = ssh_client.open_sftp()

                ## If the connection is successful, log the success and break out of the loop
                break

            ## If the connection fails
            except Exception as error:

                ## Increment the attempt counter
                attempt += 1

                ## If the maximum number of retries has not been reached, log the error and retry
                if attempt < retries:
                    logger.warning(f"Attempt {attempt} failed: {error}. Retrying in 1 minute...")
                    time.sleep(60)

                ## Otherwise, log the error and return None
                else:
                    logger.error(f"Attempt {attempt} failed: {error}. No more retries.")
                    error_handler (functionName, p1_ErrorInfo=error)
                    return None

        ## Connect to the SFTP server
        #ssh_client.connect(hostname=ASHost, port=ASPort, username=ASUsername, password=ASPassword, key_filename=ASPublicKeyPath)#, command=ASCommandLine)

        

        ## List the contents of the remote directory
        fileList = sftp_client.listdir("./Outgoing//canvas")

        ## Specify the remote file path
        remoteBasePath = "./Outgoing//Canvas//"

        ## Determine and save the term's school year
        targetSchoolYear = None
        if re.search("AF|FA|GF", p1_inputTerm):
            ## Fall terms are the first terms of a new school year so FA20 is part of the 2020-21 school year.
            targetSchoolYear = (century + p1_inputTerm[2:] + "-" + str(int(p1_inputTerm[2:]) + 1))
        elif re.search("SP|GS|AS|SG|SA|SU", p1_inputTerm):
            ## Spring and Summer terms belong in the same school year as the fall terms before them, so SP21 is part of the same 2020-21 school year as FA20.
            targetSchoolYear = (century + str(int(p1_inputTerm[2:]) - 1) + "-" + p1_inputTerm[2:])

        ## Define the incoming School Year input path
        incomingSchoolYearInputPath = f"{rawOutputPath}{targetSchoolYear}\\"

        ## Define the term specific output path
        termOutputPath = f"{incomingSchoolYearInputPath}{p1_inputTerm}\\Incoming\\"
        
        ## If the incomingSchoolYearInputPath doesn't already exist, create it
        if not (os.path.exists(incomingSchoolYearInputPath)):
            os.makedirs(incomingSchoolYearInputPath, mode=0o777, exist_ok=False)

        ## If the localFilePath doesn't already exist, create it
        if not (os.path.exists(termOutputPath)):
            os.makedirs(termOutputPath, mode=0o777, exist_ok=False)

        ## Make a list of the downloaded file paths
        downloadedFiles = []

        try: ## Irregular try clause, do not comment out in testing
            ## Download the file from the SFTP server
            for file in fileList:
                fullRemotePath = f"{remoteBasePath}{file}"
                fullLocalPath = f"{termOutputPath}{file}"
                sftp_client.get(fullRemotePath, fullLocalPath)

                ## Define a variable to hold the file contents
                fileContents = None

                ## Read the file
                with open(fullLocalPath, "r") as file:
                    fileContents = file.read()
                    
                ## Count the number of \n's in the file, and if there is only one occurence
                ## the file is empty and should be deleted
                if fileContents.count("\n") == 1:
                    os.remove(fullLocalPath)

                    ## Split the full local path by .
                    splitFullLocalPath = fullLocalPath.split(".")

                    ## Split the file path by . to remove the file extension, and if a version of the file with _canvas_data exists delete it
                    if len(splitFullLocalPath) > 1:
                        if os.path.exists(f"{splitFullLocalPath[0]}_canvas_data.{splitFullLocalPath[1]}"):
                            os.remove(f"{splitFullLocalPath[0]}_canvas_data.{splitFullLocalPath[1]}")

                    logger.warning(f"{fullLocalPath} is empty and has been deleted.")

                ## Otherwise
                else:

                    ## Add the downloaded file to the list of downloaded files
                    downloadedFiles.append(fullLocalPath)

            ## Log that the files were downloaded successfully
            logger.info("Files downloaded successfully.")
        finally:
            ## Close the SFTP client and SSH connection
            sftp_client.close()
            ssh_client.close()

        return downloadedFiles



    except Exception as error:
        error_handler (functionName, p1_ErrorInfo = error)

if __name__ == "__main__":

    ## Define the API Call header using the retreived Canvas Token
    header = {'Authorization' : f"Bearer {canvasAccessToken}"}

    ## Start and download the Canvas report
    getSlateInfo (p1_inputTerm = input("Enter the desired term in \
four character format (FA20, SU20, SP20): "))

    input("Press enter to exit")