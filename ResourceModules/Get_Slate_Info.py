## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

## Import Generic Modules
from datetime import datetime
import traceback, paramiko, os, logging, json, re, time ## External Installation: paramiko: https://www.paramiko.org/installing.html

try: ## If the module is run directly
    from Error_Email import errorEmail
    from Local_Setup import LocalSetup
except ImportError: ## Otherwise as a relative import if the module is imported
    from .Error_Email import errorEmail  ## Import errorEmailApi
    from .Local_Setup import LocalSetup

from Common_Configs import undgTermsCodesToWordsDict, gradTermsCodesToWordsDict

## Define the purpose, and external requirements for logging and error reporting purposes
__scriptName = os.path.basename(__file__).replace(".py", "")

scriptPurpose = r"""
This script (Get_Slate_Info) connects to the NNU's slate SFTP server to retrieve the csv of incoming
students for the coming semester.
"""
externalRequirements = r"""
To function properly this script requires access to the SFTP server
"""

## Create the localsetup varabile
localSetup = localSetup = LocalSetup(datetime.now(), __file__)

## Setup the error handler
errorHandler = errorEmail(__scriptName, scriptPurpose, externalRequirements, localSetup)

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
        with open(os.path.join(localSetup.configPath, "Slate_Creds.json"), "r") as file:

            ## Load the json file
            slateCreds = json.load(file)

        ## Define the slate creds
        ASHost = slateCreds["ASHost"]
        ASPort = slateCreds["ASPort"]
        ASUsername = slateCreds["ASUsername"]
        ASPassword = slateCreds["ASPassword"]
        ASPublicKeyPath = os.path.join(localSetup.configPath, "Slate_Public_Key.txt")

        ## Create an SSH client
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        # try to connect to the SFTP server
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
            except Exception as Error:

                ## Increment the attempt counter
                attempt += 1

                ## If the maximum number of retries has not been reached, log the error and retry
                if attempt < retries:
                    localSetup.logger.warning(f"Attempt {attempt} failed: {Error}. Retrying in 1 minute...")
                    time.sleep(60)

                ## Otherwise, log the error and return None
                else:
                    localSetup.logger.error(f"Attempt {attempt} failed: {Error}. No more retries.")
                    errorHandler.sendError (functionName, p1_ErrorInfo=Error)
                    return None
       

        ## List the contents of the remote directory
        fileList = sftp_client.listdir("./Outgoing//canvas")

        ## Specify the remote file path
        remoteBasePath = "./Outgoing//Canvas//"

        ## Determine and save the term's school year
        termCodePrefix = p1_inputTerm[:2]
        termWord = gradTermsCodesToWordsDict[termCodePrefix] if termCodePrefix in gradTermsCodesToWordsDict.keys() else undgTermsCodesToWordsDict[termCodePrefix]
        targetSchoolYear = localSetup.getSchoolYear(termWord, localSetup.dateDict["year"])

        ## Define the incoming School Year input path  
        incomingSchoolYearInputPath = os.path.join(localSetup.getInternalResourcePaths("Slate"), targetSchoolYear)
        termOutputPath = os.path.join(incomingSchoolYearInputPath, p1_inputTerm, "Incoming")
        os.makedirs(termOutputPath, exist_ok=True)
        
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
                fullRemotePath = os.path.join(remoteBasePath, file)
                fullLocalPath = os.path.join(termOutputPath, file)
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

                    localSetup.logger.warning(f"{fullLocalPath} is empty and has been deleted.")

                ## Otherwise
                else:

                    ## Add the downloaded file to the list of downloaded files
                    downloadedFiles.append(fullLocalPath)

            ## Log that the files were downloaded successfully
            localSetup.logger.info("Files downloaded successfully.")
        finally:
            ## Close the SFTP client and SSH connection
            sftp_client.close()
            ssh_client.close()

        return downloadedFiles



    except Exception as Error:
        errorHandler.sendError(functionName, Error)

if __name__ == "__main__":

    ## Start and download the Canvas report
    getSlateInfo (p1_inputTerm = input("Enter the desired term in \
four character format (FA20, SU20, SP20): "))

    input("Press enter to exit")