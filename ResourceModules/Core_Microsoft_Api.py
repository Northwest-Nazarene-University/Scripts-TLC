# Author: Bryce Miller - brycezmiller@nnu.edu
# Last Updated by: Bryce Miller

# Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = "Core_Microsoft_Api"

ScriptPurpose = r"""
This class contains the base methods and variables to send emails with the Outlook API
"""
externalRequirements = r"""

"""


from math import e
import os, sys, base64, time, sys, asyncio, configparser, getpass, microsoftgraph, requests, mimetypes, logging

from Download_File import downloadFile
from azure.identity import AuthenticationRecord
from azure.identity import TokenCachePersistenceOptions
from msgraph import GraphServiceClient
from configparser import SectionProxy
from azure.identity import InteractiveBrowserCredential
from msgraph.generated.users.users_request_builder import UsersRequestBuilder
from msgraph.generated.shares.item.drive_item.drive_item_request_builder import DriveItemRequestBuilder
from msgraph.generated.users.item.mail_folders.item.messages.messages_request_builder import (
    MessagesRequestBuilder)
from msgraph.generated.users.item.send_mail.send_mail_post_request_body import (
    SendMailPostRequestBody)
from msgraph.generated.models.message import Message
from msgraph.generated.models.item_body import ItemBody
from msgraph.generated.models.body_type import BodyType
from msgraph.generated.models.file import File
from msgraph.generated.models.recipient import Recipient
from msgraph.generated.models.email_address import EmailAddress
from msgraph.generated.models.drive_item import DriveItem
from kiota_abstractions.base_request_configuration import RequestConfiguration
from msgraph.generated.models.drive_item_uploadable_properties import DriveItemUploadableProperties
from msgraph.generated.drives.item.items.item.create_upload_session.create_upload_session_request_builder import CreateUploadSessionRequestBuilder
from msgraph.generated.drives.item.items.item.create_upload_session.create_upload_session_post_request_body import CreateUploadSessionPostRequestBody
from msgraph.generated.drives.item.items.item.workbook.functions.large.large_request_builder import LargeRequestBuilder


## Set working directory
fileDir = os.path.dirname(__file__)
os.chdir(fileDir)

## The relative path is used to provide a generic way of finding where the Scripts TLC folder has been placed
## This provides a non-script specific manner of finding the vaiours related modules
PFRelativePath = ".\\"

## If the Scripts TLC folder is not in the folder the PFRelative path points to
## look for it in the next parent folder
while "Scripts TLC" not in os.listdir(PFRelativePath):

    PFRelativePath = f"..\\{PFRelativePath}"

## Change the relative path to an absolute path
PFAbsolutePath = f"{os.path.abspath(PFRelativePath)}\\"

## Local Paths
microsoftResourcePath = f"{PFAbsolutePath}Microsoft Resources\\"
departmentalDataResroucesPath = f"{PFAbsolutePath}Departmental Data Resources\\"

## If the Microsoft Resources folder doesn't exist, create it
if not (os.path.exists(microsoftResourcePath)):
    os.makedirs(microsoftResourcePath, mode=0o777, exist_ok=False)
    
## If the Departmental Data Resources folder doesn't exist, create it
if not (os.path.exists(departmentalDataResroucesPath)):
    os.makedirs(departmentalDataResroucesPath, mode=0o777, exist_ok=False)
    
## Define the base log path
baseLogPath = f"{PFAbsolutePath}Logs\\{scriptName}\\"

## Add the Resource Modules folder to the sys path
sys.path.append(f"{PFAbsolutePath}Scripts TLC\\ResourceModules")
sys.path.append(f"{PFAbsolutePath}Scripts TLC\\Configs")

## Define the max chunk size for file uploads
maxChunkSize = 320 * 1024

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

class CoreMicrosoftAPI:
    settings: SectionProxy
    device_code_credential = None
    user_client: GraphServiceClient
    storedGraphConfig = None
    storedMicrosoftUsername = None

    def __init__(self, graphConfigType, microsoftUserName):

        ## If the graphConfigType is outlook lowercase
        if graphConfigType.lower() == 'outlook':
            configFileName = "Outlook_API_Config"
            
        ## Else if the graphConfigType is onedrive lowercase
        elif graphConfigType.lower() == 'onedrive':
            configFileName = "OneDrive_and_Sharepoint_API_Config"

        ## Get the user running the script
        pythonUser = getpass.getuser()    
            
        ## Store the config and username values
        self.storedGraphConfig = graphConfigType
        self.storedMicrosoftUsername = microsoftUserName

        ## Load the config settings
        config = configparser.ConfigParser()
        config.read([f"{PFAbsolutePath}Configs TLC\\{configFileName}.cfg", 'config.dev.cfg'])
        self.settings = config['azure']
        client_id = self.settings['clientId']
        tenant_id = self.settings['tenantId']
        graph_scopes = self.settings['graphUserScopes'].split(' ')
        
        ## If there is an authenticated DeviceCodeCredential at the relavent pathZ
        if os.path.exists(os.path.join(microsoftResourcePath, f"{pythonUser}_Owned_{configFileName}_Authenticated_{microsoftUserName}_Credential.json")):
            
            ## Load the authenticated microsoft account crediintial
            with open(os.path.join(microsoftResourcePath, f"{pythonUser}_Owned_{configFileName}_Authenticated_{microsoftUserName}_Credential.json"), "r") as json_file:
                deserialized_record = AuthenticationRecord.deserialize(json_file.read())
                self.device_code_credential = InteractiveBrowserCredential(disable_automatic_authentication=True, cache_persistence_options=TokenCachePersistenceOptions(), authentication_record=deserialized_record)
                
        ## Otherwise create a new DeviceCodeCredential
        else:
            
            self.device_code_credential = InteractiveBrowserCredential(client_id = client_id, tenant_id = tenant_id, cache_persistence_options=TokenCachePersistenceOptions())
            authenticatedDeviceCodeCredential = self.device_code_credential.authenticate(scopes=graph_scopes)
            authenticatedDeviceCodeCredentialJson = authenticatedDeviceCodeCredential.serialize()
            with open(os.path.join(microsoftResourcePath, f"{pythonUser}_Owned_{configFileName}_Authenticated_{microsoftUserName}_Credential.json"), "w") as json_file:
                json_file.write(authenticatedDeviceCodeCredentialJson)
                
        self.user_client = GraphServiceClient(self.device_code_credential, graph_scopes)

    ## This function sends an email asynchroniously with the option of using a shared mailbox 
    async def send_mail_async(self, subject: str, body: str, recipientEmailList: list, shared_mailbox: str = None, attempt = 1):
        
        # Create the message
        message = Message()
        
        # Set the message subject and create the message body
        message.subject = subject
        message.body = ItemBody()

        # Set the message body type and content
        if "!DOCTYPE html" in body:
            message.body.content_type = BodyType.Html
            
        else:
            message.body.content_type = BodyType.Text
            
        message.body.content = body

        ## Create a list of intended recipients
        message.to_recipients = []

        ## The code takes email addresses as a single string with email addresses seperated by commas
        ## so split the email addresses into a list and remove any spaces
        recipientEmailList = [email.strip() for email in recipientEmailList.split(',')]

        ## Remove aby empty values from the list
        recipientEmailList = [email for email in recipientEmailList if email]
        
        ## For each recipient
        for recipientEmail in recipientEmailList:

            ## Create a recipient object
            intendedRecipient = Recipient()
        
            ## Create an email address object and set it to the recipient's email
            intendedRecipient.email_address = EmailAddress()
            intendedRecipient.email_address.address = recipientEmail
            
            ## Add the recipient to the list of recipients
            message.to_recipients.append(intendedRecipient)

        ## If a shared mailbox was given
        if shared_mailbox:
            
            ## Create a recipient object to act as a sender object and set the email address to the shared mailbox
            from_sender = Recipient(email_address=EmailAddress(address=shared_mailbox))
            message.from_ = from_sender

        ## Create the request body which contains the subject, body, and recipients
        request_body = SendMailPostRequestBody(message=message)
        
        ## Send the email
        await self.user_client.me.send_mail.post(request_body)

    ## This function downloads a microsoft file asynchroniously using a shared link
    async def downloadSharedMicrosoftFileAsync(self, fileShareUrl, downloadSavePath, fileName = None, attempt = 1):

        ## Change the url to base64
        base64FileShareUrl = base64.urlsafe_b64encode(fileShareUrl.encode('utf-8')).decode('utf-8').rstrip('=').replace('/', '_').replace('+', '-')

        ## Change the base64 url to an encoded url
        encodedFileShareUrl = f"u!{base64FileShareUrl}"

        ## Define a variable to hold the api call result
        fileResult = await self.user_client.shares.by_shared_drive_item_id(encodedFileShareUrl).drive_item.get()
        
        ## Save the file download path
        fileDownloadUrl = fileResult.additional_data['@microsoft.graph.downloadUrl']
        
        ## If a file name was not given
        if not fileName:
            fileName = fileResult.name
            
        ## Otherwise, if a file name was given
        else:
            
            ## If the file name does not have the same extension as the file
            if not fileName.endswith(fileResult.name.split('.')[-1]):

                ## Add the correct file extension to the file name
                fileName = f"{fileName}.{fileResult.name.split('.')[-1]}"
            
        ## Make the download save location an absolute path
        absoluteDownloadSavePath = os.path.abspath(downloadSavePath)

        ## Download the file
        downloadFile(fileDownloadUrl, f"{absoluteDownloadSavePath}\\{fileName}", "w")
        
        return os.path.abspath(f"{absoluteDownloadSavePath}\\{fileName}")

    ## This function finds takes an encoded file share url and file name and returns whether the url is a folder and the relavent drive id and drive item id if they exist
    async def findSharedMicrosoftFileAsync(self, p1_encodedFileShareUrl, fileName):

        ## Define variables to contain whether the target url is a folder and any relevant drive ids and drive item ids
        targetUrlIsAFolder = False
        targetUrlDriveItemId = None
        targetUrlDriveId = None
        targetFileDriveItemID = None
        targetFileDriveID = None

        ## Define a variable to hold the api call result
        requestResult = await self.user_client.shares.by_shared_drive_item_id(p1_encodedFileShareUrl).drive_item.get()

        ## Record the target url's drive id and drive item id
        targetUrlDriveItemId = requestResult.id
        targetUrlDriveId = requestResult.parent_reference.drive_id

        ## If the name of the target equals the file name
        if requestResult.name == fileName:

            ## Record the drive id and drive item id
            targetFileDriveItemID = requestResult.id
            targetFileDriveID = requestResult.parent_reference.drive_id


        ## Otherwise
        else:
            
            ## If the target is a folder
            if requestResult.folder:

                ## Set the target url is a folder to true
                targetUrlIsAFolder = True

                ## Search for the file in the folder
                searchResult = await self.user_client.drives.by_drive_id(requestResult.parent_reference.drive_id).items.by_drive_item_id(requestResult.id).search_with_q(fileName).get()
                
                ## If there are results
                if searchResult.value:

                    ## For each result
                    for result in searchResult.value:

                        ## If the result name equals the file name and no drive id has been found
                        if result.name == fileName and not targetFileDriveID:

                            ## Record the drive id and drive item id
                            targetFileDriveItemID = result.id
                            targetFileDriveID = result.parent_reference.drive_id

        ## Return target variables
        return targetUrlIsAFolder, targetUrlDriveItemId, targetUrlDriveId, targetFileDriveItemID, targetFileDriveID                    
    
    ## This function uploads a new microsoft file or updates an existing one
    async def uploadSharedMicrosoftFileAsync(self, targetShareUrl, uploadItemFilePath):

        ## Get the name from the file path and mime type
        fileBaseName = os.path.basename(uploadItemFilePath)
        test = os.path.splitext(uploadItemFilePath)[1]
        fileMimeType = mimetypes.guess_type(test)

        ## Make a drive item object with the file name
        #uploadItem = DriveItem(name = fileBaseName, file = File(mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"))
        uploadItem = DriveItem(name = fileBaseName, file = File())

        ## Change the url to base64
        base64TargetShareUrl = base64.urlsafe_b64encode(targetShareUrl.encode('utf-8')).decode('utf-8').rstrip('=').replace('/', '_').replace('+', '-')
        
        ## Change the base64 url to an encoded url
        encodedTargetShareUrl = f"u!{base64TargetShareUrl}"
        
        ## Look for the target file at the target url and get the drive id and drive item id for both where possible
        targetUrlIsAFolder, targetUrlDriveItemId, targetUrlDriveId, targetFileDriveItemID, targetFileDriveID = await self.findSharedMicrosoftFileAsync(encodedTargetShareUrl, fileBaseName)

        ## If a target file drive id and drive item id were found or if the target url is a folder
        if (targetFileDriveID and targetFileDriveItemID) or targetUrlIsAFolder:

            ## Define a request body for the upload
            requestBody = {
                'item' : {
                    'name' : fileBaseName
                    }
                }

            ## Define variables to hold the upload seession builder upload body, and drive item uploadable properties
            uploadSessionBuilder = None
            uploadBody = CreateUploadSessionPostRequestBody.create_from_discriminator_value(uploadItem)
            driveItemUploadableProperties = DriveItemUploadableProperties(additional_data = {'@microsoft.graph.conflictBehavior': 'replace'})

            ## Set the upload body's item to the drive item uploadable properties
            uploadBody.item = driveItemUploadableProperties
            
            ## If drive and drive item ids were found
            if targetFileDriveID and targetFileDriveItemID:

                ## Create the upload session builder with the target ids
                uploadSessionBuilder = self.user_client.drives.by_drive_id(targetFileDriveID).items.by_drive_item_id(targetFileDriveItemID).create_upload_session

            ## Otherwise, if the target url is a folder
            elif targetUrlIsAFolder:

                ## Create a new drive item inside the folder
                createNewDriveItemResult = await self.user_client.drives.by_drive_id(targetUrlDriveId).items.by_drive_item_id(targetUrlDriveItemId).children.post(uploadItem)
                    
                ## Create the upload session builder with the target url ids
                uploadSessionBuilder = self.user_client.drives.by_drive_id(targetUrlDriveId).items.by_drive_item_id(createNewDriveItemResult.id).create_upload_session
            
                                
            ## Create the upload session
            uploadSession = await uploadSessionBuilder.post(uploadBody)

            ## Get the upload url
            uploadURL = uploadSession.upload_url

            ## Open the file
            with open (uploadItemFilePath, 'rb') as fileContent:

                ## Get the byte size of the file
                fileByteSize = os.path.getsize(uploadItemFilePath)                            
                            
                ## Count the number of complete chunks that will be needed to upload the file
                numberOfFileChunks = fileByteSize // maxChunkSize

                ## Calculate the amount left over after the chunks are uploaded in max chunk sizes
                fileChunkRemainder = fileByteSize - (numberOfFileChunks * maxChunkSize)

                ## Create a chunk counter variable
                fileChunkCounter = 0

                ## While the file hasn't completed uploading
                while fileChunkCounter <= numberOfFileChunks:

                    ## Read a chunk size worth of data from the file
                    targetChunkData = fileContent.read(maxChunkSize)

                    ## Define the content range for the chunk
                    contentStart = fileChunkCounter * maxChunkSize
                    contentEnd = contentStart + maxChunkSize 

                    ## If it is the last chunk
                    if fileChunkCounter == numberOfFileChunks:

                        ## Set the content range for the last chunk using the remainder
                        contentEnd = contentStart + fileChunkRemainder

                    ## Define the headers
                    headers = {
                        "Content-Range": f"bytes {contentStart}-{contentEnd-1}/{fileByteSize}",
                        "Content-Length": str(maxChunkSize)
                    }   
                                
                    ## Upload the file
                    uploadResult = requests.put(uploadURL, data = targetChunkData, headers = headers)
                                    
                    ## Increment the chunk counter
                    fileChunkCounter += 1
                                    
                    ## logger.info the number of chunks remaining
                    logger.info (f"File Segments Remaining: {numberOfFileChunks - fileChunkCounter}")

## This function us for when the script is called as a subprocess
def runAsASubprocess ():
    
    ## Check if at least 3 sys arguments are passed and if the first sys argument is either outlook or onedrive and if the second sys argument is an email address
    if not len(sys.argv) >= 3 or not (sys.argv[1].lower() != 'outlook' or sys.argv[1].lower() != 'onedrive') or not ('@' in sys.argv[2]):
        
        ## logger.info the usage of the general script
        logger.info("Usage: graphConfigType: <Outlook or OneDrive>, microsoftUsername: <MicrosoftEmailAddress>")
        sys.exit(2)
        
    ## If the first sys argu passed is 'Outlook'
    if sys.argv[1].lower() == 'outlook':  
        
        ## Check if at least 5 sys arguments are passed
        if not len(sys.argv) >= 5:

            ## logger.info the usage of the outlook portion of the script
            logger.info("Usage: graphConfigType: <Outlook or OneDrive>, microsoftUsername: <MicrosoftEmailAddress>, subject: <EmailSubject>, body: <EmailBody>, recipientEmailList: <RecipientEmailList>, optional shared_mailbox: <SharedMailboxAddress>")
            sys.exit(2)   

        ## Save the sys arguments as variables
        configType = sys.argv[1]
        MicrosoftUserName = sys.argv[2]
        subject = sys.argv[3]
        body = sys.argv[4]
        recipientEmailList = sys.argv[5]
        shared_mailbox = sys.argv[6]

        ## Create a outlookApi object
        outlookApi = CoreMicrosoftAPI(graphConfigType = configType, microsoftUserName = MicrosoftUserName)
        
        ## Send the email using the async method
        emailerResult = asyncio.run(outlookApi.send_mail_async(subject, body, recipientEmailList, shared_mailbox))
        
        ## logger.info and return the emailer result
        logger.info (emailerResult)
        print (emailerResult)
        return emailerResult

    ## If the first sys argu passed is 'Onedrive'
    elif sys.argv[1].lower() == 'onedrive':    

        ## Check if at least 5 sys arguments are passed
        if not len(sys.argv) >= 6:

            ## logger.info the usage of the onedrive portion of the script
            logger.info("Usage: graphConfigType: <Outlook or OneDrive>, microsoftUsername: <MicrosoftEmailAddress>, action: <'download' or 'upload', url: <MicrosoftSharedFileUrl>, filePath: <DownloadPath>, optional fileName: <Final Name and exstension of the downloaded file>")
            sys.exit(2)
        
        ## Save the sys arguments as variables
        configType = sys.argv[1]
        MicrosoftUserName = sys.argv[2]
        url = sys.argv[3]
        filePath = sys.argv[4]
        action = sys.argv[5]
        fileName = sys.argv[6] if len(sys.argv) == 7 else None
    
        ## Create a oneDriveAndSharepointApi object
        oneDriveAndSharepointApi = CoreMicrosoftAPI(graphConfigType = configType, microsoftUserName = MicrosoftUserName)

        ## If the action is download
        if action.lower() == 'download':

            ## Download the file using the async method
            filePath = asyncio.run(oneDriveAndSharepointApi.downloadSharedMicrosoftFileAsync(url, filePath, fileName))
    
            ## logger.info and return the file path
            logger.info (filePath)
            print (filePath)
            return filePath

        ## If the action is upload
        elif action.lower() == 'upload':
            
            ## Upload the file using the async method
                uploadResult = asyncio.run(oneDriveAndSharepointApi.uploadSharedMicrosoftFileAsync(url, filePath))
        
                ## logger.info and return the upload result
                logger.info (uploadResult)
                print (uploadResult)
                return uploadResult

## This function can take all of the necessary variables for sending outlook emails and runs the relavent function within a subprocess of this script
def sendOutlookEmail (p1_microsoftUserName = "lmsservice@nnu.edu", p1_subject = None, p1_body = None, p1_recipientEmailList = None, p1_shared_mailbox = "" ):
    
    import subprocess        
    
    ## Run this python script file as a subprocess with all of the neccessary microsoft object and emailer variables
    subprocessResult = subprocess.run([
                                    "python"
                                    , os.path.abspath(__file__)
                                    , "Outlook"
                                    , p1_microsoftUserName
                                    , p1_subject
                                    , p1_body
                                    , p1_recipientEmailList
                                    , p1_shared_mailbox
                                    ]
                                    , capture_output = True
                                    , text = True
                                    )
    
    ## Return the subprocess result
    return subprocessResult

## This function can take all of the necessary variables for downloading a shared microsoft file and runs the relavent function within a subprocess of this script
def downloadSharedMicrosoftFile (p1_microsoftUserName = "lmsservice@nnu.edu", p1_fileShareUrl = None, p1_downloadSavePath = None, p1_fileName = ""):
    
    import subprocess, os

    ## If the filepath doesn't exist, make it
    if not os.path.exists(p1_downloadSavePath): os.makedirs(p1_downloadSavePath)
    
    ## Run this python script file as a subprocess with all of the neccessary microsoft object and emailer variables
    subprocessResult = subprocess.run([
                                    "python"
                                    , os.path.abspath(__file__)
                                    , "OneDrive"
                                    , p1_microsoftUserName
                                    , p1_fileShareUrl
                                    , p1_downloadSavePath
                                    , "Download"
                                    , p1_fileName
                                    ]
                                    , capture_output = True
                                    , text = True
                                    )

    ## Return the subprocess result (replace the newline character at the end of the resulting filepath)
    return subprocessResult.stdout.replace("\n", "")

## This function can take all of the necessary variables for uploading a shared microsoft file and runs the relavent function within a subprocess of this script
def uploadSharedMicrosoftFile (p1_microsoftUserName = "lmsservice@nnu.edu", p1_uploadLocationUrl = None, p1_uploadFileLocation = None):

    import subprocess

    ## Run this python script file as a subprocess with all of the neccessary microsoft object and emailer variables
    subprocessResult = subprocess.run([
                                    "python"
                                    , os.path.abspath(__file__)
                                    , "OneDrive"
                                    , p1_microsoftUserName
                                    , p1_uploadLocationUrl
                                    , p1_uploadFileLocation
                                    , "Upload"
                                    ]
                                    , capture_output = True
                                    , text = True
                                    )

    ## Return the subprocess result
    return subprocessResult

## If the script is main
if __name__ == "__main__":

    ## If there are sys arguments
    if len(sys.argv) > 1:

        ## Run the subprocess function
        runAsASubprocess()

    else:

        ## Test the send mail and upload file functions and create the relavent keys
        sendOutlookEmail(p1_subject = 'Testing Microsoft Graph New!'
                         , p1_body = 'Hello world!!!!'
                         , p1_recipientEmailList = 'tlc@nnu.edu'
                         , p1_shared_mailbox = "tlc@nnu.edu"
                         )
        
        # Define the sharepoint file link
        geLink = 'https://nnuedu.sharepoint.com/:x:/s/prod-GeneralEducationCouncil/EUhnLkj26C9NvjIjTnNp72MB-nLQ1L5wIHr7AMXW1pvzvQ?e=QXffRU'
        geSavePath = f"C:\\NNU Code\\Python Scripts\\Microsoft Resources\\"

        downloadSharedMicrosoftFile(p1_fileShareUrl = geLink
                                  , p1_downloadSavePath = geSavePath
                                  )

        ## Create a outlookApi object
        ## Create a outlookApi object
        #outlookApi = CoreMicrosoftAPI(graphConfigType = "Outlook", microsoftUserName = 'lmsservice@nnu.edu')

        # Create a oneDriveAndSharepointApi object
        #oneDriveAndSharepointApi = CoreMicrosoftAPI(graphConfigType = "Onedrive", microsoftUserName = 'lmsservice@nnu.edu')
        
        # uploadSharedMicrosoftFile(p1_microsoftUserName = "lmsservice@nnu.edu"
        #                                  , p1_uploadLocationUrl = "https://nnuedu.sharepoint.com/:f:/s/InstitutionalEffectiveness/Et4ZOpcNVDdPtMY4MRaPdaYBt1pXLaymyI1Z7NVfF5s8nw"
        #                                  , p1_uploadFileLocation = r'C:\\NNU Code\\Python Scripts\\Canvas Resources\\2024-25\\FA24\\GE Outcome_Course Alignment Worksheet.xlsx'
        #                                  )
        
        #asyncioResult = asyncio.run(oneDriveAndSharepointApi.uploadSharedMicrosoftFileAsync("https://nnuedu.sharepoint.com/:f:/s/InstitutionalEffectiveness/Et4ZOpcNVDdPtMY4MRaPdaYBt1pXLaymyI1Z7NVfF5s8nw", r'C:\\NNU Code\\Python Scripts\\Canvas Resources\\2024-25\\FA24\\GE Outcome_Course Alignment Worksheet.xlsx'))
    
        # #asyncio.run(outlookApi.send_mail_async(subject = 'Testing Microsoft Graph New!', body = 'Hello world!!!!', recipientEmailList = ['brycezmiller@nnu.edu'], shared_mailbox = "instructionaldesign@nnu.edu"))
        # asyncio.run(outlookApi.send_mail_async(subject = "Course SENIOR SEMINAR SP2024_ENGL4980_01 GE Outcomes: Course Start"
        #                      , body = '\n<!DOCTYPE html>\n<html>\n<head>\n    <style>\n        td {\n            padding: 10px;\n        }\n\n        .bold-text {\n            font-weight: bold;\n        }\n    </style>\n</head>\n<body>\n    <p>Greetings!</p>\n\n    <p>You are receiving this email because you are scheduled to teach the Gen Ed course, SENIOR SEMINAR SP2024_ENGL4980_01, which is associated with the GE_HU2_U2024 Gen Ed outcome in the SP24 semester. You will find the language for the associated outcome in the list linked below.</p>\n\n    <p>Please ensure the following steps are completed for your GE course:</p>\n\n    <ol>\n        <li>Identify which outcome(s) should be assessed in your course. Outcomes are designated by two letters and one number (i.e. HU1). I have highlighted one of my courses below as an example.</li>\n        <li><a href="https://library.nnu.edu/general-education/outcomes-and-rubrics">Follow this link to our General Education Guide to identify your outcome language.</a>\n        <br>Here is the outcome language for my highlighted course:\n            <div style="border-collapse: collapse; border-spacing: 0; max-width: 70%; margin-bottom: 20px; border: 1px solid rgb(221, 221, 221); color: rgb(51, 51, 51); font-family: Arial, Helvetica, Verdana; font-size: 12px; overflow: hidden; display: flex;">\n                <div style="box-sizing: border-box; padding: 8px; height: 100%; line-height: 1.42857; vertical-align: middle; position: relative; border: none; min-width: 50px;">\n                    <span dir="ltr" style="box-sizing: border-box; margin: 0; font-weight: bold;">HU1:</span>\n                </div>\n                <div style="box-sizing: border-box; padding: 8px; line-height: 1.42857; vertical-align: top; position: relative; border: none; flex-grow: 1;">\n                    <span dir="ltr" style="box-sizing: border-box; margin: 0; padding-left: 5px;">Students will understand & appreciate visual, musical, and literary art based on the historical, political, and socio-cultural contexts in which they emerged.&nbsp;</span>\n                </div>\n            </div>\n        </li>\n        <li>Copy the outcome language.</li>\n        <li>Ensure the outcome language is in your course syllabus. Here is the GE syllabus statement from my course as an example:\n            <br><br>\n            <div style="margin-left: 20px; font-style: italic;">\n                General Education Outcomes\n                <br>Humanities\n                <br><span style="font-weight: bold;">HU1, Transformation</span> - Students will understand & appreciate literary artworks based on the historical,\n                <br>political, and socio-cultural contexts in which they emerged.\n            </div>\n        </li>\n    </ol>\n\n    <p>That\'s all for now. Please let us know if you have any questions.</p>\n\n    <p>Sincerely, \n    <br> The General Education Council</p>\n    <span style="font-weight: bold;">Catherine Becker, Ph.D.</span>\n    <br>General Education Council Chair\n    <br>Associate Professor of English\n    <br>Northwest Nazarene University\n    \n</body>\n</html>\n'
        #                      , recipientEmailList = "brycezmiller@nnu.edu, "))
        # logger.info('Mail sent.\n')

        # geLink = 'https://nnuedu.sharepoint.com/:x:/s/prod-GeneralEducationCouncil/EUhnLkj26C9NvjIjTnNp72MB-nLQ1L5wIHr7AMXW1pvzvQ?e=QXffRU'
        # geSavePath = f"C:\\NNU Code\\Python Scripts\\Canvas Resources\\\\2023-24\\SG24\\"

        # asyncioResult = asyncio.run(outlookApi.downloadSharedMicrosoftFileAsync(geLink, geSavePath, 'SG24_GE Outcome_Course Alignment Worksheet.xlsx'))
        # logger.info ('File retrieved')

        # #outlookApi.send_mail(subject = 'Testing Microsoft Graph New!', body = 'Hello world!!!!', recipient = input("Enter the email addres that will recieve the email (i.e. example@nnu.edu): "), shared_mailbox = "instructionaldesign@nnu.edu")
        # logger.info('Mail sent.\n')