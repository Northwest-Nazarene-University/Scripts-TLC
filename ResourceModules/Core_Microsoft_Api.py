## Define the encoding
## -*- coding: utf-8 -*-

##Author: Bryce Miller - brycezmiller@nnu.edu
##Last Updated by: Bryce Miller. On: 2/9/2024

## General Imports
import os, sys, base64, sys, asyncio, configparser, getpass, requests, mimetypes, re
from datetime import datetime
from cryptography.fernet import Fernet


## Microsoft Imports
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

## Adjust system path to include ResourceModules
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

try: ## If the module is run directly
    from TLC_Common import (downloadFile, getEncryptionKey)
    from Local_Setup import LocalSetup
except ImportError: ## Otherwise as a relative import if the module is imported
    from .TLC_Common import (downloadFile, getEncryptionKey)
    from .Local_Setup import LocalSetup

## import Config Variables
from Common_Configs import serviceEmailAccount

## Define the script name, purpose, and external requirements for logging and error reporting purposes
__scriptName = os.path.basename(__file__).replace(".py", "")
scriptPurpose = r"""
This class contains the base methods and variables to send emails with the Outlook API
"""
externalRequirements = r"""
Access to an appropriet "Credentials.json" file. See https://developers.google.com/outlook/api/auth/web-server \
for further details on the outlook api authorization requirements.
"""


class CoreMicrosoftAPI:
    settings: SectionProxy
    device_code_credential = None
    userClient: GraphServiceClient
    storedGraphConfig = None
    storedMicrosoftUsername = None

    def __init__(self, localSetup, graphConfigType, microsoftUserName):
        self.localSetup = localSetup
        self.storedGraphConfig = graphConfigType
        self.storedMicrosoftUsername = microsoftUserName

        ## Paths and localSetup.logger from LocalSetup
        configPath = self.localSetup.configPath
        microsoftResourcePath = self.localSetup.getInternalResourcePaths("Microsoft")
        logger = self.localSetup.logger

        ## Determine config file name
        if graphConfigType.lower() == 'outlook':
            configFileName = "Outlook_API_Config"
        elif graphConfigType.lower() == 'onedrive':
            configFileName = "OneDrive_and_Sharepoint_API_Config"
        else:
            raise ValueError("Invalid graphConfigType. Must be 'Outlook' or 'OneDrive'.")

        ## Load config settings
        config = configparser.ConfigParser()
        config.read([os.path.join(configPath, f"{configFileName}.cfg"), 'config.dev.cfg'])
        self.settings = config['azure']

        client_id = self.settings['clientId']
        tenant_id = self.settings['tenantId']
        graph_scopes = self.settings['graphUserScopes'].split(' ')

        ## Encryption key
        encryptionKey = getEncryptionKey(self.localSetup)
        fernet = Fernet(encryptionKey)

        ## Authentication logic using microsoftResourcePath
        pythonUser = getpass.getuser()
        credentialFile = os.path.join(microsoftResourcePath,
                                      f"{pythonUser}_Owned_{configFileName}_Authenticated_{microsoftUserName}_Credential.json")

        if os.path.exists(credentialFile):
            with open(credentialFile, "r") as json_file:
                encryptedContent = json_file.read()
                decryptedContent = fernet.decrypt(encryptedContent.encode()).decode()
                deserialized_record = AuthenticationRecord.deserialize(decryptedContent)
                self.device_code_credential = InteractiveBrowserCredential(
                    cache_persistence_options=TokenCachePersistenceOptions(),
                    authentication_record=deserialized_record
                )
                self.device_code_credential.authenticate(scopes=graph_scopes)
        else:
            self.device_code_credential = InteractiveBrowserCredential(
                client_id=client_id,
                tenant_id=tenant_id,
                cache_persistence_options=TokenCachePersistenceOptions()
            )
            authenticatedDeviceCodeCredential = self.device_code_credential.authenticate(scopes=graph_scopes)
            encryptedData = fernet.encrypt(authenticatedDeviceCodeCredential.serialize().encode())
            with open(credentialFile, "w") as json_file:
                json_file.write(encryptedData.decode())
                
        self.userClient = GraphServiceClient(self.device_code_credential, graph_scopes)

    ## This function sends an email asynchroniously with the option of using a shared mailbox 
    async def send_mail_async(self, subject: str, body: str, recipientEmailList: list, shared_mailbox: str = None, attempt = 1):
        
        ## Create the message
        message = Message()
        
        ## Set the message subject and create the message body
        message.subject = subject
        message.body = ItemBody()

        ## Set the message body type and content
        message.body.content_type = BodyType.Html if "!DOCTYPE html" in body else BodyType.Text
        message.body.content = body

        ## Create a list of intended recipients
        message.to_recipients = []

        ## The code takes email addresses as a single string with email addresses seperated by commas
        ## so split the email addresses into a list and remove any spaces
        recipientEmailList = [email.strip() for email in recipientEmailList.split(',')]
        
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
        await self.userClient.me.send_mail.post(request_body)

    ## This function downloads a microsoft file asynchroniously using a shared link
    async def downloadSharedMicrosoftFileAsync(self, localSetup, fileShareUrl, downloadSavePath, fileName, attempt = 1):

        ## Change the url to base64
        base64FileShareUrl = base64.urlsafe_b64encode(fileShareUrl.encode('utf-8')).decode('utf-8').rstrip('=').replace('/', '_').replace('+', '-')

        ## Change the base64 url to an encoded url
        encodedFileShareUrl = f"u!{base64FileShareUrl}"

        ## Define a variable to hold the api call result
        fileResult = await self.userClient.shares.by_shared_drive_item_id(encodedFileShareUrl).drive_item.get()
        
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
        downloadFile(localSetup, fileDownloadUrl, f"{absoluteDownloadSavePath}\\{fileName}", "w")
        
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
        requestResult = await self.userClient.shares.by_shared_drive_item_id(p1_encodedFileShareUrl).drive_item.get()

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
                searchResult = await self.userClient.drives.by_drive_id(requestResult.parent_reference.drive_id).items.by_drive_item_id(requestResult.id).search_with_q(fileName).get()
                
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
        ##uploadItem = DriveItem(name = fileBaseName, file = File(mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"))
        uploadItem = DriveItem(name = fileBaseName, file = File())

        ## Change the url to base64
        base64TargetShareUrl = base64.urlsafe_b64encode(targetShareUrl.encode('utf-8')).decode('utf-8').rstrip('=').replace('/', '_').replace('+', '-')
        
        ## Change the base64 url to an encoded url
        encodedTargetShareUrl = f"u!{base64TargetShareUrl}"
        
        ## Determine if the URL is a folder or a file
        targetUrlIsAFolder, targetUrlDriveItemId, targetUrlDriveId, targetFileDriveItemId, targetFileDriveId = await self\
            .findSharedMicrosoftFileAsync(encodedTargetShareUrl, fileBaseName)

        ## Prepare upload body
        uploadBody = CreateUploadSessionPostRequestBody.create_from_discriminator_value(uploadItem)
        uploadBody.item = DriveItemUploadableProperties(additional_data={'@microsoft.graph.conflictBehavior': 'replace'})

        ## Create the blank variable for uploadSessionBuilder
        uploadSessionBuilder = None

        ## Decide how to upload
        if targetFileDriveId and targetFileDriveItemId:
            ## URL is a file or file exists in folder ? update it
            self.localSetup.logger.info(f"Updating existing file: {fileBaseName}")
            uploadSessionBuilder = self.userClient.drives.by_drive_id(targetFileDriveId).items.by_drive_item_id(targetFileDriveItemId).create_upload_session
        elif targetUrlIsAFolder:
            ## URL is a folder and file doesn't exist ? create it
            self.localSetup.logger.info(f"Creating new file: {fileBaseName}")
            createNewDriveItemResult = await self.userClient.drives.by_drive_id(targetUrlDriveId).items.by_drive_item_id(targetUrlDriveItemId).children.post(uploadItem)
            uploadSessionBuilder = self.userClient.drives.by_drive_id(targetUrlDriveId).items.by_drive_item_id(createNewDriveItemResult.id).create_upload_session

        else:
            ## URL is a file but doesn't match the target file name ? fallback
            raise ValueError("Unable to determine target location for upload.")
            
                                
        ## Create the upload session
        uploadSession = await uploadSessionBuilder.post(uploadBody)

        ## Get the upload url
        uploadURL = uploadSession.upload_url

        ## Open the file
        with open (uploadItemFilePath, 'rb') as fileContent:

            ## Get the byte size of the file
            fileByteSize = os.path.getsize(uploadItemFilePath)
            
            ## Define the max chunk size for file uploads
            maxChunkSize = 320 * 1024
                            
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
                                    
                # self.localSetup.logger.info the number of chunks remaining
                ## self.localSetup.logger.info (f"File Segments Remaining: {numberOfFileChunks - fileChunkCounter}")

            self.localSetup.logger.info (f"{fileBaseName} uploaded")

## This function us for when the script is called as a subprocess
def runAsASubprocess ():

    ## Define localSetup
    localSetup = LocalSetup(datetime.now(), __file__)
    
    ## Aceptable Method Arguments
    acceptableMethods = ['outlook', 'onedrive']

    localSetup.logger.info(sys.argv)
    
    ## Check if at least 3 sys arguments are passed and if the first sys argument is either outlook or onedrive and if the second sys argument is an email address
    if not len(sys.argv) >= 3 or (sys.argv[1].lower() not in acceptableMethods) or ('@' not in sys.argv[2]):
        
        ## self.localSetup.logger.info the usage of the general script
        localSetup.logger.info("Usage: graphConfigType: <Outlook or OneDrive>, microsoftUsername: <MicrosoftEmailAddress>")
        sys.exit(2)
        
    ## If the first sys argu passed is 'Outlook'
    if sys.argv[1].lower() == 'outlook':  
        
        ## Check if at least 6 sys arguments are passed
        if not len(sys.argv) >= 6:

            ## self.localSetup.logger.info the usage of the outlook portion of the script
            localSetup.logger.info("Usage: graphConfigType: <Outlook or OneDrive>, microsoftUsername: <MicrosoftEmailAddress>, subject: <EmailSubject>, body: <EmailBody>, recipientEmailList: <RecipientEmailList>, optional shared_mailbox: <SharedMailboxAddress>")
            sys.exit(2)   

        ## Save the sys arguments as variables
        configType = sys.argv[1]
        MicrosoftUserName = sys.argv[2]
        subject = sys.argv[3]
        body = sys.argv[4]
        recipientEmailList = sys.argv[5]
        shared_mailbox = sys.argv[6]

        ## Create a outlookApi object
        outlookApi = CoreMicrosoftAPI(localSetup, graphConfigType = configType, microsoftUserName = MicrosoftUserName)
        
        ## Send the email using the async method
        emailerResult = asyncio.run(outlookApi.send_mail_async(subject, body, recipientEmailList, shared_mailbox))
        
        ## self.localSetup.logger.info and return the emailer result
        localSetup.logger.info (emailerResult)
        return emailerResult

    ## If the first sys argu passed is 'Onedrive'
    elif sys.argv[1].lower() == 'onedrive':    

        ## Check if at least 6 sys arguments are passed
        if not len(sys.argv) >= 6:

            ## self.localSetup.logger.info the usage of the onedrive portion of the script
            localSetup.logger.info("Usage: graphConfigType: <Outlook or OneDrive>, microsoftUsername: <MicrosoftEmailAddress>, action: <'download' or 'upload'>, url: <MicrosoftSharedFileUrl>, filePath: <Path for file to be downloaded to or uploaded from>, optional fileName: <Final Name and exstension of the downloaded file>")
            sys.exit(2)
        
        ## Save the sys arguments as variables
        configType = sys.argv[1]
        MicrosoftUserName = sys.argv[2]
        action = sys.argv[3]
        url = sys.argv[4]
        filePath = sys.argv[5]
        fileName = sys.argv[6]
    
        ## Create a oneDriveAndSharepointApi object
        oneDriveAndSharepointApi = CoreMicrosoftAPI(localSetup, graphConfigType = configType, microsoftUserName = MicrosoftUserName)

        ## If the action is download
        if action.lower() == 'download':

            ## Download the file using the async method
            downloadResult = asyncio.run(oneDriveAndSharepointApi.downloadSharedMicrosoftFileAsync(localSetup, url, filePath, fileName))
    
            ## self.localSetup.logger.info and return the file path
            localSetup.logger.info (downloadResult)
            return downloadResult

        ## If the action is upload
        elif action.lower() == 'upload':
            
            ## Upload the file using the async method
                uploadResult = asyncio.run(oneDriveAndSharepointApi.uploadSharedMicrosoftFileAsync(url, filePath))
        
                ## self.localSetup.logger.info and return the upload result
                localSetup.logger.info (uploadResult)
                return uploadResult

## This function can take all of the necessary variables for sending outlook emails and runs the relavent function within a subprocess of this script
def sendOutlookEmail (p1_microsoftUserName = serviceEmailAccount, p1_subject = "", p1_body = "", p1_recipientEmailList = "", p1_shared_mailbox = "" ):
    
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
def downloadSharedMicrosoftFile (p1_microsoftUserName = serviceEmailAccount, p1_microsoftShareUrl = None, p1_downloadSavePath = None, p1_fileName = ""):
    
    import subprocess
    
    ## Run this python script file as a subprocess with all of the neccessary microsoft object and emailer variables
    subprocessResult = subprocess.run([
                                    "python"
                                    , os.path.abspath(__file__)
                                    , "OneDrive"
                                    , p1_microsoftUserName
                                    , "Download"
                                    , p1_microsoftShareUrl
                                    , p1_downloadSavePath
                                    , p1_fileName
                                    ]
                                    , capture_output = True
                                    , text = True
                                    )

    likelyPath = subprocessResult.stderr[subprocessResult.stderr.rfind("INFO"):].replace("INFO ","",1).replace(r"\n","")
    downloadedFilePath = likelyPath if likelyPath and os.path.exists(likelyPath) else p1_downloadSavePath

    ## Return the subprocess result
    return downloadedFilePath

## This function can take all of the necessary variables for uploading a shared microsoft file and runs the relavent function within a subprocess of this script
def uploadSharedMicrosoftFile (p1_microsoftUserName = serviceEmailAccount, p1_microsoftShareUrl = "", p1_uploadItemFilePath = ""):

    import subprocess

    ## Run this python script file as a subprocess with all of the neccessary microsoft object and emailer variables
    subprocessResult = subprocess.run([
                                    "python"
                                    , os.path.abspath(__file__)
                                    , "OneDrive"
                                    , p1_microsoftUserName
                                    , "Upload"
                                    , p1_microsoftShareUrl
                                    , p1_uploadItemFilePath
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

        ## Initialize LocalSetup
        localSetup = LocalSetup(datetime.now(), __file__)
        
        ## Configure outlook and sharepoint credentials ##

        ## Outlook
        sendOutlookEmail(p1_subject = 'Associated Course Outcomes: Course Start Information'
                         , p1_body = """test"""
                         , p1_recipientEmailList = 'brycezmiller@nnu.edu'
                         , p1_shared_mailbox = "gradedu@nnu.edu"
                         )

        ## Sharepoint
        tlcDownloadUrlPath = r'https://nnuedu.sharepoint.com/:w:/s/prod-InstructionalDesignTechnologyIDT/EQAniDUXoKNPmg27srCn-ZYBNLuk9XceTvghDyhLr708KQ?e=MfU8Ci'
        downloadSharedMicrosoftFile(p1_microsoftShareUrl = tlcDownloadUrlPath
                                    , p1_downloadSavePath = localSetup.configPath
                                  )

        ##  Direct Async test run (comment out the configure above)
        #outlookApi = CoreMicrosoftAPI(localSetup, graphConfigType = "Outlook", microsoftUserName = serviceEmailAccount)
        ## oneDriveAndSharepointApi = CoreMicrosoftAPI(localSetup, graphConfigType = "Onedrive", microsoftUserName = serviceEmailAccount)
        
        ## tlcUploadUrlPath = r'https://nnuedu.sharepoint.com/sites/prod-InstructionalDesignTechnologyIDT/Shared%20Documents'
        ## uploadfilepath = os.path.join(localSetup.configPath, "Test Doc.docx")
        ## downloadfilepath = localSetup.configPath
 
        ## Email Send test, file upload test, file download test ##
        ## MAKE SURE TO ONLY RUN ONE AT A TIME (the second one will always error out due to the event loop already running)

        #asyncioResult = asyncio.run(outlookApi.send_mail_async(subject = 'Testing Microsoft Graph New!', body = 'Hello world!!!!', recipientEmailList = 'brycezmiller@nnu.edu', shared_mailbox = "tlc@nnu.edu"))
        ##asyncioResult = asyncio.run(oneDriveAndSharepointApi.uploadSharedMicrosoftFileAsync(tlcUploadUrlPath, uploadfilepath))
        ##asyncioResult = asyncio.run(oneDriveAndSharepointApi.downloadSharedMicrosoftFileAsync(tlcDownloadUrlPath, downloadfilepath, 'Test Doc'))

        ######

        ## Create a outlookApi object
        ## Create a outlookApi object
        ## outlookApi = CoreMicrosoftAPI(graphConfigType = "Outlook", microsoftUserName = serviceEmailAccount)
        
        ## Create a oneDriveAndSharepointApi object
        ## oneDriveAndSharepointApi = CoreMicrosoftAPI(graphConfigType = "Onedrive", microsoftUserName = serviceEmailAccount)
    
        ## asyncio.run(outlookApi.send_mail_async(subject = 'Testing Microsoft Graph New!', body = 'Hello world!!!!', recipientEmailList = 'brycezmiller@nnu.edu', shared_mailbox = "ie@nnu.edu"))
        ## asyncio.run(outlookApi.send_mail_async(subject = "prime test"
        ##                      , body = "hey"
        ##                      , recipientEmailList = "tlc@nnu.edu, brycezmiller@nnu.edu"
        ##                      , shared_mailbox = "instructionaldesign@nnu.edu"))
        ## self.localSetup.logger.info('Mail sent.\n')

        ## studentCohortProfileLink = ''

        ## asyncioResult = asyncio.run(oneDriveAndSharepointApi.downloadSharedMicrosoftFileAsync(fileShareUrl = studentCohortProfileLink, downloadSavePath = departmentalDataResroucesPath))
        ## self.localSetup.logger.info ('File retrieved')
    
        
        ## asyncioResult = asyncio.run(oneDriveAndSharepointApi.uploadSharedMicrosoftFileAsync(fileLink, fr"{departmentalDataResroucesPath}Test Doc.docx"))
    
        ## upload a test file
        ## asyncio .run(oneDriveAndSharepointApi.upload_file_to_sharepoint_async(site_id="InstitutionalEffectiveness", folder_id="01KZJ3QKZ3F5JZ2W3L2Z6WZ7V7Z3Z5)

        ## oneDriveAndSharepointApi.upload_microsoft_file(r"Admissions Data.xlsx", 'EpoNInDhwb9OvsaU-R3lEtEB-wFaU9qPns8huA70F0TnLA?e=Kciv0X')

        ## outlookApi.send_mail(subject = 'Testing Microsoft Graph New!', body = 'Hello world!!!!', recipient = input("Enter the email addres that will recieve the email (i.e. example@nnu.edu): "), shared_mailbox = "instructionaldesign@nnu.edu")
        ## self.localSetup.logger.info('Mail sent.\n')