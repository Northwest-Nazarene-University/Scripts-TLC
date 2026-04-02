## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

## Import Generic Modules
import os, sys, re, time, math, pandas as pd, paramiko
from datetime import datetime, timedelta
from dateutil import parser

try: ## If the module is run directly
    from Local_Setup import LocalSetup
    from TLC_Common import getEncryptionKey, makeApiCall, flattenApiObjectToJsonList
    from Canvas_Report import CanvasReport
except ImportError: ## Otherwise as a relative import if the module is imported
    from .Local_Setup import LocalSetup
    from .TLC_Common import getEncryptionKey, makeApiCall, flattenApiObjectToJsonList
    from .Canvas_Report import CanvasReport

## Add the config path
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "Configs"))

from Common_Configs import catalogToSimpleSyllabusConfig, coreCanvasApiUrl

## Cryptography import for Fernet encryption (same pattern as Core_Microsoft_Api.py)
from cryptography.fernet import Fernet

## Define the script name, purpose, and external requirements for logging and error reporting purposes
__scriptName = os.path.basename(__file__).replace(".py", "")

scriptPurpose = r"""
Provide common action-level helper functions for use across TLC action scripts.
Currently houses Simple Syllabus SFTP upload utilities, CSV sanitization helpers,
and the SFTP private key password management functions so that any script sending
data to Simple Syllabus can reuse them without duplication.
"""

externalRequirements = r"""
To be located within the ResourceModules folder alongside TLC_Common.py, Local_Setup.py, etc.
Requires access to the config path for SSH key files and encrypted password storage.
"""


## ══════════════════════════════════════════════════════════════════════════════
## CSV Helpers
## ══════════════════════════════════════════════════════════════════════════════

## Helper function to read CSV with encoding fallback
def readCsvWithEncoding(filePath: str, **kwargs) -> pd.DataFrame:
    """Read a CSV file trying utf-8-sig first, then latin-1 as fallback."""
    try:
        return pd.read_csv(filePath, encoding='utf-8-sig', **kwargs)
    except (UnicodeDecodeError, UnicodeError):
        return pd.read_csv(filePath, encoding='latin-1', **kwargs)


## This function sanitizes CSV headers per Simple Syllabus EXT-CSV requirements
def sanitizeCsvHeaders(p1_filePath: str, p1_localSetup: LocalSetup):
    """
    Reads a CSV, sanitizes its headers per Simple Syllabus EXT-CSV requirements,
    and re-saves it in place with UTF-8 encoding (no BOM).

    Acceptable header characters: A-Z, a-z, 0-9, underscores, forward slashes, dashes, spaces.
    Unacceptable: special characters (@#$%&* etc), parentheses, colons, leading/trailing spaces, blank headers.

    Args:
        p1_filePath (str): Path to the CSV file to sanitize in place.
        p1_localSetup (LocalSetup): LocalSetup instance for logging.
    """
    functionName = "sanitizeCsvHeaders"

    sanitizedDf = readCsvWithEncoding(p1_filePath)

    sanitizedHeaders = []
    for header in sanitizedDf.columns:
        ## Strip leading/trailing whitespace
        cleaned = str(header).strip()
        ## Replace unacceptable characters — keep alphanumeric, underscores, forward slashes, dashes, spaces
        cleaned = re.sub(r'[^A-Za-z0-9_/\- ]', '', cleaned)
        ## Collapse multiple spaces into one
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        ## If header is blank after cleaning, assign a placeholder
        if not cleaned:
            cleaned = f"Column_{len(sanitizedHeaders) + 1}"
        sanitizedHeaders.append(cleaned)

    sanitizedDf.columns = sanitizedHeaders

    ## Re-save with sanitized headers and UTF-8 encoding (no BOM)
    sanitizedDf.to_csv(p1_filePath, index=False, encoding='utf-8')
    p1_localSetup.logger.info(f"{functionName}: Sanitized CSV headers and re-saved to {p1_filePath}")


## ══════════════════════════════════════════════════════════════════════════════
## SFTP Private Key Password Management
## ══════════════════════════════════════════════════════════════════════════════

## This function retrieves the Simple Syllabus SFTP private key password.
## On first run it reads the plaintext password from SSPrivKP.txt, encrypts it
## using the repository's Fernet encryption setup (same pattern as Core_Microsoft_Api.py),
## saves the encrypted version as SSPrivKP_Encrypted.txt, and deletes the plaintext file.
## On subsequent runs it reads and decrypts the encrypted file.
def getSimpleSyllabusPrivateKeyPassword(p1_localSetup: LocalSetup):
    """
    Retrieve the SSH private key password for Simple Syllabus SFTP.
    Returns None if the key has no passphrase (neither password file exists).

    Args:
        p1_localSetup (LocalSetup): LocalSetup instance for config path and logging.

    Returns:
        str or None: The decrypted passphrase, or None if no passphrase is needed.
    """
    functionName = "getSimpleSyllabusPrivateKeyPassword"

    plaintextPasswordPath = os.path.join(p1_localSetup.configPath, "SSPrivKP.txt")
    encryptedPasswordPath = os.path.join(p1_localSetup.configPath, "SSPrivKP_Encrypted.txt")

    ## Case 1: Encrypted file exists — decrypt and return
    if os.path.exists(encryptedPasswordPath):
        encryptionKey = getEncryptionKey(p1_localSetup)
        fernet = Fernet(encryptionKey)
        p1_localSetup.logger.info(f"{functionName}: Reading encrypted private key password from {encryptedPasswordPath}")
        with open(encryptedPasswordPath, "r") as encFile:
            encryptedContent = encFile.read().strip()
        decryptedPassword = fernet.decrypt(encryptedContent.encode()).decode()
        return decryptedPassword if decryptedPassword else None

    ## Case 2: Plaintext file exists — encrypt, save, delete plaintext, return password
    if os.path.exists(plaintextPasswordPath):
        encryptionKey = getEncryptionKey(p1_localSetup)
        fernet = Fernet(encryptionKey)
        p1_localSetup.logger.info(f"{functionName}: Found plaintext password at {plaintextPasswordPath}. Encrypting...")
        with open(plaintextPasswordPath, "r") as ptFile:
            plaintextPassword = ptFile.read().strip()
        if not plaintextPassword:
            ## Empty file means no passphrase — delete and treat as no password
            os.remove(plaintextPasswordPath)
            p1_localSetup.logger.info(f"{functionName}: Plaintext password file was empty; treating as no passphrase")
            return None
        encryptedData = fernet.encrypt(plaintextPassword.encode())
        with open(encryptedPasswordPath, "w") as encFile:
            encFile.write(encryptedData.decode())
        os.remove(plaintextPasswordPath)
        p1_localSetup.logger.info(f"{functionName}: Encrypted and deleted plaintext password file")
        return plaintextPassword

    ## Case 3: Neither file exists — key has no passphrase
    p1_localSetup.logger.info(f"{functionName}: No password file found; assuming key has no passphrase")
    return None


## ══════════════════════════════════════════════════════════════════════════════
## SFTP Upload Function
## ══════════════════════════════════════════════════════════════════════════════

## This function uploads a CSV file to Simple Syllabus via SFTP
def uploadToSimpleSyllabus(p1_filePath: str, p1_localSetup: LocalSetup, p1_errorHandler=None, p1_writeSuccessTag: bool = True):
    """
    Uploads a CSV file to the Simple Syllabus SFTP server.

    The function:
        1. Validates the local file exists.
        2. Sanitizes CSV headers per Simple Syllabus EXT-CSV requirements.
        3. Retrieves the SSH private key password (encrypted via Fernet).
        4. Connects to the Simple Syllabus SFTP server using SSH private key authentication.
        5. Uploads the file to the configured remote /imports directory.
        6. Closes the SFTP and SSH connections.
        7. Optionally writes a success tag file next to the uploaded CSV
           so that subsequent runs can detect whether anything has changed since last upload.

    Args:
        p1_filePath (str): The local file path to the CSV to upload.
        p1_localSetup (LocalSetup): LocalSetup instance for config path and logging.
        p1_errorHandler: Optional errorEmail instance for sending error notifications on connection failure.
        p1_writeSuccessTag (bool): If True, writes a success tag file next to the uploaded file.

    Raises:
        FileNotFoundError: If the local file or SSH key does not exist.
        Exception: If the SFTP connection or upload fails after all retries.
    """

    functionName = "uploadToSimpleSyllabus"

    try:
        p1_localSetup.logger.info(f"{functionName}: Starting upload of {p1_filePath} to Simple Syllabus SFTP")

        ## ── Validate the local file ──
        if not os.path.exists(p1_filePath):
            raise FileNotFoundError(f"{functionName}: File not found: {p1_filePath}")

        ## ── Sanitize CSV headers ──
        sanitizeCsvHeaders(p1_filePath, p1_localSetup)

        ## ── Retrieve SFTP configuration from catalogToSimpleSyllabusConfig ──
        sftpConfig = catalogToSimpleSyllabusConfig.get("sftp", {})
        if not sftpConfig:
            raise ValueError(f"{functionName}: 'sftp' configuration missing from catalogToSimpleSyllabusConfig")

        sftpHost = sftpConfig.get("host")
        sftpPort = sftpConfig.get("port", 22)
        sftpUsername = sftpConfig.get("username")
        sftpRemoteDir = sftpConfig.get("remote_dir", "/imports")

        if not sftpHost or not sftpUsername:
            raise ValueError(f"{functionName}: SFTP host or username missing from configuration")

        ## ── Locate the SSH private key in the config path ──
        privateKeyPath = os.path.join(p1_localSetup.configPath, "SimpSylSSH.txt")
        if not os.path.exists(privateKeyPath):
            raise FileNotFoundError(
                f"{functionName}: SSH private key not found at {privateKeyPath}. "
                f"Please place 'SimpSylSSH.txt' in the config directory."
            )

        p1_localSetup.logger.info(f"{functionName}: Using SSH private key at {privateKeyPath}")

        ## ── Retrieve the private key password (None if no passphrase) ──
        privateKeyPassword = getSimpleSyllabusPrivateKeyPassword(p1_localSetup)

        ## ── Normalize password: encode str -> bytes for paramiko 4.0, or None ──
        normalizedPassword = privateKeyPassword.encode("utf-8") if privateKeyPassword else None

        ## ── Load the private key ──
        ## paramiko 4.0+: PKey.from_path() auto-detects key type (Ed25519, RSA, etc.)
        privateKey = None
        pwdAttempts = [normalizedPassword, None] if normalizedPassword is not None else [None]
        for pwd in pwdAttempts:
            try:
                privateKey = paramiko.PKey.from_path(privateKeyPath, passphrase=pwd)
                logMsg = "with passphrase" if pwd else "no passphrase"
                p1_localSetup.logger.info(f"{functionName}: SSH private key loaded successfully ({logMsg})")
                break
            except paramiko.ssh_exception.PasswordRequiredException:
                p1_localSetup.logger.error(f"{functionName}: Key requires a passphrase but none was provided")
                break
            except (paramiko.ssh_exception.SSHException, ValueError) as e:
                p1_localSetup.logger.warning(f"{functionName}: Key load attempt failed (pwd={'set' if pwd else 'None'}): {e}")
                continue

        if privateKey is None:
            raise ValueError(
                f"{functionName}: Could not load SSH private key from {privateKeyPath}. "
                f"Verify the key is a valid OpenSSH format key (Ed25519/RSA/ECDSA)."
            )
        p1_localSetup.logger.info(f"{functionName}: SSH private key loaded successfully")

        ## ── Connect to the SFTP server with retry logic ──
        ## Pattern follows Get_Slate_Info.py and Incoming_Student_Report.py
        attempt = 0
        retries = 5
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        while attempt < retries:
            try:
                ssh_client.connect(
                    hostname=sftpHost,
                    port=sftpPort,
                    username=sftpUsername,
                    pkey=privateKey,
                    banner_timeout=60,
                )
                p1_localSetup.logger.info(f"{functionName}: Successfully connected to SFTP server {sftpHost}:{sftpPort}")
                break

            except Exception as connError:
                attempt += 1
                if attempt < retries:
                    p1_localSetup.logger.warning(
                        f"{functionName}: Attempt {attempt} failed: {connError}. Retrying in 1 minute..."
                    )
                    time.sleep(60)
                else:
                    p1_localSetup.logger.error(
                        f"{functionName}: Attempt {attempt} failed: {connError}. No more retries."
                    )
                    if p1_errorHandler:
                        p1_errorHandler.sendError(functionName, p1_errorInfo=connError)
                    raise

        ## ── Open SFTP session and upload the file ──
        sftp_client = ssh_client.open_sftp()

        try:
            ## Build the remote file path
            localFileName = os.path.basename(p1_filePath)
            remoteFilePath = f"{sftpRemoteDir}/{localFileName}"

            p1_localSetup.logger.info(f"{functionName}: Uploading {p1_filePath} to {remoteFilePath}")
            sftp_client.put(p1_filePath, remoteFilePath)
            p1_localSetup.logger.info(f"{functionName}: File uploaded successfully to {remoteFilePath}")

        finally:
            ## Close the SFTP client and SSH connection
            sftp_client.close()
            ssh_client.close()
            p1_localSetup.logger.info(f"{functionName}: SFTP and SSH connections closed")

        ## ── Optionally tag with a success marker ──
        if p1_writeSuccessTag:
            fileDir = os.path.dirname(p1_filePath)
            baseName = os.path.splitext(os.path.basename(p1_filePath))[0]
            autoSuccessTagPath = os.path.join(fileDir, f"{baseName}_UPLOAD_SUCCESS.txt")
            writeSuccessTag(p1_filePath, autoSuccessTagPath, p1_localSetup)

    except Exception as Error:
        p1_localSetup.logger.error(f"{functionName}: {Error}")
        raise


## ══════════════════════════════════════════════════════════════════════════════
## Change Detection Helpers
## ══════════════════════════════════════════════════════════════════════════════

def hasChangedSinceLastUpload(p1_newDf: pd.DataFrame, p1_previousCsvPath: str, p1_successTagPath: str, p1_localSetup: LocalSetup) -> bool:
    """
    Compare a newly built DataFrame against the previously saved CSV that was
    successfully uploaded (indicated by the presence of a success tag file).

    Returns True if changes are detected or comparison is not possible.
    Returns False if the data is identical to the last successful upload.

    Args:
        p1_newDf (pd.DataFrame): The newly generated DataFrame.
        p1_previousCsvPath (str): Path to the CSV saved during the last run.
        p1_successTagPath (str): Path to the success tag written after a confirmed upload.
        p1_localSetup (LocalSetup): LocalSetup instance for logging.

    Returns:
        bool: True if upload should proceed, False if data is unchanged.
    """
    functionName = "hasChangedSinceLastUpload"

    if not os.path.exists(p1_previousCsvPath) or not os.path.exists(p1_successTagPath):
        p1_localSetup.logger.info(
            f"{functionName}: No previous successfully uploaded version found. Proceeding."
        )
        return True

    try:
        previousDf = readCsvWithEncoding(p1_previousCsvPath)

        newSorted = p1_newDf.sort_values(by=list(p1_newDf.columns)).reset_index(drop=True)
        prevSorted = previousDf.sort_values(by=list(previousDf.columns)).reset_index(drop=True)

        if newSorted.equals(prevSorted):
            p1_localSetup.logger.info(
                f"{functionName}: No changes detected since last successful upload. Skipping."
            )
            return False
        else:
            p1_localSetup.logger.info(
                f"{functionName}: Changes detected since last successful upload. Proceeding."
            )
            return True

    except Exception as compareError:
        p1_localSetup.logger.warning(
            f"{functionName}: Could not compare with previous version ({compareError}). Proceeding with upload."
        )
        return True


def writeSuccessTag(p1_uploadedFilePath: str, p1_successTagPath: str, p1_localSetup: LocalSetup):
    """
    Write a success tag file after a confirmed upload.

    Args:
        p1_uploadedFilePath (str): The file that was uploaded.
        p1_successTagPath (str): Where to write the tag.
        p1_localSetup (LocalSetup): LocalSetup instance for logging.
    """
    functionName = "writeSuccessTag"

    with open(p1_successTagPath, "w", encoding="utf-8") as tagFile:
        tagFile.write(f"Upload successful at {datetime.now().isoformat()}\n")
        tagFile.write(f"Uploaded file: {p1_uploadedFilePath}\n")
    p1_localSetup.logger.info(f"{functionName}: Success tag written to {p1_successTagPath}")


def removeStaleSuccessTag(p1_successTagPath: str, p1_localSetup: LocalSetup):
    """Remove a stale success tag before a new upload attempt."""
    functionName = "removeStaleSuccessTag"

    if os.path.exists(p1_successTagPath):
        os.remove(p1_successTagPath)
        p1_localSetup.logger.info(f"{functionName}: Removed stale success tag at {p1_successTagPath}")


## ══════════════════════════════════════════════════════════════════════════════
## Outcome Management Functions
## ══════════════════════════════════════════════════════════════════════════════

## This function takes in a start date and end date and returns what course week the course is currently in and what week the final week is
def determineCourseWeek (p1_startDate, p2_endDate, p1_referenceDate=None):      

    ## Default reference date to now (preserves original module-level datetime.now() behaviour)
    referenceDate = p1_referenceDate or datetime.now()

    ## Record the course start and end date as date time variables
    courseStartDateTime = datetime.strptime(p1_startDate, "%m/%d/%Y") + timedelta(weeks=3) ## Add 3 weeks as the sis date sent over is always 3 weeks earlier than the actual start date
    courseEndDateTime = datetime.strptime(p2_endDate, "%m/%d/%Y") - timedelta(weeks=3) ## Subtract 3 weeks as the sis date sent over is always 3 weeks earlier than the actual start date

    ## Determine the course's final week (e.g. 16 if it is a 16 week course)
    courseFinalWeek = math.ceil((courseEndDateTime - courseStartDateTime).days / 7) ## Round up as even a partial week is a week 

    ## Record the day of the week that the course starts
    courseStartWeekDay = courseStartDateTime.weekday()

    ## Determine what week the course is currently in
    courseWeek = (((referenceDate - (courseStartDateTime- timedelta(days=courseStartWeekDay))).days // 7) + 1) ## Add one week to make the first week be considered week 1

    ## Return the course week and the course final week
    return courseWeek, courseFinalWeek

## This function retrieves the data neccessary for determining and sending out relevent communication
def retrieveDataForRelevantCommunication (p1_localSetup
                                          , p1_errorHandler
                                          , p2_inputTerm
                                          , p3_targetDesignator
                                          ):

    functionName = "Retrieve Data For Relevant Communication"

    ## Define an auxillary data dict and auxillary df dict
    auxillaryDFDict = {}
    completeActiveCanvasCoursesDF = pd.DataFrame()

    try:

        ## Lazy-import report modules (ResourceModules cannot import ReportModules at module level)
        sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ReportModules"))
        from Outcome_Attachment_Report import termOutcomeAttachmentReport
        from Outcome_Results_Report import termProcessOutcomeResults

        ## Capture a single reference date for determineCourseWeek consistency
        referenceDate = datetime.now()

        ## Get the year of the term
        termYear = int(f"{p1_localSetup.dateDict['century']}{p2_inputTerm[2:]}")
        termPrefix = p2_inputTerm[:2]
        termWord = p1_localSetup._determineTermName(termPrefix)

        ## Retrieve the df of Active outcome courses which includes course code, required outcome/s, and the relevant instructor name/s, id/s, and email/s
        rawActiveOutcomeCourseDf = CanvasReport.getActiveOutcomeCoursesDf(p1_localSetup, p2_inputTerm, p3_targetDesignator)

        ## If the raw active outcome course df is empty
        if rawActiveOutcomeCourseDf.empty:

            ## Return an empty dataframe for the active outcome courses df and the auxillary df dict
            return rawActiveOutcomeCourseDf, auxillaryDFDict

        ## Make a list of the unique outcomes that are not blank 
        ## and a dict to hold the course id of the course named after each outcome
        auxillaryDFDict["Unique Outcomes"], auxillaryDFDict["Outcome Canvas Data Dict"] = getUniqueOutcomesAndOutcomeCoursesDict(p1_localSetup, p1_errorHandler, p2_inputTerm, rawActiveOutcomeCourseDf, p3_targetDesignator)

        ## If the retrieveDataForRelevantCommunication returned an empty list of unique outcomes or a dict with no keys for the outcome canvas data dict
        if not auxillaryDFDict["Unique Outcomes"] or not auxillaryDFDict["Outcome Canvas Data Dict"].keys():
            ## Log the fact that there are no valid outcomes to add to courses
            p1_localSetup.logger.error(f"\nNo valid outcomes found for term {p2_inputTerm} and target designator {p3_targetDesignator}. No outcomes will be added to courses.")
            ## Email the fact that there are no valid outcomes to add to courses
            p1_errorHandler.sendError (f"External Input Error: {functionName}", f"No valid outcomes found for term {p2_inputTerm} and target designator {p3_targetDesignator}. No outcomes will be added to courses.")
            ## Return an empty dataframe for the active outcome courses df and the auxillary df dict
            return pd.DataFrame(), auxillaryDFDict

        ## Remove any outcomes that don't have corresponding courses
        auxillaryDFDict["Active Outcome Courses DF"] = removeMissingOutcomes (
            p1_localSetup
            , p1_errorHandler
            , rawActiveOutcomeCourseDf
            , auxillaryDFDict["Unique Outcomes"]
            , auxillaryDFDict["Outcome Canvas Data Dict"]
            )

        ## Retrieve the csv of courses being uploaded to Canvas
        rawTermSisCoursesDF = pd.read_csv(f"{p1_localSetup.getExternalResourcePath('SIS')}canvas_course.csv")

        ## Keep only the courses with a status of active and a term_id of the input term
        activeSisCoursesDF = rawTermSisCoursesDF[(rawTermSisCoursesDF["status"] == "active") 
                                                 & (rawTermSisCoursesDF["term_id"] == p2_inputTerm)]

        ## Remove all columns from the active Sis courses df except the course_id column, the start_date, and the end_date
        reducedActiveSisCoursesDF = activeSisCoursesDF[["course_id", "start_date", "end_date"]]

        ## Get the raw term canvas courses df
        rawTermCanvasCoursesDF = CanvasReport.getCoursesDf(p1_localSetup, p2_inputTerm)

        ## Reset the index to ensure unique indices
        rawTermCanvasCoursesDF.reset_index(drop=True, inplace=True)

        ## Keep only the courses that are active and created_by_sis
        activeCanvasCoursesDF = rawTermCanvasCoursesDF[(rawTermCanvasCoursesDF["status"] != "deleted") 
                                                       & (rawTermCanvasCoursesDF["created_by_sis"] == True)]

        ## Add a Parent_Course_sis_id column to the completeActiveCanvasCoursesDF
        activeCanvasCoursesDF["Parent_Course_sis_id"] = ""

        ## Merge the two DataFrames prioritizing the start_date and end_date from reducedActiveSisCoursesDF
        ## and then using the data from rawCompleteActiveCanvasCoursesDF where the value from reducedActiveSisCoursesDF is nan or ""
        rawCompleteActiveCanvasCoursesDF = pd.merge(
            activeCanvasCoursesDF,
            reducedActiveSisCoursesDF,
            on="course_id",
            how="left",
            suffixes=('', '_sis')
        )


        ## Fill the start_date and end_date in rawCompleteActiveCanvasCoursesDF with the values from reducedActiveSisCoursesDF where they are nan or ""
        rawCompleteActiveCanvasCoursesDF['start_date'] = rawCompleteActiveCanvasCoursesDF['start_date_sis'].combine_first(rawCompleteActiveCanvasCoursesDF['start_date'])
        rawCompleteActiveCanvasCoursesDF['end_date'] = rawCompleteActiveCanvasCoursesDF['end_date_sis'].combine_first(rawCompleteActiveCanvasCoursesDF['end_date'])

        ## For any rows of auxillaryDFDict["Active Outcome Courses DF"] where there is a non nan Parent_Course_sis_id value
        for index, row in auxillaryDFDict["Active Outcome Courses DF"].dropna(subset=['Parent_Course_sis_id']).iterrows():

            ## Define a target course sis id
            targetCourseSisId = None

            ## If there is a parent course id
            if not pd.isna(row["Parent_Course_sis_id"]) and row["Parent_Course_sis_id"] not in ["", None]:

                ## Define the target course sis id as the parent course id
                targetCourseSisId = row["Parent_Course_sis_id"]

            ## If there is no parent course id
            else:

                ## Define the target course sis id as the course id
                targetCourseSisId = row['Course_sis_id']

            ## Get the index of the rawCompleteActiveCanvasCoursesDF that matches the course id
            index = rawCompleteActiveCanvasCoursesDF[rawCompleteActiveCanvasCoursesDF["course_id"] == targetCourseSisId].index[0]

            ## Set the Parent_Course_sis_id value in the rawCompleteActiveCanvasCoursesDF to the Parent_Course_sis_id value in the auxillaryDFDict["Active Outcome Courses DF"]
            rawCompleteActiveCanvasCoursesDF.at[index, "Parent_Course_sis_id"] = row["Parent_Course_sis_id"]

        ## Retrieve the all terms file
        allCanvasTermsDf = CanvasReport.getTermsDf(p1_localSetup)

        ## Drop the temporary columns
        rawCompleteActiveCanvasCoursesDF.drop(columns=['start_date_sis', 'end_date_sis'], inplace=True)

        ## Keep only the rows that have a canvas course id and a start_date
        completeActiveCanvasCoursesDF = rawCompleteActiveCanvasCoursesDF[
            (
                pd.notna(
                    rawCompleteActiveCanvasCoursesDF["canvas_course_id"]
                    )
             )
            & (
                pd.notna(
                    rawCompleteActiveCanvasCoursesDF["start_date"]
                    )
               )
            ]

        ## For each row in the completeActiveCanvasCoursesDF 
        for index, row in completeActiveCanvasCoursesDF.iterrows():

            ## If there is a parent course sis id
            if (
               not pd.isna(row["Parent_Course_sis_id"]) 
                and row["Parent_Course_sis_id"] not in ["", None]
                    ):

                ## Find the index of the parent course sis id
                parent_index = rawCompleteActiveCanvasCoursesDF[rawCompleteActiveCanvasCoursesDF["course_id"] == row["Parent_Course_sis_id"]].index[0]

                ## Set the start_date value from the parent course to the value for the row
                row["start_date"] = rawCompleteActiveCanvasCoursesDF.at[parent_index, "start_date"]

                ## Set the end_date value from the parent course to the value for the row
                row["end_date"] = rawCompleteActiveCanvasCoursesDF.at[parent_index, "end_date"]

            ## Retrieve the Term of the course
            courseTerm = rawCompleteActiveCanvasCoursesDF.at[index, "term_id"]

            ## Get the index of the term within the term_id column of the allCanvasTermsDf
            term_index = allCanvasTermsDf[allCanvasTermsDf["term_id"] == courseTerm].index[0]

            ## If the start date is nan or blank
            if not str(row["start_date"]) or str(row["start_date"]) == "nan":

                ## Set the start_date value from the term to the value for the row
                row["start_date"] = allCanvasTermsDf.at[term_index, "start_date"]

            ## If the end date is nan or blank
            if not str(row["end_date"]) or str(row["end_date"]) == "nan":

                ## Set the end_date value from the term to the value for the row
                row["end_date"] = allCanvasTermsDf.at[term_index, "end_date"]

            ## Get the start date and end date from the row
            start_date = parser.parse(row["start_date"])
            end_date = parser.parse(row["end_date"])

            ## Transform both to %m%d%Y format
            start_date = start_date.strftime("%m/%d/%Y")
            end_date = end_date.strftime("%m/%d/%Y")

            ## Set the start_date and end_date values in the rawCompleteActiveCanvasCoursesDF to the reformatted values
            completeActiveCanvasCoursesDF.at[index, "start_date"] = start_date
            completeActiveCanvasCoursesDF.at[index, "end_date"] = end_date

        ## If the complete active canvas courses df is empty
        if completeActiveCanvasCoursesDF.empty:

            ## Return an empty dataframe for the active outcome courses df and the auxillary df dict
            return completeActiveCanvasCoursesDF, auxillaryDFDict

        ## Define the term related path to the outcome attachment report
        termOutcomeAttachmentReportPath = termOutcomeAttachmentReport(p2_inputTerm, p3_targetDesignator)
        auxillaryDFDict["Outcome Courses Without Attachments DF"] = pd.read_csv(termOutcomeAttachmentReportPath)

        ## Define the term related path to the outcome results report
        termProcessOutcomeResultsPath = termProcessOutcomeResults(p2_inputTerm, p3_targetDesignator)[0]
        outcomeCoursesDataDF = pd.read_excel(termProcessOutcomeResultsPath)

        ## Create a df of outcome courses that have not been assessed
        auxillaryDFDict["Unassessed Outcome Courses DF"] = outcomeCoursesDataDF[outcomeCoursesDataDF["Assessment_Status"] != "Assessed"]

        ## Create a new "Course Week" column and a "Course Final Week" column in the complete active canvas courses df by sending the start and end dates to determineCourseWeek
        completeActiveCanvasCoursesDF["Course Week"], completeActiveCanvasCoursesDF["Course Final Week"] = zip(
            *completeActiveCanvasCoursesDF.apply(
                lambda row: determineCourseWeek(
                    row["start_date"]
                    , row["end_date"]
                    , p1_referenceDate=referenceDate
                    )
                , axis=1
                )
            )

        ## Return the active outcome courses df, the complete active canvas courses df, and the auxillary df dict
        return completeActiveCanvasCoursesDF, auxillaryDFDict

    except Exception as Error:
        p1_errorHandler.sendError(functionName, Error)
        return completeActiveCanvasCoursesDF, auxillaryDFDict 

## This function processes the rows of the CSV file and sends on the relavent data to process_course
def addOutcomeToCourse (p1_localSetup
                        , p1_errorHandler
                        , targetCourseDataDict
                        , auxillaryDFDict
                        ):
    functionName = "Add Outcome/s to courses"

    try:

        ## If the targetCourseDataDict's course_sis_id is not in the aux df dict's active outcome course df,
        ## or if it is empty, skip it
        if (targetCourseDataDict['course_id'] not in auxillaryDFDict["Active Outcome Courses DF"]["Course_sis_id"].values 
            or not targetCourseDataDict['course_id']):
            return

        ## Get the index of the course_id with the auxillaryDFDict's Active Outcome Courses Df
        targetCourseActiveOutcomeCourseDfIndex = auxillaryDFDict["Active Outcome Courses DF"][auxillaryDFDict["Active Outcome Courses DF"]["Course_sis_id"] == targetCourseDataDict['course_id']].index[0]

        ## Get the row of the targetCourseActiveOutcomeCourseDfIndex as a dict
        targetCourseActiveOutcomeCourseDataDict = auxillaryDFDict["Active Outcome Courses DF"].loc[targetCourseActiveOutcomeCourseDfIndex].to_dict()

        ## For each targetCourseDataDict in our CSV file pull the course sis id column and outcome column names
        ## Sample sess values: FA2022_PHIL2030_01
        ## Sample outcome value: GE_CF4_V1.0
        targetCourseSisId = None
        outcomeKeys = [col for col in targetCourseActiveOutcomeCourseDataDict.keys() if "Outcome" in col and "Area" not in col]

        ## If there is a parent course id
        if (
            pd.isna(targetCourseDataDict["Parent_Course_sis_id"]) 
            and targetCourseDataDict["Parent_Course_sis_id"] not in ["", None]
            ):

                ## Define the target course sis id as the parent course id
                targetCourseSisId = targetCourseDataDict["Parent_Course_sis_id"]

        ## If there is no parent course id
        else:

            ## Define the target course sis id as the course id
            targetCourseSisId = targetCourseDataDict['course_id']

        ## Log the start of the process
        p1_localSetup.logger.info("\n     Course:" + targetCourseDataDict['course_id'])

        ## Create the base course API urls
        baseCourseApiUrl = f"{coreCanvasApiUrl}courses/sis_course_id:{targetCourseSisId}"

        ## For each outcome in the targetCourseDataDict
        for outcome in outcomeKeys:

            ## If the outcome is empty skip it
            if pd.isna(targetCourseActiveOutcomeCourseDataDict[outcome]) or not targetCourseActiveOutcomeCourseDataDict[outcome] or not outcome or pd.isna(outcome):
                continue

            ## Get the outcome canvas data dict from the auxillary df dict
            outcomeCanvasData = auxillaryDFDict[
                "Outcome Canvas Data Dict"
                ][
                    targetCourseActiveOutcomeCourseDataDict[
                        outcome
                        ]
                    ]

            ## Define the API url to get the outcome groups of the course
            courseOutcomeGroupsApiUrl = f"{baseCourseApiUrl}/outcome_groups"

            ## Make the API call to get the outcome groups of the course
            courseOutcomeGroupsObject, _ = makeApiCall(p1_localSetup, p1_apiUrl=courseOutcomeGroupsApiUrl)

            ## If the API status code is anything other than 200 it is an error, so log it and skip
            if (courseOutcomeGroupsObject.status_code != 200):
                p1_localSetup.logger.error("\nCourse Error: " + str(courseOutcomeGroupsObject.status_code))
                p1_localSetup.logger.error(courseOutcomeGroupsApiUrl)
                p1_localSetup.logger.error(courseOutcomeGroupsObject.url)
                continue

            ## Define a variable to hold the whether the course already has the outcome group and another to hold its canvas id
            outcomeGroupAlreadyInCourse = False
            outcomeGroupCanvasIdInCourse = None

            ## Define a variable to hold the outcome group id of the course itself in case the outcome group needs to be added to the course
            courseOutcomeGroupCanvasId = None

            ## For each outcome group in the course outcome groups object
            for courseOutcomeGroup in courseOutcomeGroupsObject.json():

                ## If the title contains the target sis id 
                if targetCourseSisId in courseOutcomeGroup['title']:
                    ## Set the course outcome group canvas id to the id of the outcome group
                    courseOutcomeGroupCanvasId = courseOutcomeGroup['id']
                    if outcomeCanvasData["Outcome Group is Root Account"]:
                        outcomeGroupAlreadyInCourse = True
                        outcomeGroupCanvasIdInCourse = courseOutcomeGroup['id']


                ## Else if the the title is equal to the outcome group title from the outcome canvas data dict
                elif courseOutcomeGroup['title'] == outcomeCanvasData["Outcome Group Title"] or courseOutcomeGroup['title'] == outcomeCanvasData["Outcome Group Id"]:
                    ## Set the outcome group already in course variable to true
                    outcomeGroupAlreadyInCourse = True
                    outcomeGroupCanvasIdInCourse = courseOutcomeGroup['id']
                    ## Break out of the loop
                    break

            if courseOutcomeGroupCanvasId is None:
                rootOutcomeGroupApiUrl = f"{baseCourseApiUrl}/root_outcome_group"

                rootOutcomeGroupObject, _ = makeApiCall(p1_localSetup, p1_apiUrl=rootOutcomeGroupApiUrl)

                if (rootOutcomeGroupObject.status_code != 200):
                    p1_localSetup.logger.error("\nCourse Error: " + str(rootOutcomeGroupObject.status_code))
                    p1_localSetup.logger.error(rootOutcomeGroupApiUrl)
                    p1_localSetup.logger.error(rootOutcomeGroupObject.url)
                    continue

                courseOutcomeGroupCanvasId = rootOutcomeGroupObject.json()['id']

            ## If the outcome group is not already in the course
            if not outcomeGroupAlreadyInCourse:

                ## Define the API url to add the outcome group to the course using the course outcome group canvas id
                addOutcomeGroupToCourseApiUrl = f"{baseCourseApiUrl}/outcome_groups/{courseOutcomeGroupCanvasId}/import"

                ## Define the API payload to add the outcome group to the course
                addOutcomeGroupToCourseApiPayload = {
                    "source_outcome_group_id": outcomeCanvasData["Outcome Group Id"],
                    }

                ## Make the API call to add the outcome group to the course
                addOutcomeGroupToCourseObject, _ = makeApiCall(
                    p1_localSetup, 
                    p1_apiUrl=addOutcomeGroupToCourseApiUrl, 
                    p1_payload=addOutcomeGroupToCourseApiPayload, 
                    p1_apiCallType="post"
                    )

                ## If the API status code is anything other than 200 it is an error, so log it and skip
                if (addOutcomeGroupToCourseObject.status_code != 200):
                    p1_localSetup.logger.error("\nCourse Error: " + str(addOutcomeGroupToCourseObject.status_code))
                    p1_localSetup.logger.error(addOutcomeGroupToCourseApiUrl)
                    p1_localSetup.logger.error(addOutcomeGroupToCourseObject.url)
                    continue

                ## Log the fact that the outcome group has been added to the course
                p1_localSetup.logger.info(f"\n {targetCourseSisId} has been added outcome group {outcomeCanvasData['Outcome Group Title']}")

                ## Retrieve the ooutcomeGroupCanvasIdInCourse from the API call response
                outcomeGroupCanvasIdInCourse = addOutcomeGroupToCourseObject.json()['id']

            ## Define the API url to add the outcome to the course outcome group
            addOutcomeToCourseApiUrl = f"{baseCourseApiUrl}/outcome_groups/{outcomeGroupCanvasIdInCourse}/outcomes/{outcomeCanvasData['Outcome Canvas Id']}"

            ## Make the API call to add the outcome to the course
            addOutcomeToCourseObject, _ = makeApiCall(p1_localSetup, p1_apiUrl=addOutcomeToCourseApiUrl, p1_apiCallType="put")

            ## If the API status code is anything other than 200 it is an error, so log it and skip
            if (addOutcomeToCourseObject.status_code != 200):
                p1_localSetup.logger.error("\nCourse Error: " + str(addOutcomeToCourseObject.status_code))
                p1_localSetup.logger.error(addOutcomeToCourseApiUrl)
                p1_localSetup.logger.error(addOutcomeToCourseObject.url)
                continue

            ## Log the fact that the outcome has been added to the course
            p1_localSetup.logger.info(f"\n {targetCourseSisId} has had outcome {targetCourseActiveOutcomeCourseDataDict[outcome]} added")

    except Exception as Error:
        p1_errorHandler.sendError (functionName, Error)

## This function removes any outcomes that don't have corresponding courses
def removeMissingOutcomes (p1_localSetup, p1_errorHandler, p1_activeOutcomeCourseDf, p1_uniqueOutcomes, p1_outcomeCanvasDataDict):
    functionName = "Remove Missing Outcomes"

    try:

        ## Get a list of all unique outcomes that are not in the keys of the outcomeCanvasDataDict
        missingOutcomes = [outcome for outcome in p1_uniqueOutcomes if outcome not in p1_outcomeCanvasDataDict.keys()]

        ## If there are missing outcomes
        if missingOutcomes:

            ## For each row of the active outcome course df
            for index, row in p1_activeOutcomeCourseDf.iterrows():

                ## Create a list of the outcome columns in the row
                outcomesColumns = [col for col in row.keys() if "Outcome" in col and "Area" not in col]

                ## For each outcome column in the row
                for outcome in outcomesColumns:

                    ## If the outcome is in the missing outcomes list
                    if row[outcome] in missingOutcomes:

                        ## Replace it with a blank string
                        p1_activeOutcomeCourseDf.loc[index, outcome] = ""

                        ## Send an error email about the missing outcome
                        p1_errorHandler.sendError (f"External Input Error: {functionName}", f"Outcome Missing Import Course: {row[outcome]}")

                ## If all outcome values in the row are blank strings
                if all([pd.isna(row[outcome]) for outcome in outcomesColumns]):

                    ## Drop the row
                    p1_activeOutcomeCourseDf.drop(index, inplace=True)

        ## Return the active outcome course df
        return p1_activeOutcomeCourseDf

    except Exception as Error:
        p1_errorHandler.sendError (functionName, Error)
        return p1_activeOutcomeCourseDf

## This function returns a dict with the course id of the course named after each outcome
def getUniqueOutcomesAndOutcomeCoursesDict (p1_localSetup, p1_errorHandler, p3_inputTerm, p1_activeOutcomeCourseDf, p4_targetDesignator):
    functionName = "Get Unique Outcomes And Outcome Courses Dict"

    try:

        ## Make a df with one collumn where all outcome columns that don't have area in the name are stacked
        targetOutcomesDF = p1_activeOutcomeCourseDf[[col for col in p1_activeOutcomeCourseDf.columns if "Outcome" in col and "Area" not in col]].stack().reset_index(drop=True)

        ## Make a list of the unique outcomes that are not blank
        uniqueTargetOutcomes = [outcome for outcome in targetOutcomesDF.unique() 
                          if (
                              pd.notna(outcome)
                              and str(outcome).strip() not in ("", "nan", "none", "NaN", "None")
                              )
                          ]

        ## Retrieve the Automated Outcome Tool Variables excel file as a df    
        automatedOutcomeToolVariablesDf = pd.read_excel(
            os.path.join(
                p1_localSetup.getExternalResourcePath("TLC"), 
                "Automated Outcome Tool Variables.xlsx"
                )
        )

        ## Get the account name associated with the target designator
        targetAccountName = automatedOutcomeToolVariablesDf.loc[
            automatedOutcomeToolVariablesDf["Target Designator"] == p4_targetDesignator, 
            "Outcome Location Account Name"
            ].values[0]

        ## Open the p4_targetDesignator relevant outcome df
        targetDesignatorCanvasOutcomeDf = CanvasReport.getOutcomesDf(p1_localSetup, p3_inputTerm, targetAccountName, p4_targetDesignator)

        ## Open the accounts df
        accountsDf = CanvasReport.getAccountsDf(p1_localSetup)

        ## Get the target account id from the accounts df using the target account name
        targetCanvasAccountId = (
            1 if targetAccountName == "NNU"
            else accountsDf.loc[accountsDf["name"] == targetAccountName, "canvas_account_id"].values[0]
            )

        ## Define a dict to hold tail of the api url to add the outcome to a course
        uniqueOutcomesCanvasData = {}

        ## For each outcome in the unique target outcomes list
        for outcome in uniqueTargetOutcomes:

            ## Get the index of the outcome from the title column of the targetDesignatorCanvasOutcomeDf
            outcomeIndexSearch = targetDesignatorCanvasOutcomeDf[targetDesignatorCanvasOutcomeDf['title'] == outcome].index

            ## If the outcomeIndexs is empty
            if outcomeIndexSearch.empty:

                ## Log the fact that the outcome was not found
                p1_localSetup.logger.error(f"\nOutcome not found: {outcome}")

                ## Email the fact that the outcome was not found
                p1_errorHandler.sendError (f"External Input Error: {functionName}", f"Outcome not found: {outcome}")
                ## Skip to the next outcome
                continue

            ## Use the outcome index to get the vendor_guid from the outcome with the outcome as the title
            outcomeParentGuid = targetDesignatorCanvasOutcomeDf.loc[outcomeIndexSearch[0], 'parent_guids']

            ## Define the API url to add the outcome to the course using the target canvas account id 
            ## and the outcome parent guid and outcome vendor guid
            outcomeGroupsApiUrl = f"{coreCanvasApiUrl}accounts/{targetCanvasAccountId}/outcome_groups"

            ## Make an API call to get the outcome groups in the target account
            outcomeGroupsObject, outcomeGroupsObjectList = makeApiCall(p1_localSetup, p1_apiUrl=outcomeGroupsApiUrl)

            ## If the API status code is anything other than 200 it is an error, so log it and skip`
            if (outcomeGroupsObject.status_code != 200):
                p1_localSetup.logger.error("\nCourse Error: " + str(outcomeGroupsObject.status_code))
                p1_localSetup.logger.error(outcomeGroupsApiUrl)
                p1_localSetup.logger.error(outcomeGroupsObject.url)
                continue

            ## If the the api response was paginated a list of the responses will have been returned
            outcomeGroupsJsonList = []
            if outcomeGroupsObjectList:
                ## Paginated: flatten all pages
                outcomeGroupsJsonList = flattenApiObjectToJsonList(
                    p1_localSetup,
                    outcomeGroupsObjectList,
                    outcomeGroupsApiUrl
                )
            else:
                ## Non-paginated: just use the single response's json
                singlePageData = outcomeGroupsObject.json()
                if isinstance(singlePageData, list):
                    outcomeGroupsJsonList = singlePageData
                else:
                    outcomeGroupsJsonList = [singlePageData]


            ## Define a variable to hold the outcome group Canvas id
            outcomeGroupCanvasId = None

            ## For each outcome group in the outcome groups json list
            for outcomeGroup in outcomeGroupsJsonList:

                ## If the outcomeParentGuid is nan
                if (
                    pd.isna(outcomeParentGuid) 
                    or not outcomeParentGuid 
                    or str(outcomeParentGuid).strip() in ("", "nan", "none", "NaN", "None")
                    ):
                    ## The outcome is in the root outcome group, so test if the title is equal to the target account name
                    if outcomeGroup['title'] == targetAccountName:
                        ## Set the outcome group canvas id to the id of the outcome group
                        outcomeGroupCanvasId = outcomeGroup['id']
                        break
                    continue

                ## If the outcome group's vendor guid is equal to the outcome parent guid
                if outcomeGroup['vendor_guid'] == outcomeParentGuid:
                    ## Set the outcome group canvas id to the id of the outcome group
                    outcomeGroupCanvasId = outcomeGroup['id']
                    break

                ## Fallback: match by the numeric ID embedded in the parent guid (e.g. 'canvas_outcome_group:20965')
                if (
                    outcomeParentGuid 
                    and ':' in str(outcomeParentGuid)
                    and str(outcomeGroup['id']) == str(outcomeParentGuid).split(':')[-1]
                    ):
                    outcomeGroupCanvasId = outcomeGroup['id']
                    break

            ## Guard: skip this outcome if the outcome group was not found
            if outcomeGroupCanvasId is None:
                p1_localSetup.logger.error(f"\nOutcome group not found for parent guid '{outcomeParentGuid}' (outcome: {outcome})")
                p1_errorHandler.sendError(f"External Input Error: {functionName}", f"Outcome group not found for parent guid '{outcomeParentGuid}' (outcome: {outcome})")
                continue

            ## Define an outcome api url by tagging on the outcome group canvas id 
            ## and /outcomes to the end of the outcome groups api url
            outcomesApiUrl = f"{outcomeGroupsApiUrl}/{outcomeGroupCanvasId}/outcomes"

            ## Make an API call to the outcomes api url to get the outcomes in the outcome group
            outcomesObjects, _ = makeApiCall(p1_localSetup, p1_apiUrl=outcomesApiUrl)

            ## If the API status code is anything other than 200 it is an error, so log it and skip
            if (outcomesObjects.status_code != 200):
                p1_localSetup.logger.error("\nCourse Error: " + str(outcomesObjects.status_code))
                p1_localSetup.logger.error(outcomesApiUrl)
                p1_localSetup.logger.error(outcomesObjects.url)
                continue

            ## Use the outcome index to get the vendor_guid from the outcome with the outcome as the title
            outcomeVendorGuid = targetDesignatorCanvasOutcomeDf.loc[outcomeIndexSearch[0], 'vendor_guid']

            ## Define a variable to hold the outcome canvas id
            outcomeCanvasId = None

            ## For each outcome in the outcomes object
            for outcomeData in [
                outcomeObject["outcome"] 
                for outcomeObject in outcomesObjects.json() 
                if "outcome" in outcomeObject
                ]:
                ## If the outcome's vendor guid is equal to the outcome vendor guid
                ## Or if the outcomeData id is equal to the outcome vendor guid when split by ':'
                if (
                    outcomeData['vendor_guid'] == outcomeVendorGuid 
                    or str(outcomeData["id"]) == str(outcomeVendorGuid.split(':')[1])
                    ):
                    ## Set the outcome canvas id to the id of the outcome
                    outcomeCanvasId = outcomeData['id']
                    ## Break out of the loop
                    break

            ## If the outcome canvas id is not found
            if outcomeCanvasId is None:
                ## Log the fact that the outcome was not found in the outcome group
                p1_localSetup.logger.error(f"\nOutcome not found in outcome group: {outcome}")

                ## Email the fact that the outcome was not found in the outcome group
                p1_errorHandler.sendError (f"External Input Error: {functionName}", f"Outcome not found in outcome group: {outcome}")
                ## Skip to the next outcome
                continue

            ## Use the outcome Parent guide to find the index of the outcome group 
            ## with the same parent guid in the outcome groups object for the target account
            outcomeGroupIndexSearch = (
                targetDesignatorCanvasOutcomeDf[targetDesignatorCanvasOutcomeDf['vendor_guid'] == outcomeParentGuid].index
                )

            ## Use the outcome group index to get the outcome group title 
            ## from the outcome group column in the targetDesignatorCanvasOutcomeDf
            outcomeGroupTitle = (
                targetAccountName if str(outcomeParentGuid).strip() == "nan" 
                else targetDesignatorCanvasOutcomeDf.loc[outcomeGroupIndexSearch[0], 'title']
                )

            ## Make a dict for the outcome with the outcome group title and outcome canvas id
            uniqueOutcomesCanvasData[outcome] = {
                "Outcome Group Title": outcomeGroupTitle,
                "Outcome Canvas Id": outcomeCanvasId,
                "Outcome Group Id": outcomeGroupCanvasId,
                "Outcome Group is Root Account" : True if outcomeGroupTitle == targetAccountName else False
            }


        return uniqueTargetOutcomes, uniqueOutcomesCanvasData

    except Exception as Error:
        p1_errorHandler.sendError (functionName, Error)
        return [], {}

