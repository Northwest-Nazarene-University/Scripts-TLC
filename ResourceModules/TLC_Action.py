## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

## Import Generic Modules
import os, sys, re, time, math, pandas as pd, paramiko, secrets, string
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from dateutil import parser

try: ## If the module is run directly
    from Local_Setup import LocalSetup
    from TLC_Common import (
        getEncryptionKey,
        makeApiCall,
        flattenApiObjectToJsonList,
        isPresent,
        isMissing,
        isFileRecent,
        readTargetCsv,
        runThreadedRows,
        getDesignatorSettingsDict,
        getAutomatedOutcomeToolVariablesDf,
        getDesignatorFilesByType,
    )
    from Canvas_Report import CanvasReport
except ImportError: ## Otherwise as a relative import if the module is imported
    from .Local_Setup import LocalSetup
    from .TLC_Common import (
        getEncryptionKey,
        makeApiCall,
        flattenApiObjectToJsonList,
        isPresent,
        isMissing,
        isFileRecent,
        readTargetCsv,
        runThreadedRows,
        getDesignatorSettingsDict,
        getAutomatedOutcomeToolVariablesDf,
        getDesignatorFilesByType,
    )
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


## ==============================================================================
## CSV Helpers
## ==============================================================================

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


## ==============================================================================
## SFTP Private Key Password Management
## ==============================================================================

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


## ==============================================================================
## SFTP Upload Function
## ==============================================================================

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


## ==============================================================================
## Change Detection Helpers
## ==============================================================================

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


## ==============================================================================
## Outcome Management Functions
## ==============================================================================

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

        ## Get the term and target designator for which the data is being retrieved
        designatorDict = getDesignatorSettingsDict(p1_localSetup, p3_targetDesignator)
        courseLevel = "All" if isMissing(designatorDict["Course Level"]) else designatorDict["Course Level"]
        targetAccountName = "NNU" if isMissing(designatorDict["Outcome Location Account Name"]) else designatorDict["Outcome Location Account Name"]

        ## Determine the graduate term equivalent (e.g. FA25 → GF25)
        gradTerm = CanvasReport.determineGradTerm(p2_inputTerm)

        ## Build the list of terms to query based on the Course Level
        if courseLevel == "Undergraduate":
            relevantTerms = [p2_inputTerm]
        elif courseLevel == "Graduate":
            relevantTerms = [gradTerm]
        else:  ## "All" — include both
            relevantTerms = [p2_inputTerm, gradTerm] if gradTerm != p2_inputTerm else [p2_inputTerm]

        ## Retrieve the df of Active outcome courses which includes course code, required outcome/s, and the relevant instructor name/s, id/s, and email/s
        rawActiveOutcomeCourseDf = CanvasReport.getActiveOutcomeCoursesDf(p1_localSetup, p2_inputTerm, p3_targetDesignator)

        ## If the raw active outcome course df is empty
        if isMissing(rawActiveOutcomeCourseDf):

            ## Return an empty dataframe for the active outcome courses df and the auxillary df dict
            return rawActiveOutcomeCourseDf, auxillaryDFDict

        ## Make a list of the unique outcomes that are not blank 
        ## and a dict to hold the course id of the course named after each outcome
        auxillaryDFDict["Unique Outcomes"], auxillaryDFDict["Outcome Canvas Data Dict"] = getUniqueOutcomesAndOutcomeCoursesDict(p1_localSetup, p1_errorHandler, p2_inputTerm, rawActiveOutcomeCourseDf, p3_targetDesignator, targetAccountName)

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
        activeSisCoursesDF = rawTermSisCoursesDF[
            (rawTermSisCoursesDF["status"] == "active") 
            & (rawTermSisCoursesDF["term_id"].isin(relevantTerms))
        ]

        ## Remove all columns from the active Sis courses df except the course_id column, the start_date, and the end_date
        reducedActiveSisCoursesDF = activeSisCoursesDF[["course_id", "start_date", "end_date"]]

        ## Get the raw term canvas courses df for all relevant terms
        allCanvasCoursesDfs = []
        for relevantTerm in relevantTerms:
            termDf = CanvasReport.getCoursesDf(p1_localSetup, relevantTerm)
            if isPresent(termDf):
                allCanvasCoursesDfs.append(termDf)
        
        rawTermCanvasCoursesDF = pd.concat(allCanvasCoursesDfs, ignore_index=True) if allCanvasCoursesDfs else pd.DataFrame()

        ## Reset the index to ensure unique indices
        rawTermCanvasCoursesDF.reset_index(drop=True, inplace=True)

        ## Keep only the courses that are active and created_by_sis
        activeCanvasCoursesDF = rawTermCanvasCoursesDF[(rawTermCanvasCoursesDF["status"] != "deleted") 
                                                       & (rawTermCanvasCoursesDF["created_by_sis"] == True)].copy()

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
            if isPresent(row["Parent_Course_sis_id"]):

                ## Define the target course sis id as the parent course id
                targetCourseSisId = row["Parent_Course_sis_id"]

            ## If there is no parent course id
            else:

                ## Define the target course sis id as the course id
                targetCourseSisId = row['Course_sis_id']

            ## Get the index of the rawCompleteActiveCanvasCoursesDF that matches the course id
            matchingIndices = rawCompleteActiveCanvasCoursesDF[rawCompleteActiveCanvasCoursesDF["course_id"] == targetCourseSisId].index

            if isMissing(matchingIndices):
                p1_localSetup.logger.warning(
                    f"{functionName}: targetCourseSisId '{targetCourseSisId}' from outcome course "
                    f"'{row.get('Course_sis_id', 'unknown')}' not found in active Canvas courses DF. "
                    f"This may indicate a term mismatch (e.g., a GF-prefix course listed under FA). Skipping."
                )
                continue

            index = matchingIndices[0]

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

                ## Set the start_date value from the parent course to the value for the row
                row["start_date"] = rawCompleteActiveCanvasCoursesDF.at[parent_index, "start_date"]

                ## Set the end_date value from the parent course to the value for the row
                row["end_date"] = rawCompleteActiveCanvasCoursesDF.at[parent_index, "end_date"]

            ## Retrieve the Term of the course
            courseTerm = rawCompleteActiveCanvasCoursesDF.at[index, "term_id"]

            ## Get the index of the term within the term_id column of the allCanvasTermsDf
            termMatchIndices = allCanvasTermsDf[allCanvasTermsDf["term_id"] == courseTerm].index
            if isMissing(termMatchIndices):
                p1_localSetup.logger.warning(
                    f"{functionName}: Term '{courseTerm}' not found in Canvas terms DF. "
                    f"Skipping date fallback for course '{row.get('course_id', 'unknown')}'."
                )
                continue
            term_index = termMatchIndices[0]

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
        if isMissing(completeActiveCanvasCoursesDF):

            ## Return an empty dataframe for the active outcome courses df and the auxillary df dict
            return completeActiveCanvasCoursesDF, auxillaryDFDict

        ## Define the term related path to the outcome attachment report
        termOutcomeAttachmentReportPath = termOutcomeAttachmentReport(p2_inputTerm, p3_targetDesignator)
        if isFileRecent(p1_localSetup, termOutcomeAttachmentReportPath):
            auxillaryDFDict["Outcome Courses Without Attachments DF"] = pd.read_csv(termOutcomeAttachmentReportPath)
        else:
            p1_localSetup.logger.warning(f"Outcome attachment report not found or stale: {termOutcomeAttachmentReportPath}")
            auxillaryDFDict["Outcome Courses Without Attachments DF"] = pd.DataFrame()

        ## Define the term related path to the outcome results report
        termProcessOutcomeResultsPath = termProcessOutcomeResults(p2_inputTerm, p3_targetDesignator)[0]
        if isFileRecent(p1_localSetup, termProcessOutcomeResultsPath):
            outcomeCoursesDataDF = pd.read_excel(termProcessOutcomeResultsPath)
        else:
            p1_localSetup.logger.warning(f"Outcome results report not found or stale: {termProcessOutcomeResultsPath}")
            outcomeCoursesDataDF = pd.DataFrame(columns=["Assessment_Status"])

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
        p1_localSetup.logInfoThreadSafe("\n     Course:" + targetCourseDataDict['course_id'])

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
                p1_localSetup.logErrorThreadSafe("\nCourse Error: " + str(courseOutcomeGroupsObject.status_code))
                p1_localSetup.logErrorThreadSafe(courseOutcomeGroupsApiUrl)
                p1_localSetup.logErrorThreadSafe(courseOutcomeGroupsObject.url)
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
                    p1_localSetup.logErrorThreadSafe("\nCourse Error: " + str(rootOutcomeGroupObject.status_code))
                    p1_localSetup.logErrorThreadSafe(rootOutcomeGroupApiUrl)
                    p1_localSetup.logErrorThreadSafe(rootOutcomeGroupObject.url)
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
                    p1_localSetup.logErrorThreadSafe("\nCourse Error: " + str(addOutcomeGroupToCourseObject.status_code))
                    p1_localSetup.logErrorThreadSafe(addOutcomeGroupToCourseApiUrl)
                    p1_localSetup.logErrorThreadSafe(addOutcomeGroupToCourseObject.url)
                    continue

                ## Log the fact that the outcome group has been added to the course
                p1_localSetup.logInfoThreadSafe(f"\n {targetCourseSisId} has been added outcome group {outcomeCanvasData['Outcome Group Title']}")

                ## Retrieve the ooutcomeGroupCanvasIdInCourse from the API call response
                outcomeGroupCanvasIdInCourse = addOutcomeGroupToCourseObject.json()['id']

            ## Define the API url to add the outcome to the course outcome group
            addOutcomeToCourseApiUrl = f"{baseCourseApiUrl}/outcome_groups/{outcomeGroupCanvasIdInCourse}/outcomes/{outcomeCanvasData['Outcome Canvas Id']}"

            ## Make the API call to add the outcome to the course
            addOutcomeToCourseObject, _ = makeApiCall(p1_localSetup, p1_apiUrl=addOutcomeToCourseApiUrl, p1_apiCallType="put")

            ## If the API status code is anything other than 200 it is an error, so log it and skip
            if (addOutcomeToCourseObject.status_code != 200):
                p1_localSetup.logErrorThreadSafe("\nCourse Error: " + str(addOutcomeToCourseObject.status_code))
                p1_localSetup.logErrorThreadSafe(addOutcomeToCourseApiUrl)
                p1_localSetup.logErrorThreadSafe(addOutcomeToCourseObject.url)
                continue

            ## Log the fact that the outcome has been added to the course
            p1_localSetup.logInfoThreadSafe(f"\n {targetCourseSisId} has had outcome {targetCourseActiveOutcomeCourseDataDict[outcome]} added")

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
def getUniqueOutcomesAndOutcomeCoursesDict (p1_localSetup, p1_errorHandler, p3_inputTerm, p1_activeOutcomeCourseDf, p4_targetDesignator, p1_targetAccountName):
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

        ## Open the p4_targetDesignator relevant outcome df
        targetDesignatorCanvasOutcomeDf = CanvasReport.getOutcomesDf(p1_localSetup, p3_inputTerm, p1_targetAccountName, p4_targetDesignator)

        ## Open the accounts df
        accountsDf = CanvasReport.getAccountsDf(p1_localSetup)

        ## Get the target account id from the accounts df using the target account name
        targetCanvasAccountId = (
            1 if p1_targetAccountName == "NNU"
            else accountsDf.loc[accountsDf["name"] == p1_targetAccountName, "canvas_account_id"].values[0]
            )

        ## Define a dict to hold tail of the api url to add the outcome to a course
        uniqueOutcomesCanvasData = {}

        ## For each outcome in the unique target outcomes list
        for outcome in uniqueTargetOutcomes:

            ## Get the index of the outcome from the title column of the targetDesignatorCanvasOutcomeDf
            outcomeIndexSearch = targetDesignatorCanvasOutcomeDf[targetDesignatorCanvasOutcomeDf['title'] == outcome].index

            ## If the outcomeIndexs is empty
            if isMissing(outcomeIndexSearch):

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
                    if outcomeGroup['title'] == p1_targetAccountName:
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
                targetDesignatorCanvasOutcomeDf.loc[outcomeGroupIndexSearch[0], 'title'] if isPresent(outcomeParentGuid) 
                else p1_targetAccountName
                )
                
                

            ## Make a dict for the outcome with the outcome group title and outcome canvas id
            uniqueOutcomesCanvasData[outcome] = {
                "Outcome Group Title": outcomeGroupTitle,
                "Outcome Canvas Id": outcomeCanvasId,
                "Outcome Group Id": outcomeGroupCanvasId,
                "Outcome Group is Root Account" : True if outcomeGroupTitle == p1_targetAccountName else False
            }


        return uniqueTargetOutcomes, uniqueOutcomesCanvasData

    except Exception as Error:
        p1_errorHandler.sendError (functionName, Error)
        return [], {}


## ============================================================================
## Course Grades By Course Helpers
## ============================================================================

## Sanitize a value for safe use as a file-system path component
def _sanitizePathComponentForGrades(p1_rawValue, p2_fallback: str = "Unknown") -> str:
    rawText = str(p1_rawValue).strip() if p1_rawValue is not None else ""
    if not rawText:
        rawText = p2_fallback
    sanitized = re.sub(r'[<>:"/\\|?*]+', "_", rawText)
    sanitized = sanitized.replace("..", "_")
    sanitized = re.sub(r"\s+", " ", sanitized).strip().strip(".")
    return sanitized or p2_fallback


def _formatInstructorFullNamesForGrades(p1_instructorNames: list[str], p2_defaultName: str = "Unknown Instructor Name") -> str:
    fullNames = [
        str(name).strip()
        for name in p1_instructorNames
        if str(name).strip() and str(name).strip().lower() != "nan"
    ]
    uniqueFullNames = list(dict.fromkeys(fullNames))

    if not uniqueFullNames:
        return p2_defaultName
    if len(uniqueFullNames) == 1:
        return uniqueFullNames[0]
    return ", ".join(uniqueFullNames[:-1]) + f", and {uniqueFullNames[-1]}"


def _safeJoinUnderRootForGrades(p1_rootPath: str, *p2_components: str) -> str:
    absoluteRoot = os.path.abspath(p1_rootPath)
    candidatePath = os.path.abspath(os.path.join(absoluteRoot, *p2_components))
    if not candidatePath.startswith(absoluteRoot + os.sep):
        raise ValueError(f"Unsafe output path resolved outside root: {candidatePath}")
    return candidatePath


def _uniqueAssignmentColumnNamesForGrades(p1_assignments: list[dict]) -> list[str]:
    seen: dict[str, int] = {}
    columnNames: list[str] = []

    for assignment in p1_assignments:
        assignmentName = _sanitizePathComponentForGrades(
            assignment.get("name", ""),
            p2_fallback="Unnamed Assignment",
        )
        baseName = assignmentName

        if baseName in seen:
            seen[baseName] += 1
            assignmentName = f"{baseName}_{seen[baseName]}"
        else:
            seen[baseName] = 1

        columnNames.append(assignmentName)

    return columnNames


def _getCourseAssignmentsForGrades(p1_localSetup: LocalSetup, p2_courseId: str) -> list[dict]:
    """
    Retrieve published assignments for a Canvas course identified by SIS course ID.

    Args:
        p1_localSetup (LocalSetup): Local setup for API/logging context.
        p2_courseId (str): Canvas SIS course ID.

    Returns:
        list[dict]: Published assignment objects.
    """
    assignmentsUrl = f"{coreCanvasApiUrl}courses/sis_course_id:{p2_courseId}/assignments"
    apiCallResult = makeApiCall(p1_localSetup, p1_apiUrl=assignmentsUrl)
    if apiCallResult is None:
        return []
    assignmentsResponse, assignmentResponsePages = apiCallResult
    assignmentObjects = [assignmentsResponse]
    if assignmentResponsePages is not None and len(assignmentResponsePages) > 0:
        assignmentObjects.extend(assignmentResponsePages)
    assignmentList = flattenApiObjectToJsonList(p1_localSetup, assignmentObjects, assignmentsUrl)
    return [assignment for assignment in assignmentList if assignment.get("published") is True]


def _getAssignmentSubmissionsForGrades(p1_localSetup: LocalSetup, p2_courseId: str, p3_assignmentId) -> list[dict]:
    """
    Retrieve submissions for a single assignment in a Canvas SIS course.

    Args:
        p1_localSetup (LocalSetup): Local setup for API/logging context.
        p2_courseId (str): Canvas SIS course ID.
        p3_assignmentId: Canvas assignment ID.

    Returns:
        list[dict]: Submission objects for the assignment.
    """
    submissionsUrl = f"{coreCanvasApiUrl}courses/sis_course_id:{p2_courseId}/assignments/{p3_assignmentId}/submissions"
    submissionsPayload = {"include[]": ["user"]}
    apiCallResult = makeApiCall(
        p1_localSetup,
        p1_apiUrl=submissionsUrl,
        p1_payload=submissionsPayload,
    )
    if apiCallResult is None:
        return []
    submissionsResponse, submissionsPages = apiCallResult
    submissionObjects = [submissionsResponse]
    if submissionsPages is not None and len(submissionsPages) > 0:
        submissionObjects.extend(submissionsPages)
    return flattenApiObjectToJsonList(p1_localSetup, submissionObjects, submissionsUrl)


def _getAssignmentGroupsForGrades(p1_localSetup: LocalSetup, p2_courseId: str) -> list[dict]:
    assignmentGroupsUrl = f"{coreCanvasApiUrl}courses/sis_course_id:{p2_courseId}/assignment_groups"
    payload = {"include[]": ["assignments"]}
    apiCallResult = makeApiCall(
        p1_localSetup,
        p1_apiUrl=assignmentGroupsUrl,
        p1_payload=payload,
    )
    if apiCallResult is None:
        return []
    groupsResponse, groupPages = apiCallResult
    groupObjects = [groupsResponse]
    if groupPages is not None and len(groupPages) > 0:
        groupObjects.extend(groupPages)
    return flattenApiObjectToJsonList(p1_localSetup, groupObjects, assignmentGroupsUrl)


def _getCourseEnrollmentsForGrades(p1_localSetup: LocalSetup, p2_courseId: str) -> list[dict]:
    """
    Retrieve live student enrollments (with grades hash) for one Canvas SIS course.
    """
    enrollmentsUrl = f"{coreCanvasApiUrl}courses/sis_course_id:{p2_courseId}/enrollments"
    payload = {
        "type[]": ["StudentEnrollment"],
        "state[]": ["active", "completed", "concluded"],
    }

    apiCallResult = makeApiCall(
        p1_localSetup,
        p1_apiUrl=enrollmentsUrl,
        p1_payload=payload,
    )
    if apiCallResult is None:
        return []

    enrollmentsResponse, enrollmentPages = apiCallResult
    enrollmentObjects = [enrollmentsResponse]
    if enrollmentPages is not None and len(enrollmentPages) > 0:
        enrollmentObjects.extend(enrollmentPages)

    return flattenApiObjectToJsonList(p1_localSetup, enrollmentObjects, enrollmentsUrl)


def _getLiveEnrollmentGradesByCanvasUserIdForCourse(
    p1_localSetup: LocalSetup,
    p2_courseId: str,
) -> dict[str, dict]:
    """
    Build a lookup: canvas_user_id -> live enrollment grade fields from Canvas.
    """
    enrollments = _getCourseEnrollmentsForGrades(p1_localSetup, p2_courseId)
    gradeLookup: dict[str, dict] = {}

    for enrollment in enrollments:
        canvasUserId = str(enrollment.get("user_id", "")).strip()
        if not canvasUserId:
            continue

        grades = enrollment.get("grades", {}) or {}
        gradeLookup[canvasUserId] = {
            "current_score": grades.get("current_score", ""),
            "final_score": grades.get("final_score", ""),
            "current_grade": grades.get("current_grade", ""),
            "final_grade": grades.get("final_grade", ""),
            "unposted_current_score": grades.get("unposted_current_score", ""),
            "unposted_final_score": grades.get("unposted_final_score", ""),
            "unposted_current_grade": grades.get("unposted_current_grade", ""),
            "unposted_final_grade": grades.get("unposted_final_grade", ""),
        }

    return gradeLookup


def _buildCourseOutputPathForGrades(
    p1_localSetup: LocalSetup,
    p2_courseId: str,
    p3_courseAccountId: int | None,
    p4_instructorNames: list[str],
    p5_sisCourseRow,
    p6_accountsDf: pd.DataFrame,
) -> str:
    """
    Build a hierarchical output path for a course grade export file.

    Args:
        p1_localSetup (LocalSetup): Local setup object for path resolution.
        p2_courseId (str): Canvas SIS course ID.
        p3_courseAccountId (int | None): Canvas account ID for hierarchy lookup.
        p4_instructorNames (list[str]): Instructor names associated with the course.
        p5_sisCourseRow: SIS course row for term/course metadata.
        p6_accountsDf (pd.DataFrame): Canvas accounts dataframe used for hierarchy resolution.

    Returns:
        str: Final output folder path under the Canvas internal resources root.
    """
    rootOutputPath = os.path.join(p1_localSetup.getInternalResourcePaths("Canvas"), "Course_Grade_Exports")
    hierarchyComponents: list[str] = []

    if p3_courseAccountId is not None:
        structureDict = CanvasReport.determineCollegeDepartmentDiscipline(
            p1_localSetup,
            p3_courseAccountId,
            accountsDf=p6_accountsDf,
        )
        hierarchyComponents = [
            _sanitizePathComponentForGrades(component)
            for component in structureDict.get("Path_Components", [])
            if str(component).strip()
        ]

    instructorFolder = _sanitizePathComponentForGrades(
        _formatInstructorFullNamesForGrades(
            p4_instructorNames,
            p2_defaultName="Unknown Instructor Name",
        ),
        p2_fallback="Unknown Instructor Name",
    )

    return _safeJoinUnderRootForGrades(rootOutputPath, *hierarchyComponents, instructorFolder)


def _safeFloat(value, default=0.0):
    try:
        if value in [None, "", "nan", "None"]:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _pickGradeValue(primary, fallback):
    return primary if str(primary).strip() not in ["", "nan", "None"] else fallback


def _processSingleCourseGradeExport(
    p1_localSetup: LocalSetup,
    p2_errorHandler,
    p3_courseId: str,
    p4_mergedStudentEnrollmentsDf: pd.DataFrame,
    p5_canvasEnrollmentsDf: pd.DataFrame,
    p6_usersByCanvasId: pd.DataFrame,
    p7_sisCoursesByCourseId: pd.DataFrame,
    p8_canvasCoursesBySisId: pd.DataFrame,
    p9_accountsDf: pd.DataFrame,
):
    """
    Build and write one course grade export file.

    Args:
        p1_localSetup (LocalSetup): Local setup for logging and paths.
        p2_errorHandler: Error handler for exception reporting.
        p3_courseId (str): Target course SIS ID.
        p4_mergedStudentEnrollmentsDf (pd.DataFrame): Joined SIS/Canvas student enrollments.
        p5_canvasEnrollmentsDf (pd.DataFrame): Canvas enrollments dataframe.
        p6_usersByCanvasId (pd.DataFrame): Users lookup indexed by canvas_user_id.
        p7_sisCoursesByCourseId (pd.DataFrame): SIS courses indexed by course id.
        p8_canvasCoursesBySisId (pd.DataFrame): Canvas courses indexed by SIS course id.
        p9_accountsDf (pd.DataFrame): Accounts dataframe for hierarchy lookup.

    Returns:
        tuple[str, str | None]: course_id and output path (or None when skipped).
    """
    functionName = "_processSingleCourseGradeExport"
    try:
        p1_localSetup.logger.info(f"Processing course {p3_courseId}")

        ## Step 1: Isolate target course student enrollments
        courseStudentEnrollmentsDf = p4_mergedStudentEnrollmentsDf[
            p4_mergedStudentEnrollmentsDf["course_id"] == p3_courseId
        ].drop_duplicates(subset=["course_id", "user_id"])
        if courseStudentEnrollmentsDf.empty:
            return p3_courseId, None

        ## Step 1.5: Live Canvas grade enrichment lookup for this course
        liveGradeLookup = _getLiveEnrollmentGradesByCanvasUserIdForCourse(p1_localSetup, p3_courseId)

        ## Step 2: Gather assignments and assignment groups
        assignments = _getCourseAssignmentsForGrades(p1_localSetup, p3_courseId)
        assignments = sorted(assignments, key=lambda assignment: str(assignment.get("position", assignment.get("id", ""))))
        assignmentColumnNames = _uniqueAssignmentColumnNamesForGrades(assignments)

        assignmentGroups = _getAssignmentGroupsForGrades(p1_localSetup, p3_courseId)
        groupById = {}
        assignmentToGroup = {}
        for group in assignmentGroups:
            groupId = group.get("id")
            if groupId in [None, ""]:
                continue
            groupName = _sanitizePathComponentForGrades(group.get("name", ""), p2_fallback="Ungrouped")
            groupWeight = _safeFloat(group.get("group_weight", 0.0), 0.0)
            groupById[groupId] = {"name": groupName, "weight": groupWeight}
            for groupAssignment in group.get("assignments", []) or []:
                assignmentToGroup[groupAssignment.get("id")] = groupId

        if not assignmentColumnNames:
            p1_localSetup.logger.info(f"Skipping course {p3_courseId}: no published assignments found.")
            return p3_courseId, None

        assignmentColumnById = {
            assignment.get("id"): assignmentColumn
            for assignment, assignmentColumn in zip(assignments, assignmentColumnNames)
        }

        ## Step 3: Build starter student rows with live-enriched final grade columns
        studentRows = []
        for _, enrollmentRow in courseStudentEnrollmentsDf.iterrows():
            sisUserId = str(enrollmentRow["user_id"]).strip()
            canvasUserId = str(enrollmentRow.get("canvas_user_id", "")).strip()
            studentName = ""

            if isPresent(p6_usersByCanvasId) and canvasUserId and canvasUserId in p6_usersByCanvasId.index:
                userRow = p6_usersByCanvasId.loc[canvasUserId]
                if isinstance(userRow, pd.DataFrame):
                    userRow = userRow.iloc[0]
                studentName = str(userRow.get("full_name", "")).strip()

            liveGrades = liveGradeLookup.get(canvasUserId, {})

            currentScore = _pickGradeValue(
                liveGrades.get("current_score", ""),
                enrollmentRow.get("current_score", ""),
            )

            finalScore = _pickGradeValue(
                liveGrades.get("final_score", ""),
                enrollmentRow.get("final_score", ""),
            )

            currentGrade = _pickGradeValue(
                liveGrades.get("current_grade", ""),
                enrollmentRow.get("current_grade", ""),
            )

            finalGrade = _pickGradeValue(
                liveGrades.get("final_grade", ""),
                enrollmentRow.get("final_grade", ""),
            )

            rowData = {
                "course_id": p3_courseId,
                "student_sis_id": sisUserId,
                "student_canvas_id": canvasUserId,
                "student_name": studentName,

                "gradebook_current_total_percent": currentScore,
                "gradebook_final_total_percent": finalScore,
                "gradebook_current_total_grade": currentGrade,
                "gradebook_final_total_grade": finalGrade,

                "unposted_current_score": liveGrades.get("unposted_current_score", ""),
                "unposted_final_score": liveGrades.get("unposted_final_score", ""),
                "unposted_current_grade": liveGrades.get("unposted_current_grade", ""),
                "unposted_final_grade": liveGrades.get("unposted_final_grade", ""),
            }

            for assignmentColumn in assignmentColumnNames:
                rowData[assignmentColumn] = ""
            studentRows.append(rowData)

        outputDf = pd.DataFrame(studentRows)

        ## Step 4: Dynamic assignment group columns
        groupIdsInCourse = list(groupById.keys())
        groupPercentCol = {}

        for groupId in groupIdsInCourse:
            gName = groupById[groupId]["name"]
            percentCol = f"AG_{gName}_percent"
            groupPercentCol[groupId] = percentCol
            outputDf[percentCol] = ""

        rowIndexByCanvasUserId: dict[str, int] = {
            str(outputDf.at[idx, "student_canvas_id"]).strip(): int(idx)
            for idx in range(len(outputDf))
            if str(outputDf.at[idx, "student_canvas_id"]).strip()
        }

        ## Aggregation storage
        groupTotals = {}
        for canvasUserId in rowIndexByCanvasUserId.keys():
            groupTotals[canvasUserId] = {}
            for groupId in groupIdsInCourse:
                groupTotals[canvasUserId][groupId] = {"earned": 0.0, "possible": 0.0}

        ## Step 5: Populate assignment grades and group aggregates
        for assignment in assignments:
            assignmentId = assignment.get("id")
            if assignmentId in [None, ""]:
                continue
            assignmentColumn = assignmentColumnById.get(assignmentId)
            if not assignmentColumn:
                continue

            pointsPossible = _safeFloat(assignment.get("points_possible", 0.0), 0.0)
            groupId = assignmentToGroup.get(assignmentId)

            assignmentSubmissions = _getAssignmentSubmissionsForGrades(p1_localSetup, p3_courseId, assignmentId)
            for submission in assignmentSubmissions:
                submissionCanvasUserId = str(submission.get("user_id", "")).strip()
                if not submissionCanvasUserId or submissionCanvasUserId not in rowIndexByCanvasUserId:
                    continue

                scoreValue = submission.get("score")
                enteredScoreValue = submission.get("entered_score")
                gradeValue = scoreValue
                if gradeValue in [None, ""]:
                    gradeValue = enteredScoreValue
                if gradeValue in [None, ""]:
                    gradeValue = submission.get("grade")

                targetRowIndex = rowIndexByCanvasUserId[submissionCanvasUserId]
                outputDf.at[targetRowIndex, assignmentColumn] = gradeValue if gradeValue is not None else ""

                if groupId in groupById:
                    numericScore = _safeFloat(scoreValue, _safeFloat(enteredScoreValue, 0.0))
                    groupTotals[submissionCanvasUserId][groupId]["earned"] += numericScore
                    if pointsPossible > 0:
                        groupTotals[submissionCanvasUserId][groupId]["possible"] += pointsPossible

        ## Step 6: Write computed group totals to row columns
        for canvasUserId, rowIndex in rowIndexByCanvasUserId.items():
            for groupId in groupIdsInCourse:
                earned = groupTotals[canvasUserId][groupId]["earned"]
                possible = groupTotals[canvasUserId][groupId]["possible"]
                percent = (earned / possible) * 100.0 if possible > 0 else 0.0
                outputDf.at[rowIndex, groupPercentCol[groupId]] = round(percent, 4)

        ## Step 6.5: Remove unposted total columns when values are identical to gradebook totals
        gradebookToUnpostedColumnPairs = [
            ("gradebook_current_total_percent", "unposted_current_score"),
            ("gradebook_final_total_percent", "unposted_final_score"),
            ("gradebook_current_total_grade", "unposted_current_grade"),
            ("gradebook_final_total_grade", "unposted_final_grade"),
        ]

        requiredComparisonColumns = [
            columnName
            for columnPair in gradebookToUnpostedColumnPairs
            for columnName in columnPair
        ]

        if all(columnName in outputDf.columns for columnName in requiredComparisonColumns):
            areAllPairsIdentical = True

            for gradebookColumnName, unpostedColumnName in gradebookToUnpostedColumnPairs:
                gradebookSeries = outputDf[gradebookColumnName].fillna("").astype(str).str.strip()
                unpostedSeries = outputDf[unpostedColumnName].fillna("").astype(str).str.strip()
                if not gradebookSeries.equals(unpostedSeries):
                    areAllPairsIdentical = False
                    break

            if areAllPairsIdentical:
                outputDf.drop(
                    columns=[columnPair[1] for columnPair in gradebookToUnpostedColumnPairs],
                    inplace=True,
                    errors="ignore",
                )

        ## Step 7: Skip courses that have no populated grades and no final grades
        hasAssignmentGrades = outputDf[assignmentColumnNames].apply(
            lambda series: series.astype(str).str.strip().ne("")
        ).any().any()

        finalGradeCols = [
            "gradebook_current_total_percent",
            "gradebook_final_total_percent",
            "gradebook_current_total_grade",
            "gradebook_final_total_grade",
        ]
        hasFinalGrades = outputDf[finalGradeCols].apply(
            lambda series: series.astype(str).str.strip().ne("")
        ).any().any()

        if not hasAssignmentGrades and not hasFinalGrades:
            p1_localSetup.logger.info(f"Skipping course {p3_courseId}: no grades found.")
            return p3_courseId, None

        ## Step 8: Resolve instructor names for path metadata
        instructorNames = []
        courseInstructorEnrollmentsDf = p5_canvasEnrollmentsDf[
            (p5_canvasEnrollmentsDf["course_id"] == p3_courseId)
            & (p5_canvasEnrollmentsDf["role"] == "teacher")
            & (p5_canvasEnrollmentsDf["status"] != "deleted")
        ]
        if isPresent(courseInstructorEnrollmentsDf):
            for _, instructorEnrollment in courseInstructorEnrollmentsDf.iterrows():
                instructorCanvasId = str(instructorEnrollment.get("canvas_user_id", "")).strip()
                if isPresent(p6_usersByCanvasId) and instructorCanvasId and instructorCanvasId in p6_usersByCanvasId.index:
                    userRow = p6_usersByCanvasId.loc[instructorCanvasId]
                    if isinstance(userRow, pd.DataFrame):
                        userRow = userRow.iloc[0]
                    instructorName = str(userRow.get("full_name", "")).strip() or str(userRow.get("name", "")).strip()
                    if instructorName:
                        instructorNames.append(instructorName)

        ## Step 9: Resolve course/account metadata for folder path
        sisCourseRow = None
        if isPresent(p7_sisCoursesByCourseId) and p3_courseId in p7_sisCoursesByCourseId.index:
            sisCourseRow = p7_sisCoursesByCourseId.loc[p3_courseId]
            if isinstance(sisCourseRow, pd.DataFrame):
                sisCourseRow = sisCourseRow.iloc[0]

        courseAccountId: int | None = None
        if isPresent(p8_canvasCoursesBySisId) and p3_courseId in p8_canvasCoursesBySisId.index:
            canvasCourseRow = p8_canvasCoursesBySisId.loc[p3_courseId]
            if isinstance(canvasCourseRow, pd.DataFrame):
                canvasCourseRow = canvasCourseRow.iloc[0]
            rawAccountId = canvasCourseRow.get("canvas_account_id", "")
            if rawAccountId in ["", None]:
                rawAccountId = canvasCourseRow.get("account_id", "")
            try:
                courseAccountId = int(float(rawAccountId))
            except (TypeError, ValueError):
                courseAccountId = None

        ## Step 10: Write Excel output
        outputFolder = _buildCourseOutputPathForGrades(
            p1_localSetup=p1_localSetup,
            p2_courseId=p3_courseId,
            p3_courseAccountId=courseAccountId,
            p4_instructorNames=instructorNames,
            p5_sisCourseRow=sisCourseRow,
            p6_accountsDf=p9_accountsDf,
        )
        os.makedirs(outputFolder, exist_ok=True)

        outputFilePath = os.path.join(outputFolder, f"{_sanitizePathComponentForGrades(p3_courseId)}.xlsx")
        outputDf.to_excel(outputFilePath, index=False)
        return p3_courseId, outputFilePath

    except Exception as Error:
        p2_errorHandler.sendError(functionName, Error)
        return p3_courseId, None


def runCourseGradeExportsThreaded(
    p1_localSetup: LocalSetup,
    p1_errorHandler,
    p2_mergedStudentEnrollmentsDf: pd.DataFrame,
    p2_canvasEnrollmentsDf: pd.DataFrame,
    p2_usersByCanvasId: pd.DataFrame,
    p2_sisCoursesByCourseId: pd.DataFrame,
    p2_canvasCoursesBySisId: pd.DataFrame,
    p2_accountsDf: pd.DataFrame,
    p2_maxWorkers: int = 15,
) -> dict[str, str]:
    functionName = "runCourseGradeExportsThreaded"
    outputFilesByCourseId: dict[str, str] = {}

    try:
        uniqueCourseIds = sorted(
            p2_mergedStudentEnrollmentsDf["course_id"]
            .dropna()
            .astype(str)
            .str.strip()
            .unique()
            .tolist()
        )

        if not uniqueCourseIds:
            return outputFilesByCourseId

        with ThreadPoolExecutor(max_workers=p2_maxWorkers) as executor:
            futureByCourseId = {
                executor.submit(
                    _processSingleCourseGradeExport,
                    p1_localSetup,
                    p1_errorHandler,
                    courseId,
                    p2_mergedStudentEnrollmentsDf,
                    p2_canvasEnrollmentsDf,
                    p2_usersByCanvasId,
                    p2_sisCoursesByCourseId,
                    p2_canvasCoursesBySisId,
                    p2_accountsDf,
                ): courseId
                for courseId in uniqueCourseIds
            }

            for future in as_completed(futureByCourseId):
                courseId = futureByCourseId[future]
                try:
                    resultCourseId, outputFilePath = future.result()
                    if outputFilePath:
                        outputFilesByCourseId[resultCourseId] = outputFilePath
                except Exception as Error:
                    p1_localSetup.logger.error(f"{functionName}: Failed for course {courseId}: {Error}")

        return outputFilesByCourseId

    except Exception as Error:
        p1_errorHandler.sendError(functionName, Error)
        return outputFilesByCourseId


## ══════════════════════════════════════════════════════════════════════════════
## Canvas Action Helpers
## Shared primitives used by multiple ActionModule scripts so the
## per-script duplicates can be removed.
## ══════════════════════════════════════════════════════════════════════════════

## Update a single field on a Canvas course via the Courses API
def updateCourseField(
    localSetup: LocalSetup,
    errorHandler,
    courseId: str,
    fieldName: str,
    fieldValue,
) -> bool:
    """
    Set one field on a Canvas course with PUT /api/v1/courses/:id.

    Args:
        localSetup:   LocalSetup instance for logging and API auth.
        errorHandler: errorEmail instance for error reporting.
        courseId:     Canvas numeric course ID (string).
        fieldName:    Course field key (e.g. "name", "account_id", "enrollment_term_id").
        fieldValue:   Value to assign; caller is responsible for the correct Python type.

    Returns:
        True on HTTP 200, False otherwise.
    """
    functionName = "updateCourseField"
    try:
        updateUrl = f"{coreCanvasApiUrl}courses/{courseId}"
        payload = {"course": {fieldName: fieldValue}}
        response, _ = makeApiCall(
            localSetup=localSetup,
            p1_apiUrl=updateUrl,
            p1_payload=payload,
            p1_apiCallType="put",
        )
        statusCode = getattr(response, "status_code", None)
        if statusCode == 200:
            localSetup.logInfoThreadSafe(
                f"Successfully set {fieldName}={fieldValue!r} for course {courseId}"
            )
            return True
        localSetup.logWarningThreadSafe(
            f"Failed to set {fieldName} for course {courseId}. Status code: {statusCode}"
        )
        return False
    except Exception as Error:
        errorHandler.sendError(functionName, Error)
        return False


## Delete a Canvas course via the Courses API
def deleteCourse(
    localSetup: LocalSetup,
    errorHandler,
    courseId: str,
) -> bool:
    """
    Delete a Canvas course with DELETE /api/v1/courses/:id.

    Args:
        localSetup:   LocalSetup instance for logging and API auth.
        errorHandler: errorEmail instance for error reporting.
        courseId:     Canvas numeric course ID (string).

    Returns:
        True on HTTP 200, False otherwise.
    """
    functionName = "deleteCourse"
    try:
        deleteUrl = f"{coreCanvasApiUrl}courses/{courseId}"
        payload = {"event": "delete"}
        response, _ = makeApiCall(
            localSetup=localSetup,
            p1_apiUrl=deleteUrl,
            p1_payload=payload,
            p1_apiCallType="delete",
        )
        statusCode = getattr(response, "status_code", None)
        if statusCode == 200:
            localSetup.logInfoThreadSafe(f"Successfully deleted course {courseId}")
            return True
        localSetup.logWarningThreadSafe(
            f"Failed to delete course {courseId}. Status code: {statusCode}"
        )
        return False
    except Exception as Error:
        errorHandler.sendError(functionName, Error)
        return False


## Delete a Canvas enrollment via the Enrollments API
def deleteEnrollment(
    localSetup: LocalSetup,
    errorHandler,
    courseId: str,
    enrollmentId: str,
) -> bool:
    """
    Delete a Canvas enrollment with DELETE /api/v1/courses/:course_id/enrollments/:id.

    Args:
        localSetup:    LocalSetup instance for logging and API auth.
        errorHandler:  errorEmail instance for error reporting.
        courseId:      Canvas numeric course ID (string).
        enrollmentId:  Canvas numeric enrollment ID (string).

    Returns:
        True on HTTP 200, False otherwise.
    """
    functionName = "deleteEnrollment"
    try:
        deleteUrl = f"{coreCanvasApiUrl}courses/{courseId}/enrollments/{enrollmentId}"
        response, _ = makeApiCall(
            localSetup=localSetup,
            p1_apiUrl=deleteUrl,
            p1_apiCallType="delete",
        )
        statusCode = getattr(response, "status_code", None)
        if statusCode == 200:
            localSetup.logInfoThreadSafe(
                f"Successfully deleted enrollment {enrollmentId} from course {courseId}"
            )
            return True
        localSetup.logWarningThreadSafe(
            f"Failed to delete enrollment {enrollmentId} from course {courseId}. "
            f"Status code: {statusCode}"
        )
        return False
    except Exception as Error:
        errorHandler.sendError(functionName, Error)
        return False


## Enroll a user in a Canvas course via the Enrollments API
def enrollUser(
    localSetup: LocalSetup,
    errorHandler,
    courseId: str,
    userId: str,
    enrollmentType: str,
    roleId: str = None,
    enrollmentState: str = "active",
) -> bool:
    """
    Enroll a user in a Canvas course with POST /api/v1/courses/:id/enrollments.

    Args:
        localSetup:      LocalSetup instance for logging and API auth.
        errorHandler:    errorEmail instance for error reporting.
        courseId:        Canvas numeric course ID (string).
        userId:          Canvas numeric user ID (string).
        enrollmentType:  Base role type (e.g. "StudentEnrollment", "TeacherEnrollment").
        roleId:          Optional Canvas role ID for a custom role override.
        enrollmentState: Enrollment state; defaults to "active".

    Returns:
        True on HTTP 200, False otherwise.
    """
    functionName = "enrollUser"
    try:
        enrollUrl = f"{coreCanvasApiUrl}courses/{courseId}/enrollments"
        payload = {
            "enrollment[user_id]": userId,
            "enrollment[type]": enrollmentType,
            "enrollment[enrollment_state]": enrollmentState,
        }
        if roleId:
            payload["enrollment[role_id]"] = roleId
        response, _ = makeApiCall(
            localSetup=localSetup,
            p1_apiUrl=enrollUrl,
            p1_payload=payload,
            p1_apiCallType="post",
        )
        statusCode = getattr(response, "status_code", None)
        if statusCode == 200:
            localSetup.logInfoThreadSafe(
                f"Successfully enrolled user {userId} in course {courseId} as {enrollmentType}"
            )
            return True
        localSetup.logWarningThreadSafe(
            f"Failed to enroll user {userId} in course {courseId}. Status code: {statusCode}"
        )
        return False
    except Exception as Error:
        errorHandler.sendError(functionName, Error)
        return False

def _generateSecureTempPassword(length: int = 20) -> str:
    """
    Generate a strong temporary password.
    """
    alphabet = string.ascii_letters + string.digits + "!@#$%^&*()-_=+"
    chars = [
        secrets.choice(string.ascii_lowercase),
        secrets.choice(string.ascii_uppercase),
        secrets.choice(string.digits),
        secrets.choice("!@#$%^&*()-_=+"),
    ]
    chars.extend(secrets.choice(alphabet) for _ in range(max(0, length - 4)))
    secrets.SystemRandom().shuffle(chars)
    return "".join(chars)


def terminateAllUserSessions(
    localSetup: LocalSetup,
    errorHandler,
    userId: str,
) -> bool:
    """
    Terminate all active Canvas sessions/tokens for a user.

    Uses DELETE /api/v1/users/:user_id/sessions.

    Args:
        localSetup (LocalSetup): LocalSetup instance for logging and API auth.
        errorHandler: errorEmail instance for error reporting.
        userId (str): Canvas numeric user ID as a string.

    Returns:
        bool: True on HTTP 200/204, False otherwise.
    """
    functionName = "terminateAllUserSessions"
    try:
        ## Step 1: Normalize and validate the user ID
        userId = str(userId).replace(".0", "").strip()
        if not userId:
            localSetup.logWarningThreadSafe(f"{functionName}: Missing userId")
            return False

        ## Step 2: Call Canvas sessions endpoint to terminate active sessions
        terminateUrl = f"{coreCanvasApiUrl}users/{userId}/sessions"
        response, _ = makeApiCall(
            localSetup=localSetup,
            p1_apiUrl=terminateUrl,
            p1_apiCallType="delete",
        )

        statusCode = getattr(response, "status_code", None)
        ## Canvas may return 200/204 depending on endpoint behavior
        if statusCode in [200, 204]:
            localSetup.logInfoThreadSafe(
                f"{functionName}: Terminated all sessions for user {userId}"
            )
            return True

        localSetup.logWarningThreadSafe(
            f"{functionName}: Failed to terminate sessions for user {userId}. "
            f"Status code: {statusCode}"
        )
        return False

    except Exception as Error:
        errorHandler.sendError(functionName, Error)
        return False


## Require a Canvas user to reset password by:
## 1) changing login password, 2) ending sessions, 3) kicking off password recovery
def requirePasswordReset(
    localSetup: LocalSetup,
    errorHandler,
    userId: str,
) -> bool:
    """
    Require Canvas password reset flow for a user.

    The function:
        1. Normalizes/validates user ID.
        2. Retrieves user logins/pseudonyms.
        3. Selects a target login (prefers active).
        4. Sets a secure temporary password via:
           PUT /api/v1/accounts/:account_id/logins/:id
        5. Terminates all sessions via:
           DELETE /api/v1/users/:user_id/sessions
        6. Triggers recovery email via:
           POST /api/v1/users/reset_password

    Returns:
        bool: True if all required steps succeed, False otherwise.
    """
    functionName = "requirePasswordReset"
    try:
        ## Step 1: Normalize and validate the user ID
        userId = str(userId).replace(".0", "").strip()
        if not userId:
            localSetup.logWarningThreadSafe(f"{functionName}: Missing userId")
            return False

        ## Step 2: Retrieve user logins/pseudonyms
        getLoginsUrl = f"{coreCanvasApiUrl}users/{userId}/logins"
        loginsResponse, _ = makeApiCall(
            localSetup=localSetup,
            p1_apiUrl=getLoginsUrl,
            p1_apiCallType="get",
        )
        if getattr(loginsResponse, "status_code", None) != 200:
            localSetup.logWarningThreadSafe(
                f"{functionName}: Failed to get logins for user {userId}. "
                f"Status code: {getattr(loginsResponse, 'status_code', None)}"
            )
            return False

        logins = loginsResponse.json() or []
        if not logins:
            localSetup.logWarningThreadSafe(
                f"{functionName}: No logins found for user {userId}"
            )
            return False

        ## Step 3: Select login (prefer active; fallback first)
        selectedLogin = next(
            (l for l in logins if l.get("workflow_state") == "active"),
            logins[0],
        )

        pseudonymId = selectedLogin.get("id")
        accountId = selectedLogin.get("account_id")
        identifier = (selectedLogin.get("unique_id") or "").strip()
        validEmailPattern = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
        ## If identifier is missing or not a valid email, pull fallback values from users df
        if not identifier or not re.match(validEmailPattern, identifier):
            usersDf = CanvasReport.getUsersDf(localSetup)
            if isPresent(usersDf) and "canvas_user_id" in usersDf.columns:
                matchingUsersDf = usersDf[
                    usersDf["canvas_user_id"].astype(str).str.strip() == str(userId).strip()
                ]

                if isPresent(matchingUsersDf):
                    matchingUserRow = matchingUsersDf.iloc[0]
                    fallbackCandidates = [
                        str(matchingUserRow.get("email", "")).strip(),
                        str(matchingUserRow.get("login_id", "")).strip(),
                        str(matchingUserRow.get("unique_id", "")).strip(),
                    ]
                    for candidate in fallbackCandidates:
                        if candidate and re.match(validEmailPattern, candidate):
                            identifier = candidate
                            break
            if not identifier or not re.match(validEmailPattern, identifier):
                localSetup.logWarningThreadSafe(
                    f"{functionName}: Could not retrieve a valid fallback email from users df for user {userId}"
                )


        if not pseudonymId:
            localSetup.logWarningThreadSafe(
                f"{functionName}: Could not determine pseudonym ID for user {userId}"
            )
            return False
        if not accountId:
            localSetup.logWarningThreadSafe(
                f"{functionName}: Could not determine account ID for user {userId}"
            )
            return False
        if not identifier or not re.match(validEmailPattern, identifier):
            localSetup.logWarningThreadSafe(
                f"{functionName}: Could not determine a valid email identifier for user {userId}"
            )
            return False

        ## Step 4: Set temporary password (invalidates old password)
        tempPassword = _generateSecureTempPassword()
        updateLoginUrl = f"{coreCanvasApiUrl}accounts/{accountId}/logins/{pseudonymId}"
        updatePayload = {
            "login": {
                "password": tempPassword
            }
        }

        updateResponse, _ = makeApiCall(
            localSetup=localSetup,
            p1_apiUrl=updateLoginUrl,
            p1_payload=updatePayload,
            p1_apiCallType="put",
        )
        updateStatus = getattr(updateResponse, "status_code", None)
        if updateStatus != 200:
            localSetup.logWarningThreadSafe(
                f"{functionName}: Failed to change password for user {userId}. "
                f"Status code: {updateStatus}"
            )
            return False

        ## Step 5: End all active sessions/tokens
        sessionsTerminated = terminateAllUserSessions(
            localSetup=localSetup,
            errorHandler=errorHandler,
            userId=userId,
        )
        if not sessionsTerminated:
            localSetup.logWarningThreadSafe(
                f"{functionName}: Password changed but failed to terminate sessions for user {userId}"
            )
            return False

        ## Step 6: Kick off password recovery flow
        resetPasswordUrl = f"{coreCanvasApiUrl}users/reset_password"
        resetPayload = {"email": identifier}

        resetResponse, _ = makeApiCall(
            localSetup=localSetup,
            p1_apiUrl=resetPasswordUrl,
            p1_payload=resetPayload,
            p1_apiCallType="post",
        )
        resetStatus = getattr(resetResponse, "status_code", None)

        resetJson = {}
        try:
            resetJson = resetResponse.json() if resetResponse is not None else {}
        except Exception:
            resetJson = {}

        if resetStatus in [200, 201] and resetJson.get("requested") is True:
            localSetup.logInfoThreadSafe(
                f"{functionName}: Completed password reset flow for user {userId} "
                f"(login {pseudonymId})"
            )
            return True

        localSetup.logWarningThreadSafe(
            f"{functionName}: Password changed and sessions terminated, but recovery kickoff failed "
            f"for user {userId}. Status code: {resetStatus}, response: {resetJson}"
        )
        return False

    except Exception as Error:
        errorHandler.sendError(functionName, Error)
        return False
    
def kickoffPasswordRecovery(self, userIdentifier):
    """
    Kick off Canvas forgot-password flow for a user identifier (usually email / login unique_id).

    Canvas endpoint:
      POST /api/v1/users/reset_password

    Returns:
      (responseObject, responseJsonOrList)
    """
    functionName = "kickoffPasswordRecovery"

    if not userIdentifier:
        raise Exception(f"{functionName}: userIdentifier is required")

    # Keep this consistent with your existing URL construction style
    resetPasswordUrl = f"{self.localSetup.canvasBaseUrl}/api/v1/users/reset_password"

    # Canvas forgot_password expects email in API mode
    payload = {
        "email": userIdentifier
    }

    # Reuse your existing API wrapper
    resetResponse, resetJson = makeApiCall(
        localSetup=self.localSetup,
        p1_apiUrl=resetPasswordUrl,
        p1_payload=payload,
        p1_apiCallType="post"
    )

    # Optional sanity check
    # Expected JSON shape is typically: {"requested": true}
    if isinstance(resetJson, dict) and resetJson.get("requested") is True:
        return resetResponse, resetJson

    # If Canvas returns something unexpected, still return it for caller handling
    return resetResponse, resetJson

## Read a target CSV and validate that required columns are present
## ══════════════════════════════════════════════════════════════════════════════
## Threading / CSV helpers — canonical definitions live in TLC_Common.
## Re-exported here for backward compatibility with existing action-module imports.
## ══════════════════════════════════════════════════════════════════════════════
## readTargetCsv and runThreadedRows are imported from TLC_Common above and are
## available on this module for any code that imports them from TLC_Action.
