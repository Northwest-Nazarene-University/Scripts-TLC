# Author: Bryce Miller - brycezmiller@nnu.edu
# Last Updated by: Bryce Miller

## Import Generic Moduels

import os, sys, time, re, pandas as pd, paramiko
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlparse

# Ensure ResourceModules are importable when running from ActionModules
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

try: ## Irregular try clause, do not comment out in testing
    from Local_Setup import LocalSetup
    from Canvas_Report import CanvasReport
    from Error_Email import errorEmail
    from TLC_Common import isPresent, downloadFile, makeApiCall, getEncryptionKey
    from Add_Outcomes_to_Active_Courses import (
        retrieveDataForRelevantCommunication,
        getUniqueOutcomesAndOutcomeCoursesDict,
        removeMissingOutcomes,
        addOutcomeToCourse,
    )
except ImportError:
    # Fallback to relative imports if package layout differs
    from ResourceModules.Local_Setup import LocalSetup
    from ResourceModules.Canvas_Report import CanvasReport
    from ResourceModules.Error_Email import errorEmail
    from ResourceModules.TLC_Common import isPresent, downloadFile, makeApiCall, getEncryptionKey
    from ActionModules.Add_Outcomes_to_Active_Courses import (
            retrieveDataForRelevantCommunication,
            getUniqueOutcomesAndOutcomeCoursesDict,
            removeMissingOutcomes,
            addOutcomeToCourse,
        )

## Get catalogToSimpleSyllabusConfig from configs
from Common_Configs import (
    catalogToSimpleSyllabusConfig,
    undgTermsWordsToCodesDict,
    undgTermsCodesToWordsDict,
    gradTermsWordsToCodesDict,
    gradTermsCodesToWordsDict,
)

## Cryptography import for Fernet encryption (same pattern as Core_Microsoft_Api.py)
from cryptography.fernet import Fernet

## Set working directory
os.chdir(os.path.dirname(__file__))

# Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = os.path.basename(__file__)

scriptPurpose = r"""
This script determines what course date related actions need to be taken for a specific term, such as sending outcome related emails to instructors, and performs those actions.
"""
externalRequirements = r"""
This script requires the following external resources:
1. Access to the Canvas API for retrieving course and instructor data.
2. Access to the email system for sending outcome related emails to instructors.
3. The ResourceModules and ActionModules directories in the Scripts TLC directory for additional functionality.
4. Access to the Simple Syllabus SFTP server via SSH private key authentication.
5. The SSH private key file and its password stored in the config path.
"""

## Initialize LocalSetup and helpers from ResourceModules
localSetup = LocalSetup(datetime.now(), __file__)
errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)


## ══════════════════════════════════════════════════════════════════════════════
## SFTP Private Key Password Management
## ══════════════════════════════════════════════════════════════════════════════

## This function retrieves the Simple Syllabus SFTP private key password.
## On first run it reads the plaintext password from SSPrivKP.txt, encrypts it
## using the repository's Fernet encryption setup (same pattern as Core_Microsoft_Api.py),
## saves the encrypted version as SSPrivKP_Encrypted.txt, and deletes the plaintext file.
## On subsequent runs it reads and decrypts the encrypted file.
def _getSimpleSyllabusPrivateKeyPassword():
    """
    Retrieve the SSH private key password for Simple Syllabus SFTP.
    Returns None if the key has no passphrase (neither password file exists).
    """
    functionName = "_getSimpleSyllabusPrivateKeyPassword"

    plaintextPasswordPath = os.path.join(localSetup.configPath, "SSPrivKP.txt")
    encryptedPasswordPath = os.path.join(localSetup.configPath, "SSPrivKP_Encrypted.txt")

    ## Case 1: Encrypted file exists — decrypt and return
    if os.path.exists(encryptedPasswordPath):
        encryptionKey = getEncryptionKey(localSetup)
        fernet = Fernet(encryptionKey)
        localSetup.logger.info(f"{functionName}: Reading encrypted private key password from {encryptedPasswordPath}")
        with open(encryptedPasswordPath, "r") as encFile:
            encryptedContent = encFile.read().strip()
        decryptedPassword = fernet.decrypt(encryptedContent.encode()).decode()
        return decryptedPassword if decryptedPassword else None

    ## Case 2: Plaintext file exists — encrypt, save, delete plaintext, return password
    if os.path.exists(plaintextPasswordPath):
        encryptionKey = getEncryptionKey(localSetup)
        fernet = Fernet(encryptionKey)
        localSetup.logger.info(f"{functionName}: Found plaintext password at {plaintextPasswordPath}. Encrypting...")
        with open(plaintextPasswordPath, "r") as ptFile:
            plaintextPassword = ptFile.read().strip()
        if not plaintextPassword:
            ## Empty file means no passphrase — delete and treat as no password
            os.remove(plaintextPasswordPath)
            localSetup.logger.info(f"{functionName}: Plaintext password file was empty; treating as no passphrase")
            return None
        encryptedData = fernet.encrypt(plaintextPassword.encode())
        with open(encryptedPasswordPath, "w") as encFile:
            encFile.write(encryptedData.decode())
        os.remove(plaintextPasswordPath)
        localSetup.logger.info(f"{functionName}: Encrypted and deleted plaintext password file")
        return plaintextPassword

    ## Case 3: Neither file exists — key has no passphrase
    localSetup.logger.info(f"{functionName}: No password file found; assuming key has no passphrase")
    return None


## ══════════════════════════════════════════════════════════════════════════════
## SFTP Upload Function
## ══════════════════════════════════════════════════════════════════════════════

## This function uploads the processed Course Extract CSV to Simple Syllabus via SFTP
def uploadToSimpleSyllabus(p1_filePath: str):
    """
    Uploads the Course Extract CSV file to the Simple Syllabus SFTP server.

    The function:
        1. Validates the local file exists and is a CSV.
        2. Reads the CSV to sanitize headers per Simple Syllabus EXT-CSV requirements:
           - Only alphanumeric, underscores, forward slashes, and dashes allowed.
           - No special characters, parentheses, colons, leading/trailing spaces, or blank headers.
        3. Re-saves the sanitized CSV with UTF-8 encoding (no BOM).
        4. Retrieves the SSH private key password (encrypted via Fernet).
        5. Connects to the Simple Syllabus SFTP server using SSH private key authentication.
        6. Uploads the file to the configured remote /imports directory.
        7. Closes the SFTP and SSH connections.
        8. On successful upload, writes a success tag file next to the Combined Catalog CSV
           so that subsequent runs can detect whether anything has changed since last upload.

    Args:
        p1_filePath (str): The local file path to the Course Extract CSV to upload.

    Raises:
        FileNotFoundError: If the local file or SSH key does not exist.
        Exception: If the SFTP connection or upload fails after all retries.
    """

    functionName = "uploadToSimpleSyllabus"

    try:
        localSetup.logger.info(f"{functionName}: Starting upload of {p1_filePath} to Simple Syllabus SFTP")

        ## ── Validate the local file ──
        if not os.path.exists(p1_filePath):
            raise FileNotFoundError(f"{functionName}: File not found: {p1_filePath}")

        ## ── Sanitize CSV headers per Simple Syllabus EXT-CSV requirements ──
        ## Acceptable: A-Z, a-z, 0-9, underscores, forward slashes, dashes
        ## Unacceptable: special chars (@#$%&* etc), parentheses, colons, leading/trailing spaces, blank headers
        sanitizedDf = _readCsvWithEncoding(p1_filePath)

        sanitizedHeaders = []
        for header in sanitizedDf.columns:
            ## Strip leading/trailing whitespace
            cleaned = str(header).strip()
            ## Replace unacceptable characters with underscores
            ## Keep: alphanumeric, underscores, forward slashes, dashes, spaces (spaces between words are OK in CSV headers)
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
        localSetup.logger.info(f"{functionName}: Sanitized CSV headers and re-saved to {p1_filePath}")

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
        ## Key file is SimpSylSSH.txt (no passphrase)
        privateKeyPath = os.path.join(localSetup.configPath, "SimpSylSSH.txt")
        if not os.path.exists(privateKeyPath):
            raise FileNotFoundError(
                f"{functionName}: SSH private key not found at {privateKeyPath}. "
                f"Please place 'SimpSylSSH.txt' in the config directory."
            )

        localSetup.logger.info(f"{functionName}: Using SSH private key at {privateKeyPath}")

        ## ── Retrieve the private key password (None if no passphrase) ──
        privateKeyPassword = _getSimpleSyllabusPrivateKeyPassword()

        ## ── Normalize password: encode str -> bytes for paramiko 4.0, or None ──
        ## paramiko 4.0 PKey.from_path() requires passphrase as bytes or None
        normalizedPassword = privateKeyPassword.encode("utf-8") if privateKeyPassword else None

        ## ── Load the private key ──
        ## paramiko 4.0+: PKey.from_path() auto-detects key type (Ed25519, RSA, etc.)
        ## and handles OpenSSH format. passphrase must be bytes or None.
        privateKey = None
        pwdAttempts = [normalizedPassword, None] if normalizedPassword is not None else [None]
        for pwd in pwdAttempts:
            try:
                privateKey = paramiko.PKey.from_path(privateKeyPath, passphrase=pwd)
                logMsg = "with passphrase" if pwd else "no passphrase"
                localSetup.logger.info(f"{functionName}: SSH private key loaded successfully ({logMsg})")
                break
            except paramiko.ssh_exception.PasswordRequiredException:
                localSetup.logger.error(f"{functionName}: Key requires a passphrase but none was provided")
                break
            except (paramiko.ssh_exception.SSHException, ValueError) as e:
                localSetup.logger.warning(f"{functionName}: Key load attempt failed (pwd={'set' if pwd else 'None'}): {e}")
                continue

        if privateKey is None:
            raise ValueError(
                f"{functionName}: Could not load SSH private key from {privateKeyPath}. "
                f"Verify the key is a valid OpenSSH format key (Ed25519/RSA/ECDSA)."
            )
        localSetup.logger.info(f"{functionName}: SSH private key loaded successfully")

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
                localSetup.logger.info(f"{functionName}: Successfully connected to SFTP server {sftpHost}:{sftpPort}")
                break

            except Exception as connError:
                attempt += 1
                if attempt < retries:
                    localSetup.logger.warning(
                        f"{functionName}: Attempt {attempt} failed: {connError}. Retrying in 1 minute..."
                    )
                    time.sleep(60)
                else:
                    localSetup.logger.error(
                        f"{functionName}: Attempt {attempt} failed: {connError}. No more retries."
                    )
                    errorHandler.sendError(functionName, p1_errorInfo=connError)
                    raise

        ## ── Open SFTP session and upload the file ──
        sftp_client = ssh_client.open_sftp()

        try:
            ## Build the remote file path
            localFileName = os.path.basename(p1_filePath)
            remoteFilePath = f"{sftpRemoteDir}/{localFileName}"

            localSetup.logger.info(f"{functionName}: Uploading {p1_filePath} to {remoteFilePath}")
            sftp_client.put(p1_filePath, remoteFilePath)
            localSetup.logger.info(f"{functionName}: File uploaded successfully to {remoteFilePath}")

        finally:
            ## Close the SFTP client and SSH connection
            sftp_client.close()
            ssh_client.close()
            localSetup.logger.info(f"{functionName}: SFTP and SSH connections closed")

        ## ── Tag the Combined Catalog CSV with a success marker ──
        ## Write a sentinel file next to the Combined Catalog CSV at the catalog root path
        ## so that retrieveCatalogCourseReportsDfs can detect if anything changed since last successful upload
        catalogDir = os.path.dirname(p1_filePath)
        successTagPath = os.path.join(catalogDir, "Combined Catalog Course Report_UPLOAD_SUCCESS.txt")
        with open(successTagPath, "w", encoding="utf-8") as tagFile:
            tagFile.write(f"Upload successful at {datetime.now().isoformat()}\n")
            tagFile.write(f"Uploaded file: {p1_filePath}\n")
        localSetup.logger.info(f"{functionName}: Success tag written to {successTagPath}")

    except Exception as Error:
        localSetup.logger.error(f"{functionName}: {Error}")
        raise


## ══════════════════════════════════════════════════════════════════════════════
## CSV Helpers
## ══════════════════════════════════════════════════════════════════════════════

## Helper function to read CSV with encoding fallback
def _readCsvWithEncoding(filePath: str, **kwargs) -> pd.DataFrame:
    """Read a CSV file trying utf-8-sig first, then latin-1 as fallback."""
    try:
        return pd.read_csv(filePath, encoding='utf-8-sig', **kwargs)
    except (UnicodeDecodeError, UnicodeError):
        localSetup.logger.warning(f"UTF-8 decode failed for {filePath}, falling back to latin-1")
        return pd.read_csv(filePath, encoding='latin-1', **kwargs)

## This function normalizes spacing and delimiters in prerequisite/corequisite strings from the catalog, which often have inconsistent formatting issues.
def _normalizeRequisiteSpacing(text: str) -> str:
    """
    Fix spacing issues and replace uppercase AND / OR delimiters with commas
    in prerequisite/corequisite strings from the catalog.

    The catalog data often jams separate requirements together with uppercase AND/OR
    instead of proper punctuation. This function:
      1. Splits apart words jammed together at lowercase→uppercase boundaries
         (e.g. 'permissionAdmission' -> 'permission, Admission')
      2. Inserts a space after periods jammed against a capital letter
         (e.g. 'program.COMP3480' -> 'program. COMP3480')
      3. Does NOT touch lowercase 'or'/'and' inside phrases like 'or instructor's approval'

    Examples:
        'Completion of COMP2750 or COMP6120 AND:Admission into BSU's accelerated
         Master of Science degree program OR Instructor permission.'
        ->
        'Completion of COMP2750 or COMP6120, Admission into BSU's accelerated
         Master of Science degree program, Instructor permission.'

        'Instructor permissionAdmission into BSU's accelerated Master of Science
         degree program.COMP3480 (or taken concurrently)'
        ->
        'Instructor permission, Admission into BSU's accelerated Master of Science
         degree program. COMP3480 (or taken concurrently)'

        'ARDE2430, ARDE2760, or instructor's approval.' -> unchanged
    """
    if not text:
        return text

    ## Step 1: Split apart words jammed at lowercase→Uppercase boundary
    ## e.g. 'permissionAdmission' -> 'permission, Admission'
    ## This catches cases where the catalog data has no delimiter at all
    text = re.sub(r'([a-z])([A-Z][a-z])', r'\1, \2', text)

    ## Step 2: Insert space after period jammed against a capital letter
    ## e.g. 'program.COMP3480' -> 'program. COMP3480'
    text = re.sub(r'\.([A-Z])', r'. \1', text)

    ## Step 3: Clean up artifacts
    ## Collapse multiple commas into one
    text = re.sub(r',\s*,+', ',', text)
    ## Remove comma right after a period  (". ," -> ".")
    text = re.sub(r'\.\s*,', '.', text)
    ## Remove comma right before a period  (", ." -> ".")
    text = re.sub(r',\s*\.', '.', text)
    ## Collapse multiple spaces
    text = re.sub(r'\s+', ' ', text).strip()
    ## Clean leading/trailing commas
    text = text.strip(',').strip()

    return text


## This function formats the combined catalog course report df into the format needed for Simple Syllabus and saves as a new CSV
def formatCombinedCatalogForSimpleSyllabus(p1_combinedCatalogDf: pd.DataFrame, p1_catalogSchoolYear: str) -> pd.DataFrame:
    """
    Formats the combined catalog course report DataFrame into the Simple Syllabus Course Extract format.
    
    Steps:
        1. Determine whether the catalog school year is current or next, and get the appropriate term codes.
        2. Retrieve the Canvas terms DataFrame to map term codes to human-readable term names.
        3. Load the Simple Syllabus Organizations CSV and the Canvas accounts hierarchy to build
           a Parent Organization lookup for each course.
        4. For each catalog course, determine which terms it belongs to (undergrad vs graduate).
        5. Expand each course into one row per applicable term, only if the course exists in Canvas for that term.
        6. Split 'Title' into 'Subject' (first 4 chars) and 'Course Number' (remainder).
        7. Combine prerequisite and corequisite columns per the specified rules.
        8. Resolve 'Parent Organization' by walking up the Canvas account hierarchy until a
           Simple Syllabus Organization match is found.
        9. Save the result as a new CSV.

    Args:
        p1_combinedCatalogDf (pd.DataFrame): The combined catalog DataFrame with columns:
            Title, Name, Class Program, Description, Credits, Prerequisites, Corequisites,
            Prerequisite Courses, Corequisite Courses, Concurrent, Concurrent Requisite,
            Catalog Type, Recommended Prerequisites, Prerequisite or Corequisite
        p1_catalogSchoolYear (str): The catalog school year string, e.g. "2025-2026".

    Returns:
        pd.DataFrame: The formatted DataFrame in Simple Syllabus Course Extract format.
    """

    functionName = "formatCombinedCatalogForSimpleSyllabus"

    try:
        localSetup.logger.info(f"{functionName}: Starting formatting of combined catalog for Simple Syllabus")

        ## ── Helper: safe string strip that handles NaN/None ──
        def _safe_strip(val):
            if pd.isna(val) or val is None:
                return ""
            return str(val).strip()

        ## ── Helper: extract all course codes from a text string ──
        _courseCodePattern = re.compile(r'[A-Z]{3,4}\d{4}[A-Z]?')

        def _extractCourseCodes(text: str) -> set:
            if not text:
                return set()
            return set(_courseCodePattern.findall(text))

        ## ── Helper: combine text fragments, removing duplicates and fixing spacing ──
        ## Now accepts tuples of (label, value) so that the 2nd, 3rd, etc. fragments
        ## are prefixed with their original column name for clarity.
        ## Example: _combineTextFragments(("Prerequisites", "Program Admission"), ("Prerequisite Courses", "EDUC8521"))
        ##   -> "Program Admission. Prerequisite Courses: EDUC8521"
        def _combineTextFragments(*labeledFragments) -> str:
            """
            Combine labeled text fragments into a single string.
            
            Args:
                *labeledFragments: Each argument is either:
                    - A tuple of (label: str, value: str)  — label is the original column name
                    - A plain str (for backward compatibility; treated as label="" for the first, but should not happen)
            
            Returns:
                str: Combined string where the first non-empty fragment appears as-is,
                     and subsequent non-empty fragments are prefixed with their label + ": ".
            """
            combined = []
            for item in labeledFragments:
                if isinstance(item, tuple):
                    label, value = item
                else:
                    label, value = "", item
                cleaned = _safe_strip(value)
                if cleaned and cleaned not in combined:
                    if len(combined) == 0:
                        ## First fragment: no label prefix
                        combined.append(cleaned)
                    else:
                        ## Subsequent fragments: prefix with original column name
                        if label:
                            combined.append(f"{label}: {cleaned}")
                        else:
                            combined.append(cleaned)
            result = ". ".join(combined)
            ## Clean up spacing: collapse multiple spaces, fix ". ." patterns
            result = re.sub(r'\s+', ' ', result).strip()
            result = re.sub(r'\.\s*\.', '.', result)
            return result

        ## ══════════════════════════════════════════════════════════════════════
        ## STEP 1: Determine term codes for the catalog school year
        ## ══════════════════════════════════════════════════════════════════════

        currentSchoolYear = localSetup.getCurrentSchoolYear()

        ## Parse the start year from each school year string (e.g. "2025-2026" → 2025)
        currentStartYear = int(currentSchoolYear.split("-")[0])
        catalogStartYear = int(p1_catalogSchoolYear.split("-")[0])

        if catalogStartYear == currentStartYear:
            targetTermCodes = localSetup.getCurrentSchoolYearTermCodes()
        elif catalogStartYear > currentStartYear:
            targetTermCodes = localSetup.getNextSchoolYearTermCodes()
        else:
            ## Catalog year is in the past — processing it could overwrite historical data.
            ## Log, send an error notification, and halt.
            errorMsg = (
                f"{functionName}: Catalog school year '{p1_catalogSchoolYear}' is in the past "
                f"(current school year is '{currentSchoolYear}'). "
                f"Processing a past catalog year risks overwriting historical data. Halting."
            )
            localSetup.logger.error(errorMsg)
            errorHandler.sendError(functionName, ValueError(errorMsg))
            raise ValueError(errorMsg)

        localSetup.logger.info(f"{functionName}: Catalog school year={p1_catalogSchoolYear}, "
                               f"current school year={currentSchoolYear}, "
                               f"target term codes={targetTermCodes}")

        ## ══════════════════════════════════════════════════════════════════════
        ## STEP 2: Retrieve the Canvas terms DataFrame and build lookup
        ## ══════════════════════════════════════════════════════════════════════

        termsDf = CanvasReport.getTermsDf(localSetup)
        ## The terms df has columns like 'term_id' (SIS term id) and 'name'
        ## Build a dict: term_code -> term name  (e.g. "SU26" -> "Undergraduate SUMMER SEMESTER 2026")
        termCodeToNameDict = {}
        if termsDf is not None and not termsDf.empty:
            for _, row in termsDf.iterrows():
                termSisId = _safe_strip(row.get("term_id", ""))
                termName = _safe_strip(row.get("name", ""))
                if termSisId and termSisId in targetTermCodes and termName:
                    termCodeToNameDict[termSisId] = termName

        localSetup.logger.info(f"{functionName}: Term code to name mapping: {termCodeToNameDict}")

        ## Separate term codes into undergraduate and graduate
        undgTermCodes = [tc for tc in targetTermCodes 
                         if tc[:2] in undgTermsCodesToWordsDict]
        gradTermCodes = [tc for tc in targetTermCodes 
                         if tc[:2] in gradTermsCodesToWordsDict]

        ## ══════════════════════════════════════════════════════════════════════
        ## STEP 3: Load Simple Syllabus Organizations and Canvas Accounts
        ##         hierarchy to build the Parent Organization lookup
        ## ══════════════════════════════════════════════════════════════════════

        ## Load the Simple Syllabus Organizations CSV from the config path
        simpleSyllabusOrgsPath = os.path.join(localSetup.configPath, "Simple Syllabus Organizations.csv")
        simpleSyllabusOrgsDf = _readCsvWithEncoding(simpleSyllabusOrgsPath)

        ## Build a set of canvas_account_ids that are valid Simple Syllabus organizations
        ## (only rows that have a canvas_account_id)
        validOrgCanvasIds = set()
        canvasIdToOrgNameDict = {}
        for _, orgRow in simpleSyllabusOrgsDf.iterrows():
            orgCanvasId = orgRow.get("canvas_account_id")
            orgName = _safe_strip(orgRow.get("name", ""))
            if pd.notna(orgCanvasId) and orgName:
                validOrgCanvasIds.add(int(orgCanvasId))
                canvasIdToOrgNameDict[int(orgCanvasId)] = orgName

        ## Retrieve the Canvas accounts hierarchy
        accountsDf = CanvasReport.getAccountsDf(localSetup)

        ## Build a lookup: canvas_account_id -> canvas_parent_id from the accounts report
        accountParentDict = {}  ## canvas_account_id -> canvas_parent_id
        accountNameDict = {}    ## canvas_account_id -> name
        if accountsDf is not None and not accountsDf.empty:
            for _, accRow in accountsDf.iterrows():
                canvasAccId = accRow.get("canvas_account_id")
                canvasParentId = accRow.get("canvas_parent_id")
                accName = _safe_strip(accRow.get("name", ""))
                if pd.notna(canvasAccId):
                    canvasAccId = int(canvasAccId)
                    accountParentDict[canvasAccId] = int(canvasParentId) if pd.notna(canvasParentId) else None
                    accountNameDict[canvasAccId] = accName

        ## Helper: Resolve a canvas_account_id to the closest matching Simple Syllabus org name
        ## by walking up the parent chain
        def _resolveParentOrganization(canvasAccountId) -> str:
            """Walk up the Canvas account hierarchy to find the closest matching SS org."""
            if pd.isna(canvasAccountId):
                return ""
            currentId = int(canvasAccountId)
            visited = set()
            while currentId and currentId not in visited:
                visited.add(currentId)
                if currentId in canvasIdToOrgNameDict:
                    return canvasIdToOrgNameDict[currentId]
                ## Walk up to parent
                parentId = accountParentDict.get(currentId)
                if parentId is None or parentId == 1:
                    ## Reached root without finding a match
                    if 1 in canvasIdToOrgNameDict:
                        return canvasIdToOrgNameDict[1]
                    return ""
                currentId = parentId
            return ""

        ## ══════════════════════════════════════════════════════════════════════
        ## STEP 4: Retrieve Canvas courses for each target term to:
        ##         a) filter catalog courses that don't exist in Canvas
        ##         b) resolve the Parent Organization from the course's canvas_account_id
        ## ══════════════════════════════════════════════════════════════════════

        ## Build per-term lookups: { termCode: { courseCode: canvas_account_id } }
        canvasCourseInfoByTerm = {}  ## { termCode: { "ACCT2065": canvas_account_id, ... } }
        for termCode in targetTermCodes:
            try:
                coursesDf = CanvasReport.getCoursesDf(localSetup, termCode)
                courseInfo = {}
                if coursesDf is not None and not coursesDf.empty:
                    for _, crsRow in coursesDf.iterrows():
                        courseId = crsRow.get("course_id")
                        canvasAccId = crsRow.get("canvas_account_id")
                        if pd.isna(courseId) or "_" not in str(courseId):
                            continue
                        parts = str(courseId).split("_")
                        if len(parts) >= 2:
                            courseCode = parts[1]
                            ## Keep the first occurrence (or overwrite — all sections share the same account)
                            if courseCode not in courseInfo:
                                courseInfo[courseCode] = canvasAccId
                canvasCourseInfoByTerm[termCode] = courseInfo
            except Exception as termError:
                localSetup.logger.warning(f"{functionName}: Could not retrieve courses for term {termCode}: {termError}")
                canvasCourseInfoByTerm[termCode] = {}

        ## ══════════════════════════════════════════════════════════════════════
        ## STEP 4.5: Expand rows where the Title column contains multiple
        ##           course codes separated by "/" (e.g. "MUSC2250/MUSC2254").
        ##           Each code gets its own row with all other columns identical.
        ## ══════════════════════════════════════════════════════════════════════

        expandedRows = []
        for _, row in p1_combinedCatalogDf.iterrows():
            rawTitle = _safe_strip(row.get("Title", ""))
            if "/" in rawTitle:
                ## Split on "/" and create one row per course code
                codes = [code.strip() for code in rawTitle.split("/") if code.strip()]
                for code in codes:
                    newRow = row.copy()
                    newRow["Title"] = code
                    expandedRows.append(newRow)
            else:
                expandedRows.append(row)

        p1_combinedCatalogDf = pd.DataFrame(expandedRows).reset_index(drop=True)
        localSetup.logger.info(f"{functionName}: After expanding multi-code rows: {len(p1_combinedCatalogDf)} rows")

        ## ══════════════════════════════════════════════════════════════════════
        ## STEP 5: Process each catalog row — build prerequisites/corequisites,
        ##         expand into per-term rows, and filter by Canvas presence
        ## ══════════════════════════════════════════════════════════════════════

        extractRows = []

        for _, catalogRow in p1_combinedCatalogDf.iterrows():

            ## ── Parse Title into Subject and Course Number ──
            title = _safe_strip(catalogRow.get("Title", ""))
            if len(title) < 5:
                localSetup.logger.warning(f"{functionName}: Skipping row with short/missing Title: '{title}'")
                continue

            subject = title[:4]
            courseNumber = title[4:]

            ## ── Get other fields ──
            name = _safe_strip(catalogRow.get("Name", "")).upper()
            classProgram = _safe_strip(catalogRow.get("Class Program", ""))
            description = _safe_strip(catalogRow.get("Description", ""))
            credits = catalogRow.get("Credits", "")
            catalogType = _safe_strip(catalogRow.get("Catalog Type", "")).lower()

            ## ── PREREQUISITE / COREQUISITE PROCESSING ──

            rawPrerequisites = _safe_strip(catalogRow.get("Prerequisites", ""))
            rawPrerequisiteCourses = _safe_strip(catalogRow.get("Prerequisite Courses", ""))
            rawPrereqOrCoreq = _safe_strip(catalogRow.get("Prerequisite or Corequisite", ""))
            rawRecommendedPrereqs = _safe_strip(catalogRow.get("Recommended Prerequisites", ""))
            rawCorequisites = _safe_strip(catalogRow.get("Corequisites", ""))
            rawCorequisiteCourses = _safe_strip(catalogRow.get("Corequisite Courses", ""))
            rawConcurrent = _safe_strip(catalogRow.get("Concurrent", ""))
            rawConcurrentRequisite = _safe_strip(catalogRow.get("Concurrent Requisite", ""))

            ## ── Normalize spacing in all requisite fields (Rule 0) ──
            rawPrerequisites = _normalizeRequisiteSpacing(rawPrerequisites)
            rawPrerequisiteCourses = _normalizeRequisiteSpacing(rawPrerequisiteCourses)
            rawPrereqOrCoreq = _normalizeRequisiteSpacing(rawPrereqOrCoreq)
            rawRecommendedPrereqs = _normalizeRequisiteSpacing(rawRecommendedPrereqs)
            rawCorequisites = _normalizeRequisiteSpacing(rawCorequisites)
            rawCorequisiteCourses = _normalizeRequisiteSpacing(rawCorequisiteCourses)
            rawConcurrent = _normalizeRequisiteSpacing(rawConcurrent)
            rawConcurrentRequisite = _normalizeRequisiteSpacing(rawConcurrentRequisite)

            ## ── Rule 2: Remove cross-column duplicates, prioritizing prereq columns ──
            ## If the same text appears in both a prereq-related and coreq-related column,
            ## remove it from the coreq-related column
            prereqTexts = {rawPrerequisites, rawPrerequisiteCourses, rawPrereqOrCoreq, rawRecommendedPrereqs}
            prereqTexts.discard("")

            if rawConcurrent and rawConcurrent in prereqTexts:
                rawConcurrent = ""
            if rawConcurrentRequisite and rawConcurrentRequisite in prereqTexts:
                rawConcurrentRequisite = ""
            if rawCorequisites and rawCorequisites in prereqTexts:
                rawCorequisites = ""
            if rawCorequisiteCourses and rawCorequisiteCourses in prereqTexts:
                rawCorequisiteCourses = ""

                        ## ── Build combined Prerequisites string (Rule 1) ──
            prereqParts = []

            ## Put Prerequisites & Prerequisite Courses first
            basePrereq = _combineTextFragments(
                ("Prerequisites", rawPrerequisites),
                ("Prerequisite Courses", rawPrerequisiteCourses)
            )
            if basePrereq:
                prereqParts.append(basePrereq)

            ## "Prerequisite or Corequisite:" section
            if rawPrereqOrCoreq:
                prereqParts.append(f"Prerequisite or Corequisite: {rawPrereqOrCoreq}")

            ## "Recommended Prerequisites:" section
            if rawRecommendedPrereqs:
                prereqParts.append(f"Recommended Prerequisites: {rawRecommendedPrereqs}")

            ## ── Build combined Corequisites string (Rule 3) ──
            combinedCoreq = _combineTextFragments(
                ("Corequisites", rawCorequisites),
                ("Corequisite Courses", rawCorequisiteCourses),
                ("Concurrent", rawConcurrent),
                ("Concurrent Requisite", rawConcurrentRequisite)
            )

            ## ── Rule 4: Move course codes duplicated between prereqs and coreqs into ──
            ## the "Prerequisite or Corequisite:" section
            prereqCodesAll = _extractCourseCodes(". ".join(prereqParts))
            coreqCodesAll = _extractCourseCodes(combinedCoreq)
            sharedCodes = prereqCodesAll & coreqCodesAll

            if sharedCodes:
                ## Remove the shared codes from wherever they currently appear
                ## and add them to the "Prerequisite or Corequisite:" section
                sharedCodesStr = ", ".join(sorted(sharedCodes))

                ## Remove from prereq parts (rebuild without those codes)
                cleanedPrereqParts = []
                for part in prereqParts:
                    cleanedPart = part
                    for code in sharedCodes:
                        cleanedPart = cleanedPart.replace(code, "")
                    ## Clean up leftover commas/spaces
                    cleanedPart = re.sub(r',\s*,+', ',', cleanedPart)
                    cleanedPart = re.sub(r'\s+', ' ', cleanedPart).strip().strip(',').strip()
                    if cleanedPart and not cleanedPart.startswith("Prerequisite or Corequisite:"):
                        cleanedPrereqParts.append(cleanedPart)
                    elif cleanedPart.startswith("Prerequisite or Corequisite:"):
                        ## Append the shared codes to the existing section
                        existingPoC = cleanedPart.replace("Prerequisite or Corequisite:", "").strip()
                        if existingPoC:
                            cleanedPrereqParts.append(f"Prerequisite or Corequisite: {existingPoC}, {sharedCodesStr}")
                        else:
                            cleanedPrereqParts.append(f"Prerequisite or Corequisite: {sharedCodesStr}")

                ## If no "Prerequisite or Corequisite:" section was already present, add one
                hasPoCSection = any(p.startswith("Prerequisite or Corequisite:") for p in cleanedPrereqParts)
                if not hasPoCSection:
                    cleanedPrereqParts.append(f"Prerequisite or Corequisite: {sharedCodesStr}")

                prereqParts = cleanedPrereqParts

                ## Remove from coreq string
                for code in sharedCodes:
                    combinedCoreq = combinedCoreq.replace(code, "")
                combinedCoreq = re.sub(r',\s*,+', ',', combinedCoreq)
                combinedCoreq = re.sub(r'\s+', ' ', combinedCoreq).strip().strip(',').strip()

            ## ── Final prerequisite and corequisite strings ──
            finalPrerequisites = ". ".join(prereqParts)
            finalPrerequisites = re.sub(r'\s+', ' ', finalPrerequisites).strip()
            finalPrerequisites = re.sub(r'\.\s*\.', '.', finalPrerequisites)

            finalCorequisites = combinedCoreq

            ## ── Determine which terms this course applies to ──
            if catalogType == "gps":
                applicableTermCodes = gradTermCodes
            elif catalogType == "tug":
                applicableTermCodes = undgTermCodes
            else:
                applicableTermCodes = targetTermCodes

            ## ── Expand into per-term rows, filtering by Canvas presence ──
            for termCode in applicableTermCodes:
                termName = termCodeToNameDict.get(termCode, "")
                if not termName:
                    continue

                ## Check if this course exists in Canvas for this term
                courseCode = title  ## e.g. "ACCT2065"
                termCourseInfo = canvasCourseInfoByTerm.get(termCode, {})
                if courseCode not in termCourseInfo:
                    continue

                ## Resolve Parent Organization from the course's canvas_account_id
                courseCanvasAccountId = termCourseInfo[courseCode]
                parentOrg = _resolveParentOrganization(courseCanvasAccountId)

                extractRows.append({
                    "Term": termName,
                    "Subject": subject,
                    "Course Number": courseNumber,
                    "Title": name,
                    "Parent Organization": parentOrg,
                    "Class Program": classProgram,
                    "Description": description,
                    "Credits": credits,
                    "Prerequisites": finalPrerequisites,
                    "Corequisites": finalCorequisites,
                })

        ## ═══════════════���══════════════════════════════════════════════════════
        ## STEP 6: Build the output DataFrame and save
        ## ══════════════════════════════════════════════════════════════════════

        courseExtractDf = pd.DataFrame(extractRows, columns=[
            "Term", "Subject", "Course Number", "Title", "Parent Organization",
            "Class Program", "Description", "Credits", "Prerequisites", "Corequisites"
        ])

        ## Remove exact duplicate rows
        courseExtractDf.drop_duplicates(inplace=True)
        courseExtractDf.reset_index(drop=True, inplace=True)

        ## Save the Course Extract CSV
        baseCatalogPath = localSetup.getInternalResourcePaths("Catalog")
        catalogPath = os.path.join(baseCatalogPath, p1_catalogSchoolYear)
        os.makedirs(catalogPath, exist_ok=True)
        outputFilePath = os.path.join(catalogPath, "Course Extract.csv")
        courseExtractDf.to_csv(outputFilePath, index=False, encoding='utf-8')

        localSetup.logger.info(f"{functionName}: Successfully formatted {len(courseExtractDf)} rows and saved to {outputFilePath}")

        return courseExtractDf

    except Exception as Error:
        localSetup.logger.error(f"{functionName}: {Error}")
        raise


## This function determines the school year for the catalog based on the provided catalog links, 
## retrieves the catalog home page to find the school year if necessary, 
## and builds a local path for storing the catalog reports based on that school year. 
def buildCatalogSchoolYearRelatedLocalPath(p1_catalogLinksDict: dict) -> str:

    functionName = "buildCatalogSchoolYearRelatedLocalPath"

    def _tryParseCatalogSchoolYearFromCatalogHomeHtml(htmlText: str):
        match = re.search(r"(20\d{2})\s*[-/\u2013\u2014]?\s*(20\d{2})", htmlText)
        if match:
            return f"{match.group(1)}-{match.group(2)}"
        compactMatch = re.search(r"(20\d{2})(20\d{2})", htmlText)
        if compactMatch:
            return f"{compactMatch.group(1)}-{compactMatch.group(2)}"
        return None

    try:
        schoolYear = None
        for _, downloadUrl in (p1_catalogLinksDict or {}).items():
            if not downloadUrl:
                continue

            reportPageUrl = downloadUrl.split("/download", 1)[0]
            localSetup.logger.info(f"Determining catalog school year from {reportPageUrl}")

            # Use makeApiCall instead of direct requests.get()
            reportPageResponse, _ = makeApiCall(
                localSetup,
                p1_apiUrl=reportPageUrl,
                p1_apiCallType="get"
            )
            reportPageHtml = reportPageResponse.text

            # Parse HTML with BeautifulSoup
            soup = BeautifulSoup(reportPageHtml, 'html.parser')
            
            # Find the "Catalog Home" link using BeautifulSoup
            catalogHomeLink = soup.find(
                'a',
                string=re.compile(r'Catalog\s+Home', re.IGNORECASE)
            )

            if not catalogHomeLink or not catalogHomeLink.get('href'):
                schoolYear = _tryParseCatalogSchoolYearFromCatalogHomeHtml(reportPageHtml)
                if schoolYear:
                    break
                continue

            catalogHomeHref = catalogHomeLink['href']
            ## Parse the base URL from the report page URL
            parsedReportPageUrl = urlparse(reportPageUrl)
            ## Build the full URL for the catalog home page
            catalogHomeUrl = f"{parsedReportPageUrl.scheme}://{parsedReportPageUrl.netloc}{catalogHomeHref}"

            # Use makeApiCall instead of direct requests.get()
            homeResponse, _ = makeApiCall(
                localSetup,
                p1_apiUrl=catalogHomeUrl,
                p1_apiCallType="get"
            )

            # Parse home page HTML with BeautifulSoup
            catalogSchoolYear = _tryParseCatalogSchoolYearFromCatalogHomeHtml(homeResponse.text)
            if catalogSchoolYear:
                break

        if not catalogSchoolYear:
            nowYear = datetime.now().year
            catalogSchoolYear = f"{nowYear}-{nowYear+1}"
            localSetup.logger.warning(f"{functionName}: Could not determine catalog school year; defaulting to {catalogSchoolYear}")

        baseCatalogPath = localSetup.getInternalResourcePaths("Catalog")
        catalogPath = os.path.join(baseCatalogPath, catalogSchoolYear)
        os.makedirs(catalogPath, exist_ok=True)
        return catalogPath, catalogSchoolYear

    except Exception as Error:
        localSetup.logger.error(f"{functionName}: {Error}")
        raise

## Retrieve the TUG and GPS catalog course reports from the urls in catalogProduction in the catalogToSimpleSyllabusConfig
def retrieveCatalogCourseReportsDfs():

    functionName = "retrieveCatalogCourseReports"

    try:
        catalogLinks = catalogToSimpleSyllabusConfig.get("catalogProduction", {})
        if not catalogLinks or not isinstance(catalogLinks, dict):
            raise ValueError("catalogToSimpleSyllabusConfig['catalogProduction'] missing or invalid")

        catalogRootPath, catalogSchoolYear = buildCatalogSchoolYearRelatedLocalPath(catalogLinks)

        catalogCourseReportsDict = {}
        for catalogType, downloadUrl in catalogLinks.items():
            if not downloadUrl:
                continue

            fileName = f"{catalogType.upper()} Course Report.csv"
            localFilePath = os.path.join(catalogRootPath, fileName)
            localSetup.logger.info(f"Downloading {catalogType} catalog course report to {localFilePath}")

            downloadedPath = downloadFile(localSetup, downloadUrl, localFilePath)
            if not downloadedPath or not os.path.exists(downloadedPath):
                raise FileNotFoundError(f"Download failed for {catalogType}: {downloadedPath}")

            catalogCourseReportsDict[catalogType] = downloadedPath

        if not catalogCourseReportsDict:
            raise ValueError("No catalog reports were downloaded")

        ## Open the downloaded files as dfs, add a column for catalog type, and combine into a single df
        combinedCatalogCourseReportDf = pd.DataFrame()
        for catalogType, filePath in catalogCourseReportsDict.items():
            catalogCourseReportsDf = _readCsvWithEncoding(filePath)
            catalogCourseReportsDf['Catalog Type'] = catalogType
            if combinedCatalogCourseReportDf.empty:
                combinedCatalogCourseReportDf = catalogCourseReportsDf
            else:
                combinedCatalogCourseReportDf = pd.concat([combinedCatalogCourseReportDf, catalogCourseReportsDf], ignore_index=True)

        ## ── Check whether the new combined catalog differs from the last successfully uploaded version ──
        combinedCsvPath = os.path.join(catalogRootPath, "Combined Catalog Course Report.csv")
        successTagPath = os.path.join(catalogRootPath, "Combined Catalog Course Report_UPLOAD_SUCCESS.txt")

        if os.path.exists(combinedCsvPath) and os.path.exists(successTagPath):
            ## A previous combined catalog exists AND was successfully uploaded
            try:
                previousCombinedDf = _readCsvWithEncoding(combinedCsvPath)

                ## Compare the new combined df against the previous one
                ## Sort both by all columns and reset index to ensure consistent comparison
                newSorted = combinedCatalogCourseReportDf.sort_values(
                    by=list(combinedCatalogCourseReportDf.columns)
                ).reset_index(drop=True)
                prevSorted = previousCombinedDf.sort_values(
                    by=list(previousCombinedDf.columns)
                ).reset_index(drop=True)

                if newSorted.equals(prevSorted):
                    localSetup.logger.info(
                        f"{functionName}: No changes detected in the combined catalog since last successful upload. Skipping."
                    )
                    return combinedCatalogCourseReportDf, catalogSchoolYear, False
                else:
                    localSetup.logger.info(
                        f"{functionName}: Changes detected in the combined catalog since last successful upload. Proceeding."
                    )
            except Exception as compareError:
                localSetup.logger.warning(
                    f"{functionName}: Could not compare with previous catalog ({compareError}). Proceeding with upload."
                )
        else:
            localSetup.logger.info(
                f"{functionName}: No previous successfully uploaded combined catalog found. Proceeding."
            )

        ## Save the combined df as a new CSV in the same location as the downloaded reports, with a name like "Combined Catalog Course Report.csv"
        combinedCatalogCourseReportDf.to_csv(combinedCsvPath, index=False, encoding='utf-8')

        ## If the success tag exists from a prior run but the data HAS changed, remove the stale tag
        if os.path.exists(successTagPath):
            os.remove(successTagPath)
            localSetup.logger.info(f"{functionName}: Removed stale success tag at {successTagPath}")

        return combinedCatalogCourseReportDf, catalogSchoolYear, True

    except Exception as Error:
        localSetup.logger.error(f"{functionName}: {Error}")
        raise

## This function retreives the catalog course reports, combines them into a simple syllabus format, and uploads the result to Simple Syllabus
def processCatalogCoursesAndUploadToSimpleSyllabus():
    functionName = "processCatalogCoursesAndUploadToSimpleSyllabus"
    try:
        combinedCatalogCourseReportDf, catalogSchoolYear, hasChanges = retrieveCatalogCourseReportsDfs()

        ## If no changes since last successful upload, exit early
        if not hasChanges:
            localSetup.logger.info(
                f"{functionName}: No changes detected in catalog data since last successful upload. Exiting early."
            )
            return

        courseExtractDf = formatCombinedCatalogForSimpleSyllabus(combinedCatalogCourseReportDf, catalogSchoolYear)

        ## Build the path to the saved Course Extract CSV
        baseCatalogPath = localSetup.getInternalResourcePaths("Catalog")
        catalogPath = os.path.join(baseCatalogPath, catalogSchoolYear)
        courseExtractFilePath = os.path.join(catalogPath, "Course Extract.csv")

        ## Upload the processed file to Simple Syllabus via SFTP
        uploadToSimpleSyllabus(courseExtractFilePath)

        localSetup.logger.info(f"{functionName}: Successfully processed catalog courses and uploaded to Simple Syllabus")
    except Exception as Error:
        localSetup.logger.error(f"{functionName}: {Error}")
        raise
        
## If the script is being run directly, execute the main function to process catalog courses and upload to Simple Syllabus
if __name__ == "__main__":
    functionName = "main"
    try:
        processCatalogCoursesAndUploadToSimpleSyllabus()
    except Exception as Error:
        localSetup.logger.error(f"{functionName}: {Error}")
        errorHandler.sendError(functionName, Error)