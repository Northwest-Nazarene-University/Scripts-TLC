## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

## Import Generic Modules
import os, sys, re, time, pandas as pd, paramiko
from datetime import datetime

try: ## If the module is run directly
    from Local_Setup import LocalSetup
    from TLC_Common import getEncryptionKey
except ImportError: ## Otherwise as a relative import if the module is imported
    from .Local_Setup import LocalSetup
    from .TLC_Common import getEncryptionKey

## Add the config path
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "Configs"))

from Common_Configs import catalogToSimpleSyllabusConfig

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
            catalogDir = os.path.dirname(p1_filePath)
            successTagPath = os.path.join(catalogDir, "Combined Catalog Course Report_UPLOAD_SUCCESS.txt")
            with open(successTagPath, "w", encoding="utf-8") as tagFile:
                tagFile.write(f"Upload successful at {datetime.now().isoformat()}\n")
                tagFile.write(f"Uploaded file: {p1_filePath}\n")
            p1_localSetup.logger.info(f"{functionName}: Success tag written to {successTagPath}")

    except Exception as Error:
        p1_localSetup.logger.error(f"{functionName}: {Error}")
        raise