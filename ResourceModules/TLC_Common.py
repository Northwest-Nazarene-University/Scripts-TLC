## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

import os, sys, zipfile, requests, pandas as pd
from datetime import datetime
from dotenv import load_dotenv

try: ## If the module is run directly
    from Local_Setup import LocalSetup, logInfo as _logInfo, logWarning as _logWarning, logError as _logError
    from Api_Caller import retry, RateLimitExceeded, makeApiCall
except ImportError: ## Otherwise as a relative import if the module is imported
    from .Local_Setup import LocalSetup, logInfo as _logInfo, logWarning as _logWarning, logError as _logError
    from .Api_Caller import retry, RateLimitExceeded, makeApiCall

## Define the script name, purpose, and external requirements for logging and error reporting purposes
__scriptName = os.path.basename(__file__).replace(".py", "")

scriptPurpose = r"""
Provide common variables, dictionaries, and setup functions for use in TLC scripts.
"""

externalRequirements = r"""
To be located within a folder named "Resource Folder" which has "Configs" folder at the same courseLevel.
Both folders should be under a main project folder, often named for the department, ## e.g., "Scripts_TLC".
"""

## Add the config path
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "Configs"))


## Return Encryption Key Function
def getEncryptionKey(localSetup: LocalSetup):
    ## Load .env from configPath
    envPath = os.path.join(localSetup.configPath, ".env")
    load_dotenv(dotenv_path=envPath) 

    ## Retrieve the encryption key from environment variables
    encryptionKey = os.getenv("ENCRYPTION_KEY")
    
    ## If the encryption key is not found, raise an error
    if not encryptionKey:
        _logError(localSetup, "ENCRYPTION_KEY not found in environment variables.")
        raise ValueError("ENCRYPTION_KEY not found in environment variables.")

    return encryptionKey

## File Download Function
@retry(max_attempts=5, delay=5, backoff=2.0)
def downloadFile(localSetup: LocalSetup, fileLink, filePathWithName, mode = 'w'):
    """
    Downloads a file from the given URL to the specified path.
    Automatically retries on failure using the retry decorator.
    """
    ## Define a blank variable for the final file path
    finalFilePathWithName = ""
    ## Shorten filename if path exceeds 255 characters
    if len(filePathWithName) > 255:
        fileName = filePathWithName.split("\\")[-1]
        fileNameWithoutExt = fileName.split(".")[0]
        numCharsToRemove = len(filePathWithName) - 255
        cutoffPoint = len(fileNameWithoutExt) - numCharsToRemove
        newFileName = fileNameWithoutExt[:cutoffPoint] + "." + fileName.split(".")[-1]
        filePathWithoutName = filePathWithName.rsplit("\\", maxsplit=1)[0]
        finalFilePathWithName = filePathWithoutName + "\\" + newFileName
    else:
        finalFilePathWithName = filePathWithName
    ## Ensure the directory exists
    filePathWithoutName = finalFilePathWithName.rsplit("\\", maxsplit=1)[0]
    os.makedirs(filePathWithoutName, mode=0o777, exist_ok=True)
    ## Download the file in chunks
    response = requests.get(fileLink, stream=True, allow_redirects=True)
    if response.status_code != 200:
        raise Exception(f"Failed to download file from URL: {response.url}: HTTP {response.status_code}")
    with open(finalFilePathWithName, 'wb' if mode == "w" else 'ab') as f:
        for chunk in response.iter_content(1024 * 1024 * 2):  ## 2 MiB chunks
            f.write(chunk)
    ## Validate the downloaded file if it's an Excel file
    try:
        if finalFilePathWithName.lower().endswith(".xlsx"):
            if not zipfile.is_zipfile(finalFilePathWithName):
                _logWarning(localSetup, f"Downloaded file is not a valid Excel file. Attempting repair...")
                ## Try reading as CSV and resave as proper Excel
                try:
                    fileDataframe = pd.read_csv(finalFilePathWithName)
                    with pd.ExcelWriter(finalFilePathWithName, engine="openpyxl") as writer:
                        fileDataframe.to_excel(writer, index=False)
                    _logInfo(localSetup, f"File repaired")
                    return finalFilePathWithName
                except Exception as e:
                    _logError(localSetup, f"Repair failed: {e}")
                    raise
        ## If valid or not Excel, return original path
        return finalFilePathWithName
    except Exception as e:
        _logError(localSetup, f"Validation/repair step failed: {e}")
    return finalFilePathWithName

## This function normalizes a Canvas API response (or list of responses) into a single list of JSON objects.
def flattenApiObjectToJsonList(localSetup, apiObjectList, apiUrl):
    functionName = "flattenApiObjectToJsonList"
    try:
        ## Flatten the json from all pages into a single list
        flattenedJsonList = []

        for responseObject in apiObjectList:
            pageData = responseObject.json()

            ## If the page data is a list, extend the flattened list
            if isinstance(pageData, list):
                flattenedJsonList.extend(pageData)
            else:
                ## If the page data is a dict or single object, append it
                flattenedJsonList.append(pageData)

        return flattenedJsonList

    except Exception as error:
        ## Log the error here; the calling function can decide whether to send an error email
        _logError(localSetup,
            f"{functionName}: Error while flattening API responses for URL {apiUrl}: {error}"
        )
        raise

## Check if a file exists and was modified within the last X hours
def isFileRecent(localSetup: LocalSetup, filePath, maxAgeHours=3.5):
    functionName = "isFileRecent"
    try:

        ## If the file does not exist, return False
        if not os.path.exists(filePath):
            if localSetup.logger:
                _logInfo(localSetup, f"\n{filePath} does not exist.")
            return False

        ## Get the last modified time and calculate age in hours
        lastModified = os.path.getmtime(filePath)
        fileAgeHours = (datetime.now() - datetime.fromtimestamp(lastModified)).total_seconds() / 3600

        ## Log and return based on file age
        if fileAgeHours < maxAgeHours:
            if localSetup.logger:
                _logInfo(localSetup, f"\n{filePath} is recent ({fileAgeHours:.2f} hours old).")
            return True
        else:
            if localSetup.logger:
                _logInfo(localSetup, f"\n{filePath} is outdated ({fileAgeHours:.2f} hours old).")
            return False
    except Exception as error:
        ## Log any unexpected errors
        if localSetup.logger:
            _logError(localSetup, f"Couldn't determine file age. Error: {error}")
        return False

## Load Excel File with Multiple Strategies
def loadExcelFile(filePath, sheetName=None):
    """
    Attempts to load an Excel file using multiple strategies.
    
    Parameters:
        filePath (str): Path to the Excel file.
        sheetName (str or None): Sheet name to read. If None, reads all sheets.
    
    Returns:
        pd.DataFrame or dict of DataFrames: Loaded data, or None if failed.
    """
    ## Validate file existence
    if not os.path.exists(filePath):
        raise FileNotFoundError(f"File not found: {filePath}")
    
    ## Validate file size
    if os.path.getsize(filePath) == 0:
        raise ValueError(f"File is empty: {filePath}")
    
    ## Validate extension
    if not filePath.lower().endswith((".xlsx", ".xls")):
        raise ValueError(f"Invalid file type. Expected Excel (.xlsx or .xls), got: {filePath}")
    
    ## Try multiple engines
    engines = ["openpyxl", "xlrd"]
    lastError = None
    
    for engine in engines:
        try:
            excelFile = pd.ExcelFile(filePath, engine=engine)
            
            ## If sheetName is None, return all sheets as dict
            if sheetName is None:
                return {sheet: excelFile.parse(sheet) for sheet in excelFile.sheet_names}
            else:
                return excelFile.parse(sheetName)
        
        except Exception as e:
            lastError = e
    
    ## If all attempts fail
    raise RuntimeError(f"Failed to load Excel file after trying all engines. Last error: {lastError}")

## Helper function to determine if a value is missing/NA based on multiple criteria
def isMissing(value):
    """
    Returns True if the value should be considered 'missing'.
    Handles:
    - None
    - NaN / pd.NA / numpy.nan
    - empty string ""
    - whitespace-only strings
    - strings that spell 'nan' (case-insensitive)
    - empty DataFrames or Series
    """

    ## DataFrames -> missing if empty
    if isinstance(value, pd.DataFrame):
        return value.empty

    ## Series -> missing if empty
    if isinstance(value, pd.Series):
        return value.empty

    ## String-like values -> normalize and check
    if isinstance(value, str):
        stripped = value.strip()

        ## Empty or whitespace-only
        if stripped == "":
            return True

        ## Literal "nan", any casing
        if stripped.lower() == "nan":
            return True

    ## True NaN / None / pd.NA -> missing
    if pd.isna(value):
        return True

    return False

## Inverse helper function to determine if a value is present (not missing)
def isPresent(value):
    """Inverse helper for convenience."""
    return not isMissing(value)
