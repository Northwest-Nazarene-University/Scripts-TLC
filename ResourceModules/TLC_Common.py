## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

## Import Generic Modules
import os, sys, requests, time, functools, zipfile, pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from typing import Callable, Tuple, Type

try: ## If the module is run directly
    from Local_Setup import LocalSetup
except ImportError: ## Otherwise as a relative import if the module is imported
    from .Local_Setup import LocalSetup

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

from Common_Configs import canvasAccessToken

def retry(
    max_attempts: int = 5,
    delay: float = 5.0,
    backoff: float = 1.5,
    exceptions: Tuple[Type[Exception], ...] = (Exception,)
):
    """
    Retry decorator that uses the localSetup.logger from a LocalSetup instance passed as the first argument.
    Assumes the decorated function's first argument is a LocalSetup object.
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            localSetup = args[0]  ## Assumes LocalSetup is the first argument

            attempts = 0
            current_delay = delay

            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    if localSetup.logger:
                        localSetup.logger.warning(
                            f"Attempt {attempts} failed for {func.__name__}: {e}. Retrying in {current_delay:.1f} seconds..."
                        )
                    if attempts == max_attempts:
                        if localSetup.logger:
                            localSetup.logger.error(f"{func.__name__} failed after {attempts} attempts.")
                        raise
                    time.sleep(current_delay)
                    current_delay *= backoff
        return wrapper
    return decorator


## Return Encryption Key Function
def getEncryptionKey(localSetup: LocalSetup):
    ## Load .env from configPath
    envPath = os.path.join(localSetup.configPath, ".env")
    load_dotenv(dotenv_path=envPath) 

    ## Retrieve the encryption key from environment variables
    encryptionKey = os.getenv("ENCRYPTION_KEY")
    
    ## If the encryption key is not found, raise an error
    if not encryptionKey:
        localSetup.logger.error("ENCRYPTION_KEY not found in environment variables.")
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
                localSetup.logger.warning(f"Downloaded file is not a valid Excel file. Attempting repair...")
                ## Try reading as CSV and resave as proper Excel
                try:
                    fileDataframe = pd.read_csv(finalFilePathWithName)
                    with pd.ExcelWriter(finalFilePathWithName, engine="openpyxl") as writer:
                        fileDataframe.to_excel(writer, index=False)
                    localSetup.logger.info(f"File repaired")
                    return finalFilePathWithName
                except Exception as e:
                    localSetup.logger.error(f"Repair failed: {e}")
                    raise
        ## If valid or not Excel, return original path
        return finalFilePathWithName
    except Exception as e:
        localSetup.logger.error(f"Validation/repair step failed: {e}")
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

    except Exception as Error:
        ## Log the error here; the calling function can decide whether to send an error email
        localSetup.logger.error(
            f"{functionName}: Error while flattening API responses for URL {apiUrl}: {Error}"
        )
        raise

## This function takes a api header and url and returns the json object of the api call, recursively calling itself in a seperate instance up to 5 times if the call fails
@retry(max_attempts=5, delay=5, backoff=2.0)
def makeApiCall(
    localSetup: LocalSetup, 
    p1_apiUrl,
    p1_header = None,
    p1_payload=None,
    p1_files=None,
    p1_apiCallType="get",
    firstPageOnly=False,
):
    """
    Makes an API call with retry logic.
    Supports GET, POST, PUT, DELETE methods.
    Automatically retries on failure using the retry decorator.
    """
    ## Set the default header, payload, and files if not provided
    if p1_header is None:
        p1_header = {'Authorization': f'Bearer {canvasAccessToken}'}
    if p1_payload is None:
        p1_payload = {}
    if p1_files is None:
        p1_files = {}

    ## Initialize variables for the API response and list of responses (for pagination)
    p1_apiObject = None
    p1_apiObjectList = []
    ## Perform the API call based on type
    if p1_apiCallType.lower() == "get":
        p1_payload.setdefault("per_page", 100)
        p1_apiObject = requests.get(url=p1_apiUrl, headers=p1_header, params=p1_payload)

    elif p1_apiCallType.lower() == "post":
        if p1_payload and p1_files:
            p1_apiObject = requests.post(url=p1_apiUrl, headers=p1_header, json=p1_payload, files=p1_files)
        elif p1_payload:
            p1_apiObject = requests.post(url=p1_apiUrl, headers=p1_header, params=p1_payload)
        else:
            p1_apiObject = requests.post(url=p1_apiUrl, headers=p1_header)

    elif p1_apiCallType.lower() == "put":
        if p1_payload:
            p1_apiObject = requests.put(url=p1_apiUrl, headers=p1_header, json=p1_payload)
        else:
            p1_apiObject = requests.put(url=p1_apiUrl, headers=p1_header)

    elif p1_apiCallType.lower() == "delete":
        if p1_payload:
            p1_apiObject = requests.delete(url=p1_apiUrl, headers=p1_header, params=p1_payload)
        else:
            p1_apiObject = requests.delete(url=p1_apiUrl, headers=p1_header)

    else:
        raise ValueError(f"Unsupported API call type: {p1_apiCallType}")    
    ## Validate response
    ## log the response status code
    if not p1_apiObject.status_code or p1_apiObject.status_code not in [200, 400]:
        if p1_apiObject.status_code:
            ## --- SPECIAL CASE: 409 Conflict for PUT/PATCH ---
            if p1_apiObject.status_code == 409 and p1_apiCallType.lower() in ["put", "patch", "post"]:
                localSetup.logger.warning(f"Received 409 Conflict for {p1_apiCallType.upper()} {p1_apiUrl}. Checking for active existing item...")

                ## Make the GET call to retrieve current index
                indexResponse, _ = makeApiCall(
                    localSetup,
                    p1_apiUrl=p1_apiUrl,
                    p1_header=p1_header,
                    p1_apiCallType="get",
                    firstPageOnly=True,
                )
                indexData = indexResponse.json() if hasattr(indexResponse, "json") else []

                requestedParams = {
                        key[len("parameters["):-1]: value
                        for key, value in p1_payload.items()
                        if key.startswith("parameters[")
                    }
          
                ## Find any active report with matching parameters
                matchingReport = next(
                    (r for r in indexData
                     if r.get("status") in ["running", "pending", "created"]
                     and r.get("parameters", {}) == requestedParams),
                    None
                )                                 
                if matchingReport:
                    localSetup.logger.info("Found active report with matching parameters. Returning its status response instead of retrying.")
                    reportId = matchingReport["id"]
                    statusUrl = f"{p1_apiUrl}/{reportId}"
                    statusResponse, _ = makeApiCall(
                        localSetup,
                        p1_apiUrl=statusUrl,
                        p1_header=p1_header
                    )
                    return statusResponse, []
                else:
                    localSetup.logger.info(f"409 received but no matching active report with paramters: {requestedParams}, found - retrying normally.")
            try:
                p1_apiObject.close()
            except Exception as close_error:
                localSetup.logger.warning(f"Failed to close API response before retry: {close_error}")
            if p1_apiCallType != "delete":
                raise Exception(f"Failed API call to {p1_apiUrl}: HTTP {p1_apiObject.status_code}")  
            else:
                localSetup.logger.warning(f"Failed to delete resource at {p1_apiUrl}: HTTP {p1_apiObject.status_code}")
                ## Break out of the retry loop for delete calls
                return None, None
    ## Handle pagination if applicable
    if hasattr(p1_apiObject, 'links') and 'next' in getattr(p1_apiObject, 'links', {}) and not firstPageOnly:
        p1_apiObjectList.append(p1_apiObject)
        next_url = p1_apiObject.links["next"]["url"]
        next_page, next_pageList = makeApiCall(
            localSetup,
            p1_apiUrl=next_url,
            p1_header=p1_header,
            p1_payload=None,
            p1_files=p1_files,
            p1_apiCallType=p1_apiCallType,
            firstPageOnly=firstPageOnly
        )
        if next_pageList:
            p1_apiObjectList.extend(next_pageList)
        elif next_page:
            p1_apiObjectList.append(next_page)

    return p1_apiObject, p1_apiObjectList

## Check if a file exists and was modified within the last X hours
def isFileRecent(localSetup: LocalSetup, filePath, maxAgeHours=3.5):
    functionName = "isFileRecent"
    try:

        ## If the file does not exist, return False
        if not os.path.exists(filePath):
            if localSetup.logger:
                localSetup.logger.info(f"\n{filePath} does not exist.")
            return False

        ## Get the last modified time and calculate age in hours
        lastModified = os.path.getmtime(filePath)
        fileAgeHours = (datetime.now() - datetime.fromtimestamp(lastModified)).total_seconds() / 3600

        ## Log and return based on file age
        if fileAgeHours < maxAgeHours:
            if localSetup.logger:
                localSetup.logger.info(f"\n{filePath} is recent ({fileAgeHours:.2f} hours old).")
            return True
        else:
            if localSetup.logger:
                localSetup.logger.info(f"\n{filePath} is outdated ({fileAgeHours:.2f} hours old).")
            return False
    except Exception as Error:
        ## Log any unexpected errors
        if localSetup.logger:
            localSetup.logger.error(f"Couldn't determine file age. Error: {Error}")
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
            print(f"Trying engine: {engine}")
            excelFile = pd.ExcelFile(filePath, engine=engine)
            
            ## If sheetName is None, return all sheets as dict
            if sheetName is None:
                return {sheet: excelFile.parse(sheet) for sheet in excelFile.sheet_names}
            else:
                return excelFile.parse(sheetName)
        
        except Exception as e:
            lastError = e
            print(f"Engine {engine} failed: {e}")
    
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
    """

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
