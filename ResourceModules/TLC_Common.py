## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

import os, sys, time, functools, zipfile, threading, random, requests, pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from typing import Callable, Tuple, Type, Optional

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

## -------------------------
## Rate-Limit Configuration
## -------------------------

## Default seconds to wait when Canvas returns HTTP 429 and no Retry-After header exists
BaseRateLimitWaitSeconds: float = 5.0

## Maximum jitter added to rate-limit waits (Uniform(0, Max))
## Average jitter = Max / 2
RateLimitJitterMaxSeconds: float = 2.0

## Backoff applied ONLY to repeated 429 waits when Retry-After is missing
RateLimitBackoffMultiplier: float = 1.5
MaxThrottleRetries: int = 10

## Pre-emptive pause when remaining quota is low
RateLimitPauseThreshold: float = 10.0
BasePreemptivePauseSeconds: float = 0.5
PreemptivePauseJitterMaxSeconds: float = 0.25

## Hard timeout for requests to prevent hanging forever
RequestTimeoutSeconds: float = 600.0

## Shared rate-limit state across threads in this process
_RateLimitRemaining: Optional[float] = None
_RateLimitLock = threading.Lock()


## -------------------------
## Custom Exceptions
## -------------------------

class RateLimitExceeded(Exception):
    """Raised when the Canvas API returns HTTP 429 (rate limit exceeded)."""
    def __init__(self, RetryAfter: Optional[float] = None, Message: str = "Rate limit exceeded"):
        self.RetryAfter = RetryAfter
        super().__init__(Message)


## -------------------------
## Email Helper (Best Effort)
## -------------------------

def _SendTimeoutEmail(localSetup, ApiUrl: str, TimeoutSeconds: float, Error: Exception) -> None:
    """
    Best-effort timeout notification.
    Tries common LocalSetup email methods; falls back to logging.
    """
    Subject = f"Canvas API Timeout ({TimeoutSeconds:.0f}s)"
    Body = (
        f"A Canvas API request timed out.\n\n"
        f"URL: {ApiUrl}\n"
        f"TimeoutSeconds: {TimeoutSeconds:.0f}\n"
        f"Error: {Error}\n"
        f"Timestamp: {datetime.now().isoformat()}\n"
    )

    ## Try likely method names without assuming your LocalSetup implementation
    for MethodName in ["SendErrorEmail", "sendErrorEmail", "SendEmail", "sendEmail"]:
        SendMethod = getattr(localSetup, MethodName, None)
        if callable(SendMethod):
            try:
                ## Support both (subject, body) and keyword forms
                try:
                    SendMethod(Subject, Body)
                except TypeError:
                    SendMethod(Subject=Subject, Body=Body)
                return
            except Exception as EmailError:
                if getattr(localSetup, "logger", None):
                    localSetup.logger.error(f"Failed sending timeout email via {MethodName}: {EmailError}")

    if getattr(localSetup, "logger", None):
        localSetup.logger.error(f"No email method found on LocalSetup. Timeout email not sent.\n{Subject}\n{Body}")


## -------------------------
## Rate-Limit Header Helpers
## -------------------------

def _UpdateRateLimitRemainingFromResponse(ResponseObject) -> None:
    global _RateLimitRemaining

    RawRemaining = ResponseObject.headers.get("X-Rate-Limit-Remaining")
    if RawRemaining is None:
        return

    try:
        RemainingValue = float(RawRemaining)
    except (ValueError, TypeError):
        return

    with _RateLimitLock:
        _RateLimitRemaining = RemainingValue


def _PreemptiveRateLimitPauseIfNeeded(localSetup, ApiUrl: str) -> None:
    with _RateLimitLock:
        CurrentRemaining = _RateLimitRemaining

    if CurrentRemaining is None:
        return

    if CurrentRemaining <= RateLimitPauseThreshold:
        JitterSeconds = random.uniform(0.0, PreemptivePauseJitterMaxSeconds)
        PauseSeconds = BasePreemptivePauseSeconds + JitterSeconds

        if getattr(localSetup, "logger", None):
            localSetup.logger.info(
                f"Rate-limit remaining low ({CurrentRemaining:.1f} <= {RateLimitPauseThreshold:.1f}). "
                f"Preemptive pause {PauseSeconds:.2f}s before {ApiUrl}."
            )

        time.sleep(PauseSeconds)


## -------------------------
## Retry Decorator (Separate 429 Lane)
## -------------------------

def retry(
    max_attempts: int = 5,
    delay: float = 5.0,
    backoff: float = 1.5,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    max_throttle_retries: int = MaxThrottleRetries,
):
    """
    Retry decorator using LocalSetup.logger.
    Assumes first arg is a LocalSetup object.

    Rate-limit retries (RateLimitExceeded) are handled separately and do not count
    against max_attempts.
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            localSetup = args[0]

            Attempts = 0
            ThrottleRetries = 0

            CurrentDelaySeconds = delay
            CurrentRateLimitWaitSeconds = BaseRateLimitWaitSeconds

            while Attempts < max_attempts:
                try:
                    return func(*args, **kwargs)

                except RateLimitExceeded as RateLimitError:
                    ThrottleRetries += 1

                    ## Use Retry-After if provided, else base wait + jitter
                    if RateLimitError.RetryAfter is not None:
                        WaitSeconds = float(RateLimitError.RetryAfter)
                    else:
                        JitterSeconds = random.uniform(0.0, RateLimitJitterMaxSeconds)
                        WaitSeconds = CurrentRateLimitWaitSeconds + JitterSeconds

                        ## Increase rate-limit wait for repeated 429s (still doesn't consume Attempts)
                        CurrentRateLimitWaitSeconds *= RateLimitBackoffMultiplier

                    if getattr(localSetup, "logger", None):
                        localSetup.logger.warning(
                            f"Rate limit hit for {func.__name__} "
                            f"(throttle retry {ThrottleRetries}/{max_throttle_retries}). "
                            f"Waiting {WaitSeconds:.2f}s before retrying..."
                        )

                    if ThrottleRetries >= max_throttle_retries:
                        if getattr(localSetup, "logger", None):
                            localSetup.logger.error(
                                f"{func.__name__} exceeded max throttle retries ({max_throttle_retries})."
                            )
                        raise

                    time.sleep(WaitSeconds)

                except exceptions as Error:
                    Attempts += 1

                    if getattr(localSetup, "logger", None):
                        localSetup.logger.warning(
                            f"Attempt {Attempts} failed for {func.__name__}: {Error}. "
                            f"Retrying in {CurrentDelaySeconds:.1f} seconds..."
                        )

                    if Attempts == max_attempts:
                        if getattr(localSetup, "logger", None):
                            localSetup.logger.error(f"{func.__name__} failed after {Attempts} attempts.")
                        raise

                    time.sleep(CurrentDelaySeconds)
                    CurrentDelaySeconds *= backoff

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


## -------------------------
## makeApiCall (Session + Timeout + 429 -> RateLimitExceeded)
## -------------------------

@retry(max_attempts=5, delay=5, backoff=2.0)
def makeApiCall(
    localSetup,
    p1_apiUrl,
    p1_header=None,
    p1_payload=None,
    p1_files=None,
    p1_apiCallType="get",
    firstPageOnly=False,
):
    """
    Makes a Canvas API call using localSetup.canvasSession and a 600s timeout.
    - Preemptive pause when remaining quota is low (X-Rate-Limit-Remaining)
    - HTTP 429 raises RateLimitExceeded (handled separately by retry decorator)
    - Status validation:
        * Any 2xx is success
        * 400 is allowed (unchanged behavior)
        * others raise, with delete special-case preserved
    """

    ## Defaults
    if p1_header is None:
        p1_header = {"Authorization": f"Bearer {canvasAccessToken}"}
    if p1_payload is None:
        p1_payload = {}
    if p1_files is None:
        p1_files = {}

    ## Ensure session exists
    CanvasSession = localSetup.canvasSession

    ## Preemptive rate-limit pause
    _PreemptiveRateLimitPauseIfNeeded(localSetup, p1_apiUrl)

    ## Dispatch
    try:
        if p1_apiCallType.lower() == "get":
            p1_payload.setdefault("per_page", 100)
            ResponseObject = CanvasSession.get(
                url=p1_apiUrl,
                headers=p1_header,
                params=p1_payload,
                timeout=RequestTimeoutSeconds,
            )

        elif p1_apiCallType.lower() == "post":
            if p1_payload and p1_files:
                ResponseObject = CanvasSession.post(
                    url=p1_apiUrl,
                    headers=p1_header,
                    json=p1_payload,
                    files=p1_files,
                    timeout=RequestTimeoutSeconds,
                )
            elif p1_payload:
                ResponseObject = CanvasSession.post(
                    url=p1_apiUrl,
                    headers=p1_header,
                    params=p1_payload,
                    timeout=RequestTimeoutSeconds,
                )
            else:
                ResponseObject = CanvasSession.post(
                    url=p1_apiUrl,
                    headers=p1_header,
                    timeout=RequestTimeoutSeconds,
                )

        elif p1_apiCallType.lower() == "put":
            if p1_payload:
                ResponseObject = CanvasSession.put(
                    url=p1_apiUrl,
                    headers=p1_header,
                    json=p1_payload,
                    timeout=RequestTimeoutSeconds,
                )
            else:
                ResponseObject = CanvasSession.put(
                    url=p1_apiUrl,
                    headers=p1_header,
                    timeout=RequestTimeoutSeconds,
                )

        elif p1_apiCallType.lower() == "delete":
            if p1_payload:
                ResponseObject = CanvasSession.delete(
                    url=p1_apiUrl,
                    headers=p1_header,
                    params=p1_payload,
                    timeout=RequestTimeoutSeconds,
                )
            else:
                ResponseObject = CanvasSession.delete(
                    url=p1_apiUrl,
                    headers=p1_header,
                    timeout=RequestTimeoutSeconds,
                )

        else:
            raise ValueError(f"Unsupported API call type: {p1_apiCallType}")

    except requests.exceptions.Timeout as TimeoutError:
        ## Send error email then raise so @retry can retry
        if getattr(localSetup, "logger", None):
            localSetup.logger.error(f"Timeout after {RequestTimeoutSeconds:.0f}s calling {p1_apiUrl}: {TimeoutError}")
        _SendTimeoutEmail(localSetup, p1_apiUrl, RequestTimeoutSeconds, TimeoutError)
        raise

    ## Update shared remaining tracker from headers (success or failure)
    _UpdateRateLimitRemainingFromResponse(ResponseObject)

    ## Handle 429 -> RateLimitExceeded (separate retry lane)
    if ResponseObject.status_code == 429:
        RetryAfterSeconds: Optional[float] = None

        RawRetryAfter = ResponseObject.headers.get("Retry-After")
        if RawRetryAfter:
            try:
                RetryAfterSeconds = float(RawRetryAfter)
            except (ValueError, TypeError):
                RetryAfterSeconds = None

        try:
            ResponseObject.close()
        except Exception:
            pass

        raise RateLimitExceeded(
            RetryAfter=RetryAfterSeconds,
            Message=f"Canvas API rate limit exceeded for {p1_apiUrl}.",
        )

    ## -------------------------
    ## Validate response codes
    ## -------------------------

    StatusCode = ResponseObject.status_code

    ## Keep historical behavior: 400 is allowed
    IsAllowed400 = (StatusCode == 400)

    ## Standard success is any 2xx
    IsSuccess2xx = (StatusCode >= 200 and StatusCode < 300)

    if not StatusCode or (not IsSuccess2xx and not IsAllowed400):
        if StatusCode:
            ## SPECIAL CASE: 409 Conflict for PUT/PATCH/POST
            if StatusCode == 409 and p1_apiCallType.lower() in ["put", "patch", "post"]:
                if getattr(localSetup, "logger", None):
                    localSetup.logger.warning(
                        f"Received 409 Conflict for {p1_apiCallType.upper()} {p1_apiUrl}. "
                        f"Checking for active existing item..."
                    )

                ## Retrieve current index
                IndexResponse, _ = makeApiCall(
                    localSetup,
                    p1_apiUrl=p1_apiUrl,
                    p1_header=p1_header,
                    p1_apiCallType="get",
                    firstPageOnly=True,
                )

                IndexData = IndexResponse.json() if hasattr(IndexResponse, "json") else []

                RequestedParams = {
                    Key[len("parameters["):-1]: Value
                    for Key, Value in p1_payload.items()
                    if Key.startswith("parameters[")
                }

                MatchingReport = next(
                    (
                        r for r in IndexData
                        if r.get("status") in ["running", "pending", "created"]
                        and r.get("parameters", {}) == RequestedParams
                    ),
                    None,
                )

                if MatchingReport:
                    if getattr(localSetup, "logger", None):
                        localSetup.logger.info(
                            "Found active report with matching parameters. Returning its status response."
                        )

                    ReportId = MatchingReport["id"]
                    StatusUrl = f"{p1_apiUrl}/{ReportId}"

                    StatusResponse, _ = makeApiCall(
                        localSetup,
                        p1_apiUrl=StatusUrl,
                        p1_header=p1_header,
                    )
                    return StatusResponse, []

                else:
                    if getattr(localSetup, "logger", None):
                        localSetup.logger.info(
                            f"409 received but no matching active report with parameters: {RequestedParams}. "
                            f"Retrying normally."
                        )

            try:
                ResponseObject.close()
            except Exception as CloseError:
                if getattr(localSetup, "logger", None):
                    localSetup.logger.warning(f"Failed to close API response before retry: {CloseError}")

            if p1_apiCallType.lower() != "delete":
                raise Exception(f"Failed API call to {p1_apiUrl}: HTTP {StatusCode}")
            else:
                if getattr(localSetup, "logger", None):
                    localSetup.logger.warning(f"Failed to delete resource at {p1_apiUrl}: HTTP {StatusCode}")
                return None, None

    ## -------------------------
    ## Pagination (unchanged behavior)
    ## -------------------------

    ResponseObjectList = []

    if hasattr(ResponseObject, "links") and "next" in getattr(ResponseObject, "links", {}) and not firstPageOnly:
        ResponseObjectList.append(ResponseObject)

        NextUrl = ResponseObject.links["next"]["url"]
        NextPage, NextPageList = makeApiCall(
            localSetup,
            p1_apiUrl=NextUrl,
            p1_header=p1_header,
            p1_payload=None,
            p1_files=p1_files,
            p1_apiCallType=p1_apiCallType,
            firstPageOnly=firstPageOnly,
        )

        if NextPageList:
            ResponseObjectList.extend(NextPageList)
        elif NextPage:
            ResponseObjectList.append(NextPage)

    return ResponseObject, ResponseObjectList

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
