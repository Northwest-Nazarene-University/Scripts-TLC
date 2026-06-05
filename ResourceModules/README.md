# ResourceModules

Shared libraries used by both Report and Action workflows throughout the Scripts-TLC automation suite.

---

## Purpose

`ResourceModules` provides the reusable foundation that every other module depends on:

- **Runtime environment setup** — directory creation, logging, date/time context, term logic
- **Canvas API client** — paginated API calls with rate-limit handling, retry logic, and connection pooling
- **Canvas Reports API** — request, poll, download, and load Canvas provisioning reports into pandas DataFrames
- **Microsoft Graph integration** — email sending, SharePoint/OneDrive file operations via MSAL
- **SFTP connectivity** — Slate incoming-student data retrieval, Simple Syllabus uploads
- **Error notification** — formatted error emails sent automatically on failure
- **Shared utilities** — file download, encryption, CSV reading, missing-value checks, threaded row processing

---

## Modules

### `Local_Setup.py` — Environment & Runtime Context

The `LocalSetup` class is instantiated at the start of every script and provides:

| Feature | Description |
|---------|-------------|
| **Path management** | Automatically locates the `Scripts_TLC` root directory, sets up `configPath`, `baseLogPath`, and adds all module directories to `sys.path` |
| **Logging** | Creates per-script log directories with separate Info, Warning, and Error log files, plus console output. Thread-safe logging via `logInfoThreadSafe()`, `logWarningThreadSafe()`, `logErrorThreadSafe()` |
| **Date context** | Exposes `dateDict` with `hour`, `day`, `weekDay`, `month`, `year`, `century`, `decade`, `lastDayOfCurrentMonth` |
| **Term logic** | Methods like `getCurrentTermCodes()`, `getNextTermCodes()`, `getPreviousTermCodes()`, `getCurrentSchoolYearTermCodes()`, `getPreviousSchoolYearTermCodes()`, `getSchoolYear()` using term dictionaries from config |
| **Resource paths** | `getInternalResourcePaths(key)` and `getExternalResourcePath(key)` for organized file storage |
| **Canvas session** | Provides a persistent `requests.Session` with connection pooling (10 connections) for all API calls |

**Initialization pattern used by all scripts:**

```python
from ResourceModules.Local_Setup import LocalSetup
from datetime import datetime

localSetup = LocalSetup(datetime.now(), __file__)
```

---

### `Api_Caller.py` — HTTP API Client with Rate-Limit Handling

Provides the core `makeApiCall()` function and `@retry` decorator used for all HTTP API interactions:

| Feature | Description |
|---------|-------------|
| **Canvas-aware rate limiting** | Detects Canvas URLs (`.instructure.com`), respects `X-Rate-Limit-Remaining` headers, and pauses pre-emptively when quota is low |
| **429 handling** | Reads `Retry-After` header; backs off with jitter when absent; coordinates across threads using a shared gate to avoid thundering-herd |
| **Concurrency control** | Global semaphore limits concurrent Canvas API requests (default 10) to prevent overwhelming the token bucket |
| **Pagination** | Automatically follows `Link: rel="next"` headers to collect all pages of paginated API responses |
| **Retry decorator** | `@retry(max_attempts=5, delay=5, backoff=2.0)` for transient failures |
| **Timeout** | Hard 600-second timeout per request to prevent indefinite hangs |

**Key function:**

```python
from ResourceModules.TLC_Common import makeApiCall

# Returns list of Response objects (one per page)
responses = makeApiCall(localSetup, url, header, method="GET", payload=None)
```

---

### `TLC_Common.py` — Shared Utility Functions

General-purpose helpers imported by nearly every script:

| Function | Description |
|----------|-------------|
| `getEncryptionKey(localSetup)` | Loads `ENCRYPTION_KEY` from the `.env` file in the config path |
| `downloadFile(localSetup, url, path, mode)` | Downloads a file with retry logic; auto-repairs corrupted Excel files; handles long path names |
| `flattenApiObjectToJsonList(localSetup, responses, url)` | Flattens paginated API response objects into a single JSON list |
| `isFileRecent(localSetup, path, maxAgeHours, isTermSensitive)` | Checks if a file exists and was modified within a time threshold; protects historical term files from overwrite |
| `loadExcelFile(path, sheetName)` | Loads Excel with multiple engine fallback (`openpyxl`, `xlrd`) |
| `isMissing(value)` / `isPresent(value)` | Unified missing-value checks handling `None`, `NaN`, empty strings, `"nan"` strings, and empty DataFrames/Series |
| `readTargetCsv(localSetup, filename, directory)` | Reads a target CSV from the Canvas internal resource path with encoding detection |
| `runThreadedRows(localSetup, df, func, maxWorkers)` | Processes DataFrame rows in parallel using ThreadPoolExecutor |
| `getDesignatorSettingsDict(localSetup, designator)` | Retrieves settings for a specific target designator from the Automated Outcome Tool Variables file |
| `getAutomatedOutcomeToolVariablesDf(localSetup)` | Loads the Automated Outcome Tool Variables Excel file as a DataFrame |
| `getDesignatorFilesByType(localSetup, designator, fileType)` | Finds designator-specific files by type from the external resource path |

---

### `TLC_Action.py` — Action-Level Helper Functions

Higher-level utilities used by Action modules:

| Function | Description |
|----------|-------------|
| `readCsvWithEncoding(filePath)` | Reads CSV trying `utf-8-sig` first, then `latin-1` fallback |
| `uploadToSimpleSyllabus(localSetup, localFilePath, remoteFileName)` | Uploads a file to Simple Syllabus via SFTP using SSH private key authentication |
| `hasChangedSinceLastUpload(localSetup, filePath)` | Checks whether a file has been modified since the last successful SFTP upload |
| `writeSuccessTag(localSetup, filePath)` / `removeStaleSuccessTag(...)` | Manages upload success marker files |
| `determineCourseWeek(startDate, endDate)` | Calculates what week a course is currently in |
| `retrieveDataForRelevantCommunication(...)` | Gathers Canvas data needed for outcome-related instructor communications |
| `getUniqueOutcomesAndOutcomeCoursesDict(...)` | Builds a mapping of outcomes to their associated courses |
| `removeMissingOutcomes(...)` | Filters out outcomes that are no longer valid |
| `addOutcomeToCourse(...)` | Attaches an outcome to a Canvas course via API |
| `requirePasswordReset(...)` / `terminateAllUserSessions(...)` | Security actions for user account management |
| `runCourseGradeExportsThreaded(...)` | Threaded grade export for Course Grades By Course report |

---

### `Canvas_Report.py` — Canvas Provisioning Reports Client

The `CanvasReport` class manages the full lifecycle of Canvas provisioning reports:

1. **Request** a report via the Canvas Reports API
2. **Poll** for completion status
3. **Download** the resulting CSV/ZIP file
4. **Load** into a pandas DataFrame

**Supported report types** (via class methods):

| Method | Report |
|--------|--------|
| `getTermsDf(localSetup)` | All terms |
| `getUsersDf(localSetup)` | All users |
| `getCoursesDf(localSetup, termCode)` | Courses (optionally filtered by term) |
| `getSectionsDf(localSetup, termCode)` | Sections |
| `getEnrollmentsDf(localSetup, termCode)` | Enrollments |
| `getUnpublishedCoursesDf(localSetup, termCode)` | Unpublished courses |
| `getAccountsDf(localSetup)` | Sub-accounts |
| `getCanvasUserLastAccessDf(localSetup)` | User last-access data |
| `getActiveOutcomeCoursesDf(localSetup, term, designator)` | Active outcome courses for a designator |

**Constructor parameters:**

```python
CanvasReport(localSetup, reportType, apiUrl=None, header=None, termCode=None,
             accountName="NNU", outputRoot=None, includeDeleted=None,
             filename=None, payload=None, endpoint="provisioning_csv",
             accountCanvasID=None)
```

---

### `Core_Microsoft_Api.py` — Microsoft Graph API Client

Provides email and file operations through the Microsoft Graph API:

| Function | Description |
|----------|-------------|
| `sendOutlookEmail(subject, body, recipients, localSetup)` | Sends an email via Microsoft Graph using the configured service account |
| `downloadSharedMicrosoftFile(localSetup, shareLink, outputPath)` | Downloads a file from a SharePoint/OneDrive shared link |
| `CoreMicrosoftAPI` class | Full Graph API client with auth, mail folder access, and drive operations |

**Authentication:** Uses MSAL (`azure.identity`) with `InteractiveBrowserCredential` and persistent token cache. Config is read from `Outlook_API_Config.cfg` and `OneDrive_and_Sharepoint_API_Config.cfg` in the config path.

---

### `Error_Email.py` — Automated Error Notifications

The `errorEmail` class captures exceptions and sends formatted notification emails:

```python
errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)

try:
    # ... script logic ...
except Exception as error:
    errorHandler.sendError("functionName", error)
```

**Features:**
- Thread-safe (uses `RLock` to prevent duplicate sends)
- Tracks sent errors to avoid repeated notifications for the same issue
- Includes script purpose, external requirements, function name, and full error details in the email body
- Sends via Microsoft Graph API (Outlook) to the configured service email account

---

### `Get_Slate_Info.py` — Slate SFTP Integration

Connects to NNU's Slate CRM SFTP server to download incoming-student CSV files:

| Feature | Description |
|---------|-------------|
| **Connection** | Uses `paramiko` with credentials from `Slate_Creds.json` and public key from `Slate_Public_Key.txt` |
| **File freshness** | Skips download if local files are recent (uses `isFileRecent()`) |
| **Output** | Saves CSVs to organized term/school-year directories under the Slate internal resource path |

**Main function:**

```python
from ResourceModules.Get_Slate_Info import getSlateInfo

filePaths = getSlateInfo("FA26")  # Returns list of local file paths
```

---

## Import Pattern

All Resource modules support two import styles to allow both direct execution and package imports:

```python
try:  # Direct execution
    from Local_Setup import LocalSetup
    from TLC_Common import makeApiCall
except ImportError:  # Package import
    from .Local_Setup import LocalSetup
    from .TLC_Common import makeApiCall
```

---

## Dependencies

| Package | Used By | Purpose |
|---------|---------|---------|
| `pandas` | All | DataFrame operations |
| `numpy` | TLC_Common | Numerical/NaN checks |
| `requests` | Api_Caller, TLC_Common | HTTP calls |
| `paramiko` | Get_Slate_Info, TLC_Action | SFTP connections |
| `python-dotenv` | TLC_Common | Loading `.env` files |
| `cryptography` | TLC_Common, TLC_Action, Core_Microsoft_Api | Fernet encryption |
| `msal` / `azure.identity` | Core_Microsoft_Api | Microsoft auth |
| `msgraph-sdk` | Core_Microsoft_Api | Graph API client |
| `openpyxl` | TLC_Common, Canvas_Report | Excel read/write |
| `python-dateutil` | TLC_Action | Date parsing |

---

## Usage Notes

- Every script initializes `LocalSetup` as its first action — this sets up paths, logging, and the Canvas session.
- These modules are imported by `IDT_Canvas_Primary.py`, all `ActionModules`, and all `ReportModules`.
- The config path (`Configs/`) must exist with the required configuration files before any module will function.
- Thread safety is built into logging and API rate-limiting — scripts use `threading.Thread` extensively for parallel processing.
