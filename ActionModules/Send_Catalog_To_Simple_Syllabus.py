# Author: Bryce Miller - brycezmiller@nnu.edu
# Last Updated by: Bryce Miller

## Import Generic Modules
import os, sys
from datetime import datetime
import pandas as pd

## Add Script repository to syspath
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

# Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = __file__.replace(".py", "")
scriptPurpose = r"""
Send the course catalog to Simple Syllabus with properly mapped organizations.
This ensures course data is correctly categorized by subject/department based on Canvas account hierarchy.
"""
externalRequirements = r"""
To function properly this script requires:
- Access to course catalog file
- Simple Syllabus Organizations.csv in Configs TLC folder
- Canvas API access via CanvasReport
- All Accounts.csv from Canvas (Canvas Report)
"""

## Initialize LocalSetup and resource helpers
try:  ## Irregular try clause, do not comment out in testing
    from Local_Setup import LocalSetup
    from Canvas_Report import CanvasReport
    from Error_Email import errorEmail
    from TLC_Common import downloadFile, isFileRecent
    from Common_Configs import simpleSyllabusConfig
except ImportError:
    from ResourceModules.Local_Setup import LocalSetup
    from ResourceModules.Canvas_Report import CanvasReport
    from ResourceModules.Error_Email import errorEmail
    from ResourceModules.TLC_Common import downloadFile, isFileRecent
    from ResourceModules.Common_Configs import simpleSyllabusConfig

# Create LocalSetup and localSetup.logger
localSetup = LocalSetup(datetime.now(), __file__)
logger = localSetup.logger

## Setup error handler
errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)

## =========================================================================
## HELPER FUNCTIONS - Data Preparation
## =========================================================================

## This function extracts subject code and course number from raw title
def splitCourseCode(p1_rawTitle):
    p1_functionName = "splitCourseCode"
    try:
        ## Split on spaces and extract first two parts (subject code + number)
        parts = str(p1_rawTitle).split()
        if len(parts) >= 2:
            p2_subject = parts[0].upper()
            p2_courseNum = parts[1]
            return (p2_subject, p2_courseNum)
    except Exception as Error:
        logger.warning(f"\nFailed to parse course code from '{p1_rawTitle}': {Error}")
    return (p1_rawTitle, "")


## This function combines multiple fields into a single string, filtering out empty values
def combineFields(*p1_fields):
    p1_functionName = "combineFields"
    try:
        ## Filter out empty strings and join with spaces
        p2_combined = " ".join(
            str(p1_field).strip()
            for p1_field in p1_fields
            if p1_field and str(p1_field).strip()
        )
        return p2_combined
    except Exception as Error:
        logger.warning(f"\nFailed to combine fields: {Error}")
        return ""


## This function normalizes names for comparison operations
def _normalizeNameForMatch(p1_name):
    p1_functionName = "_normalizeNameForMatch"
    try:
        ## Normalization rules:
        ## - Convert to lowercase
        ## - Strip whitespace
        ## - Remove special characters
        ## - Replace multiple spaces with single space
        if not p1_name:
            return ""
        ## Convert to lowercase and strip
        p2_normalized = p1_name.lower().strip()
        ## Remove common punctuation
        for p2_char in [".", ",", "-", "_"]:
            p2_normalized = p2_normalized.replace(p2_char, "")
        ## Collapse multiple spaces
        p2_normalized = " ".join(p2_normalized.split())
        return p2_normalized
    except Exception as Error:
        logger.warning(f"\nFailed to normalize name: {Error}")
        return p1_name


## =========================================================================
## CONFIG-BASED HELPERS
## =========================================================================

def _getCatalogUrlFromConfig(audience="gps", environment="production"):
    """
    Resolve the CleanCatalog CSV URL using simpleSyllabusConfig.

    audience: "gps" or "tug"
    environment: "production" or "staging"
    """
    p1_functionName = "_getCatalogUrlFromConfig"
    envKey = "catalogProduction" if environment == "production" else "catalogStaging"
    try:
        url = simpleSyllabusConfig[envKey][audience]
        logger.info(
            f"\nUsing CleanCatalog URL from simpleSyllabusConfig: "
            f"env='{envKey}', audience='{audience}' -> {url}"
        )
        return url
    except Exception as Error:
        errorHandler.sendError(p1_functionName, str(Error))
        raise


## =========================================================================
## EXTERNAL DATA LOADING FUNCTIONS
## =========================================================================

## This function loads Simple Syllabus organizations from the CSV config file
def _loadsimpSylNamesAndCanvasAccIdsDictFromCsv():
    functionName = "_loadsimpSylNamesAndCanvasAccIdsDictFromCsv"
    try:
        ## Config path for Simple Syllabus Organizations CSV
        simpSylNamesAndCanvasAccIdsDictCsvPath = os.path.join(
            localSetup.configPath,
            "Simple Syllabus Organizations.csv",
        )
        logger.info(
            f"\nLoading Simple Syllabus organizations from {simpSylNamesAndCanvasAccIdsDictCsvPath}..."
        )
        ## Check if file exists
        if not os.path.exists(simpSylNamesAndCanvasAccIdsDictCsvPath):
            raise FileNotFoundError(
                f"\nSimple Syllabus Organizations CSV not found at {simpSylNamesAndCanvasAccIdsDictCsvPath}"
            )
        ## Read the CSV file
        orgsDf = pd.read_csv(simpSylNamesAndCanvasAccIdsDictCsvPath)
        ## Convert DataFrame to dictionary with canvas_account_id as key
        simpSylNamesAndCanvasAccIdsDict = {}
        for index, row in orgsDf.iterrows():
            simpSylAccName = row.get("name")
            ## Skip rows with NaN account_id
            if pd.isna(simpSylAccName):
                continue
            simpSylNamesAndCanvasAccIdsDict[simpSylAccName] = int(row.get("canvas_account_id", ""))
        logger.info(
            f"\nLoaded {len(simpSylNamesAndCanvasAccIdsDict)} Simple Syllabus organizations from CSV"
        )
        return simpSylNamesAndCanvasAccIdsDict
    except Exception as Error:
        errorHandler.sendError(functionName, str(Error))
        raise


## This function loads all Canvas accounts for parent lookup
def _loadAllCanvasAccounts():
    p1_functionName = "_loadAllCanvasAccounts"
    try:
        logger.info("\nLoading all Canvas accounts for parent hierarchy lookup...")
        ## Use CanvasReport to get the accounts DataFrame
        rawAllAccountsDf = CanvasReport.getAccountsDf(localSetup)

        ## Filter out entries that were not created_by_sis
        allAccountsDf = rawAllAccountsDf[
            rawAllAccountsDf["created_by_sis"] == True
            ].copy()

        ## Convert to dictionary with canvas_account_id as key for quick lookup
        accountsDict = {}
        for index, row in allAccountsDf.iterrows():
            accountId = str(row.get("canvas_account_id", ""))
            parentId = (
                str(row.get("canvas_parent_id", ""))
                if pd.notna(row.get("canvas_parent_id"))
                else None
            )
            accountsDict[accountId] = {
                "name": row.get("name", ""),
                "canvas_account_id": accountId,
                "parent_id": parentId,
            }
        logger.info(f"\nLoaded {len(accountsDict)} Canvas accounts")
        return accountsDict
    except Exception as Error:
        errorHandler.sendError(p1_functionName, str(Error))
        raise


## =========================================================================
## ACCOUNT HIERARCHY AND MAPPING FUNCTIONS
## =========================================================================

## This function finds the parent organization by walking up the account hierarchy
def _findParentOrgAccountId(
    p1_courseAccountId,
    p2_allAccountsDict,
    p3_simpSylNamesAndCanvasAccIdsDictDict,
    p4_visitedAccounts=None,
):
    p1_functionName = "_findParentOrgAccountId"
    try:
        ## Initialize visited accounts set to prevent infinite loops
        if p4_visitedAccounts is None:
            p4_visitedAccounts = set()

        ## Get the account ID as string for comparison
        p2_currentAccountId = str(p1_courseAccountId)

        ## Check if we've already visited this account (prevent circular references)
        if p2_currentAccountId in p4_visitedAccounts:
            logger.warning(
                f"\nCircular reference detected in account hierarchy at account {p2_currentAccountId}"
            )
            return ""

        p4_visitedAccounts.add(p2_currentAccountId)

        ## Check if current account is in Simple Syllabus orgs
        if p2_currentAccountId in p3_simpSylNamesAndCanvasAccIdsDictDict:
            logger.info(
                f"\nFound Simple Syllabus org for account "
                f"{p2_currentAccountId}: {p3_simpSylNamesAndCanvasAccIdsDictDict[p2_currentAccountId]['name']}"
            )
            return p2_currentAccountId

        ## If not in orgs, try to find parent
        if p2_currentAccountId not in p2_allAccountsDict:
            logger.warning(
                f"\nAccount {p2_currentAccountId} not found in Canvas accounts"
            )
            return ""

        p2_parentAccountId = p2_allAccountsDict[p2_currentAccountId].get("parent_id")

        ## If no parent or parent is None, we've reached the top
        if not p2_parentAccountId or p2_parentAccountId == "None":
            logger.warning(
                f"\nNo Simple Syllabus org found in hierarchy for account {p2_currentAccountId}"
            )
            return ""

        ## Recursively check parent
        logger.info(
            f"\nAccount {p2_currentAccountId} not in orgs, checking parent {p2_parentAccountId}"
        )
        return _findParentOrgAccountId(
            p2_parentAccountId, p2_allAccountsDict, p3_simpSylNamesAndCanvasAccIdsDictDict, p4_visitedAccounts
        )
    except Exception as Error:
        errorHandler.sendError(p1_functionName, str(Error))
        return ""


## =========================================================================
## CORE TRANSFORMATION FUNCTIONS
## =========================================================================

## This function builds mapping of Canvas account IDs to Simple Syllabus organization IDs from CATALOG ONLY
## KEY CHANGE: This function now accepts p1_catalogDf as a parameter, ensuring we ONLY map
## accounts that exist in the catalog, not all Canvas accounts
def buildAccountOrgMap(p2_allAccountsDict, p3_simpSylNamesAndCanvasAccIdsDictDict):
    p1_functionName = "buildAccountOrgMap"
    try:
        logger.info(
            "\nBuilding Canvas account-to-organization mapping from catalog courses..."
        )


        ## STEP 1: Map each catalog account to its Simple Syllabus org
        accountOrgMap = {}
        for canvasAccountId in p3_simpSylNamesAndCanvasAccIdsDictDict.values(): ## The keys are the corresponding canvas_account_ids
            try:
                ## Find the org account by walking up the hierarchy
                p2_orgAccountId = _findParentOrgAccountId(
                    canvasAccountId, p2_allAccountsDict, p3_simpSylNamesAndCanvasAccIdsDictDict
                )
                if p2_orgAccountId:
                    accountOrgMap[canvasAccountId] = p2_orgAccountId
                    orgName = p3_simpSylNamesAndCanvasAccIdsDictDict[p2_orgAccountId]["name"]
                    logger.info(
                        f"\nMapped account {canvasAccountId} to organization "
                        f"{p2_orgAccountId} ({orgName})"
                    )
                else:
                    logger.warning(
                        f"\nFailed to find organization for account {canvasAccountId}"
                    )
            except Exception as Error:
                logger.warning(f"\nError mapping account {canvasAccountId}: {Error}")
                continue

        logger.info(
            f"\nSuccessfully built mapping for {len(accountOrgMap)} accounts"
        )
        return accountOrgMap
    except Exception as Error:
        errorHandler.sendError(p1_functionName, str(Error))
        raise


## This function expands course titles containing slashes into separate rows
def expandSlashTitles(p1_catalogDf):
    p1_functionName = "expandSlashTitles"
    try:
        p2_expandedRows = []
        for p2_index, p2_row in p1_catalogDf.iterrows():
            p2_title = str(p2_row.get("course_code", ""))
            ## Check if title contains a slash
            if "/" in p2_title:
                p2_parts = p2_title.split("/")
                ## Create a separate row for each part
                for p2_part in p2_parts:
                    p2_newRow = p2_row.copy()
                    p2_newRow["course_code"] = p2_part.strip()
                    p2_expandedRows.append(p2_newRow)
            else:
                p2_expandedRows.append(p2_row)
        p2_expandedDf = pd.DataFrame(p2_expandedRows)
        logger.info(
            f"\nExpanded {len(p1_catalogDf)} rows to {len(p2_expandedDf)} rows after slash expansion"
        )
        return p2_expandedDf
    except Exception as Error:
        errorHandler.sendError(p1_functionName, str(Error))
        raise


## =========================================================================
## SIS HELPERS FOR PROD/STAGING RESOLUTION
## =========================================================================

def _normalize_course_code(code):
    """Uppercase and remove whitespace for robust matching."""
    return "".join(str(code).upper().split())


def _extract_course_code_from_course_id(course_id):
    """
    From SIS course_id like 'FA2026_ACCT2060_01' -> 'ACCT2060'.
    Adjust if your pattern differs.
    """
    parts = str(course_id).split("_")
    if len(parts) >= 2:
        return parts[1]
    return ""


def _get_school_year(date_obj):
    """
    Academic year helper: if month >= 7, take that year; otherwise year - 1.
    e.g., 2026-08-10 -> 2026 school year, 2027-01-10 (spring) -> 2026 school year.
    """
    if date_obj.month >= 7:
        return date_obj.year
    return date_obj.year - 1


def _dedupeProdStaging(p1_catalogDf, activeSisCourses):
    """
    For each course_code, if there are both production and staging catalog rows,
    use SIS school-year info to pick:
      - current school year -> production
      - coming school year  -> staging
    """
    # Build SIS mapping: normalized course code -> set of school years
    sisDf = activeSisCourses.copy()
    sisDf["sis_course_code"] = sisDf["course_id"].apply(
        _extract_course_code_from_course_id
    )
    sisDf["sis_course_code_norm"] = sisDf["sis_course_code"].apply(
        _normalize_course_code
    )

    sisDf["start_date_dt"] = pd.to_datetime(sisDf["start_date"])
    sisDf["school_year"] = sisDf["start_date_dt"].dt.date.apply(_get_school_year)

    sisYearByCourseCode = (
        sisDf.groupby("sis_course_code_norm")["school_year"]
        .apply(lambda s: set(int(y) for y in s.dropna().unique()))
        .to_dict()
    )

    # Normalize course_code in catalog
    p1_catalogDf = p1_catalogDf.copy()
    p1_catalogDf["course_code_norm"] = p1_catalogDf["course_code"].apply(
        _normalize_course_code
    )

    today = datetime.now().date()
    currentSchoolYear = _get_school_year(today)
    nextSchoolYear = currentSchoolYear + 1

    def pick_row(group):
        code_norm = group["course_code_norm"].iloc[0]
        years = sisYearByCourseCode.get(code_norm, set())
        envs = set(group["environment"])

        has_current = currentSchoolYear in years
        has_next = nextSchoolYear in years

        # Current school year -> prefer production
        if has_current and "production" in envs:
            return group[group["environment"] == "production"].iloc[0]

        # Coming school year -> prefer staging
        if has_next and "staging" in envs:
            return group[group["environment"] == "staging"].iloc[0]

        # Fallbacks:
        if len(envs) == 1:
            # Only one env, just return first
            return group.iloc[0]

        # Otherwise, prefer production if present
        prod_rows = group[group["environment"] == "production"]
        if not prod_rows.empty:
            return prod_rows.iloc[0]

        # Last resort
        return group.iloc[0]

    deduped = (
        p1_catalogDf.groupby("course_code_norm", as_index=False)
        .apply(pick_row)
        .reset_index(drop=True)
    )

    return deduped


## This function constructs rows for upload to Simple Syllabus
## KEY CHANGE: This function now uses SIS to filter & resolve prod vs staging
def buildOutputRows(p1_catalogDf, accountOrgMap, p3_simpSylNamesAndCanvasAccIdsDictDict):
    p1_functionName = "buildOutputRows"
    try:
        logger.info("\nBuilding output rows for Simple Syllabus upload...")

        # --- NEW: load active SIS courses and restrict catalog to those ---
        sisPath = os.path.join(
            localSetup.getExternalResourcePath("SIS"),
            "canvas_course.csv",
        )
        if not os.path.exists(sisPath):
            raise FileNotFoundError(f"\nSIS canvas_course.csv not found at {sisPath}")

        rawSisDf = pd.read_csv(sisPath)
        activeSisCourses = rawSisDf[rawSisDf["status"] == "active"].copy()

        # Build normalized course_code from SIS course_id
        activeSisCourses["sis_course_code"] = activeSisCourses["course_id"].apply(
            _extract_course_code_from_course_id
        )
        activeSisCourses["sis_course_code_norm"] = activeSisCourses[
            "sis_course_code"
        ].apply(_normalize_course_code)

        # Normalize catalog course_code
        p1_catalogDf = p1_catalogDf.copy()
        p1_catalogDf["course_code_norm"] = p1_catalogDf["course_code"].apply(
            _normalize_course_code
        )

        # Filter catalog: only keep courses whose code appears in active SIS
        validCodes = set(activeSisCourses["sis_course_code_norm"].unique())
        p1_catalogDf = p1_catalogDf[
            p1_catalogDf["course_code_norm"].isin(validCodes)
        ].copy()

        logger.info(
            f"\nFiltered catalog to {len(p1_catalogDf)} rows present in active SIS courses"
        )

        # Dedupe prod vs staging per course_code using school-year logic
        p1_catalogDf = _dedupeProdStaging(p1_catalogDf, activeSisCourses)

        logger.info(
            f"\nAfter prod/staging school-year resolution, {len(p1_catalogDf)} catalog rows remain"
        )
        # --- END NEW SIS logic ---

        # Validate input
        p2_requiredColumns = ["course_code", "title", "account_id"]
        p2_missingColumns = [
            p2_col for p2_col in p2_requiredColumns if p2_col not in p1_catalogDf.columns
        ]
        if p2_missingColumns:
            raise ValueError(f"\nMissing required columns: {p2_missingColumns}")

        p2_outputRows = []

        for p2_index, p2_row in p1_catalogDf.iterrows():
            try:
                ## Get account ID and look up org
                p2_courseAccountId = str(p2_row.get("account_id", "")).strip()
                p2_courseCode = str(p2_row.get("course_code", ""))
                if not p2_courseAccountId or p2_courseAccountId.upper() == "NAN":
                    logger.warning(
                        f"\nNo account ID for course '{p2_courseCode}'. Skipping."
                    )
                    continue

                ## Get the organization account ID from mapping
                p2_orgAccountId = accountOrgMap.get(p2_courseAccountId, "")
                if not p2_orgAccountId:
                    logger.warning(
                        f"\nNo organization mapping for account '{p2_courseAccountId}' "
                        f"in course '{p2_courseCode}'. Skipping."
                    )
                    continue

                ## Get organization name
                p2_orgName = p3_simpSylNamesAndCanvasAccIdsDictDict.get(p2_orgAccountId, {}).get(
                    "name", ""
                )

                ## Build output row
                p2_outputRow = {
                    "course_code": p2_courseCode,
                    "course_title": p2_row.get("title", ""),
                    "organization_id": p2_orgAccountId,
                    "organization_name": p2_orgName,
                    "canvas_account_id": p2_courseAccountId,
                    "sis_course_id": p2_row.get("sis_course_id", ""),
                    "description": p2_row.get("description", ""),
                    "environment": p2_row.get("environment", ""),
                    "audience": p2_row.get("audience", ""),
                }
                p2_outputRows.append(p2_outputRow)

            except Exception as Error:
                logger.warning(
                    f"\nFailed to build row for course "
                    f"'{p2_row.get('course_code', 'UNKNOWN')}': {Error}"
                )
                continue

        p2_outputDf = pd.DataFrame(p2_outputRows)
        logger.info(f"\nBuilt {len(p2_outputDf)} output rows for upload")
        return p2_outputDf
    except Exception as Error:
        errorHandler.sendError(p1_functionName, str(Error))
        raise


## =========================================================================
## DOWNLOAD AND UPLOAD FUNCTIONS
## =========================================================================

## This function downloads catalog from URL and returns as DataFrame
def downloadCatalog(p1_url, p2_localPath):
    p1_functionName = "downloadCatalog"
    try:
        logger.info(f"\nDownloading catalog from {p1_url} to {p2_localPath}...")

        ## If the file isn't recent
        if not isFileRecent(localSetup, p2_localPath):

            # Use TLC_Common.downloadFile to make a GET call to the URL
            downloadFile(localSetup, p1_url, p2_localPath)

        # Now read the downloaded CSV
        p2_catalogDf = pd.read_csv(p2_localPath)
        logger.info(f"\nDownloaded {len(p2_catalogDf)} courses from {p1_url}")
        return p2_catalogDf
    except Exception as Error:
        errorHandler.sendError(p1_functionName, str(Error))
        raise


def downloadAllCatalogs():
    """
    Download all four CleanCatalog exports (prod/staging x gps/tug),
    tag each with environment/audience, and return a single combined DataFrame.
    """
    p1_functionName = "downloadAllCatalogs"
    try:
        baseDir = os.path.join(
            localSetup.getInternalResourcePaths("Simple_Syllabus"),
            "Catalog_Export",
        )
        os.makedirs(baseDir, exist_ok=True)

        combos = [
            ("production", "gps"),
            ("production", "tug"),
            ("staging", "gps"),
            ("staging", "tug"),
        ]

        allDfs = []

        for environment, audience in combos:
            url = _getCatalogUrlFromConfig(audience=audience, environment=environment)
            localPath = os.path.join(
                baseDir,
                f"{environment}_{audience}_catalog.csv",
            )

            catalogDf = downloadCatalog(url, localPath)

            # Tag with environment & audience
            catalogDf["environment"] = environment
            catalogDf["audience"] = audience

            allDfs.append(catalogDf)

        if not allDfs:
            raise ValueError("\nNo catalog data downloaded")

        combinedDf = pd.concat(allDfs, ignore_index=True)
        logger.info(
            f"\nCombined all catalogs into a single DataFrame with {len(combinedDf)} rows"
        )
        return combinedDf

    except Exception as Error:
        errorHandler.sendError(p1_functionName, str(Error))
        raise


## This function uploads output rows to Simple Syllabus
def uploadToSimpleSyllabus(p1_outputDf):
    p1_functionName = "uploadToSimpleSyllabus"
    try:
        if p1_outputDf.empty:
            raise ValueError("\nOutput DataFrame is empty")

        logger.info(f"\nUploading {len(p1_outputDf)} courses to Simple Syllabus...")

        # Define a local output directory inside the Simple_Syllabus internal path
        outputDir = os.path.join(
            localSetup.getInternalResourcePaths("Simple_Syllabus"),
            "Catalog_Export",
        )
        os.makedirs(outputDir, exist_ok=True)

        p2_outputFile = os.path.join(outputDir, "SimpleSyllabus_Catalog_Export.csv")
        p1_outputDf.to_csv(p2_outputFile, index=False)
        logger.info(
            f"\nSuccessfully wrote {len(p1_outputDf)} courses to local file {p2_outputFile}"
        )

        # EXAMPLE: How you'd access SFTP config when you're ready to actually push the file
        sftpConfig = simpleSyllabusConfig.get("sftp", {})
        logger.info(
            f"\nSimple Syllabus SFTP config loaded for future use: "
            f"host={sftpConfig.get('host')}, remote_dir={sftpConfig.get('remote_dir')}"
        )

        # TODO: Implement actual SFTP upload using sftpConfig and LocalSetup-managed private key.

        return True
    except Exception as Error:
        errorHandler.sendError(p1_functionName, str(Error))
        return False


## =========================================================================
## MAIN ORCHESTRATION FUNCTION
## =========================================================================

## This function orchestrates the entire catalog synchronization process
## Process flow:
## 1. Load Simple Syllabus organizations from CSV
## 2. Load all Canvas accounts for hierarchy lookup
## 3. Download catalog from CleanCatalog (via simpleSyllabusConfig)
## 4. Expand slash titles
## 5. Build account-to-org mapping from catalog (KEY CHANGE)
## 6. Build output rows using the mapping (with SIS filtering and prod/staging logic)
## 7. Upload to Simple Syllabus
def sendCatalogToSimpleSyllabus(
    p1_catalogUrl=None,
    audience="gps",           # used only if you pass a manual URL
    environment="production", # used only if you pass a manual URL
):
    p1_functionName = "sendCatalogToSimpleSyllabus"
    try:
        logger.info("\n\nStarting catalog synchronization with Simple Syllabus...")

        ## STEP 1: Load Simple Syllabus organizations from CSV
        p2_simpSylNamesAndCanvasAccIdsDictDict = _loadsimpSylNamesAndCanvasAccIdsDictFromCsv()
        if not p2_simpSylNamesAndCanvasAccIdsDictDict:
            raise ValueError("\nFailed to load Simple Syllabus organizations")

        ## STEP 2: Load all Canvas accounts for parent hierarchy lookup
        p2_allAccountsDict = _loadAllCanvasAccounts()
        if not p2_allAccountsDict:
            raise ValueError("\nFailed to load Canvas accounts")

        ## STEP 3: Download and combine ALL catalogs (prod/staging x gps/tug)
        if p1_catalogUrl:
            # Manual single-URL test path
            p2_localDownloadPath = os.path.join(
                localSetup.getInternalResourcePaths("Simple_Syllabus"),
                "Catalog_Export",
                "catalog_download_manual.csv",
            )
            p2_catalogDf = downloadCatalog(p1_catalogUrl, p2_localDownloadPath)
            p2_catalogDf["environment"] = environment
            p2_catalogDf["audience"] = audience
        else:
            p2_catalogDf = downloadAllCatalogs()

        if p2_catalogDf.empty:
            raise ValueError("\nDownloaded catalog is empty")

        logger.info(f"\nDownloaded/combined {len(p2_catalogDf)} total catalog rows")

        ## STEP 4: Expand slash titles
        p2_expandedCatalogDf = expandSlashTitles(p2_catalogDf)
        logger.info(
            f"\nExpanded to {len(p2_expandedCatalogDf)} courses after slash expansion"
        )

        ## STEP 5: Build account-to-org mapping from CATALOG ONLY
        accountOrgMap = buildAccountOrgMap(
            p2_allAccountsDict, p2_simpSylNamesAndCanvasAccIdsDictDict
        )
        if not accountOrgMap:
            raise ValueError("\nFailed to build account-to-org mapping")

        ## STEP 6: Build output rows (includes SIS filtering and prod/staging resolution)
        p2_outputDf = buildOutputRows(
            p2_expandedCatalogDf, accountOrgMap, p2_simpSylNamesAndCanvasAccIdsDictDict
        )
        if p2_outputDf.empty:
            raise ValueError("\nNo valid output rows generated")

        ## STEP 7: Upload to Simple Syllabus
        p2_uploadSuccess = uploadToSimpleSyllabus(p2_outputDf)
        if p2_uploadSuccess:
            logger.info("\nCatalog synchronization completed successfully")
            return True
        else:
            logger.error("\nCatalog upload failed")
            return False
    except Exception as Error:
        errorHandler.sendError(p1_functionName, str(Error))
        return False


## =========================================================================
## MAIN ENTRY POINT
## =========================================================================

if __name__ == "__main__":
    logger.info("\n\nStarting Send_Catalog_To_Simple_Syllabus script")
    try:
        p1_success = sendCatalogToSimpleSyllabus()
        if p1_success:
            logger.info("\nScript completed successfully")
        else:
            logger.error("\nScript completed with errors")
    except Exception as Error:
        errorHandler.sendError("main", str(Error))
        logger.error(f"\nScript failed: {Error}")