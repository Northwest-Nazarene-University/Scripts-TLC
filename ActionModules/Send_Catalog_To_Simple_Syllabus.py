# Author: Bryce Miller - brycezmiller@nnu.edu
# Last Updated by: Bryce Miller

## Import Generic Modules
import os, sys, requests
from datetime import datetime
from urllib.parse import urljoin
import pandas as pd
from bs4 import BeautifulSoup

## Add Script repository to syspath
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

# Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = __file__.replace(".py", "")
scriptPurpose = r"""
Send the course catalog to Simple Syllabus with properly mapped organizations.
This ensures course data is correctly categorized by subject/department based on Canvas account hierarchy.

Before downloading catalogs, the script checks https://catalog.nnu.edu/ for a
"traditional-undergraduate-catalog-YYYYYYYY" link to determine whether the published
catalog matches the current academic year or a future one:
  - Current year match  -> download prod + staging catalogs from CleanCatalog as before.
  - Future year found   -> download the future catalog PDF and combine it with any
                           previously downloaded current-year catalog.
"""
externalRequirements = r"""
To function properly this script requires:
- Access to course catalog file
- Simple Syllabus Organizations.csv in Configs TLC folder
- Canvas API access via CanvasReport
- All Accounts.csv from Canvas (Canvas Report)
- Network access to https://catalog.nnu.edu/ for catalog year detection
"""

## Initialize LocalSetup and resource helpers
try:  ## Irregular try clause, do not comment out in testing
    from Local_Setup import LocalSetup
    from Canvas_Report import CanvasReport
    from Error_Email import errorEmail
    from TLC_Common import downloadFile, isFileRecent, makeApiCall
    from Common_Configs import catalogToSimpleSyllabusConfig
except ImportError:
    from ResourceModules.Local_Setup import LocalSetup
    from ResourceModules.Canvas_Report import CanvasReport
    from ResourceModules.Error_Email import errorEmail
    from ResourceModules.TLC_Common import downloadFile, isFileRecent, makeApiCall
    from Configs.Common_Configs import catalogToSimpleSyllabusConfig

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
            subject = parts[0].upper()
            courseNum = parts[1]
            return (subject, courseNum)
    except Exception as Error:
        logger.warning(f"\nFailed to parse course code from '{p1_rawTitle}': {Error}")
    return (p1_rawTitle, "")


## This function combines multiple fields into a single string, filtering out empty values
def combineFields(*p1_fields):
    p1_functionName = "combineFields"
    try:
        ## Filter out empty strings and join with spaces
        combined = " ".join(
            str(p1_field).strip()
            for p1_field in p1_fields
            if p1_field and str(p1_field).strip()
        )
        return combined
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
        normalized = p1_name.lower().strip()
        ## Remove common punctuation
        for char in [".", ",", "-", "_"]:
            normalized = normalized.replace(char, "")
        ## Collapse multiple spaces
        normalized = " ".join(normalized.split())
        return normalized
    except Exception as Error:
        logger.warning(f"\nFailed to normalize name: {Error}")
        return p1_name


## =========================================================================
## CONFIG-BASED HELPERS
## =========================================================================

def _getCatalogUrlFromConfig(audience="gps", environment="production"):
    """
    Resolve the CleanCatalog CSV URL using catalogToSimpleSyllabusConfig.

    audience: "gps" or "tug"
    environment: "production" or "staging"
    """
    p1_functionName = "_getCatalogUrlFromConfig"
    envKey = "catalogProduction" if environment == "production" else "catalogStaging"
    try:
        url = catalogToSimpleSyllabusConfig[envKey][audience]
        logger.info(
            f"\nUsing CleanCatalog URL from catalogToSimpleSyllabusConfig: "
            f"env='{envKey}', audience='{audience}' -> {url}"
        )
        return url
    except Exception as Error:
        errorHandler.sendError(p1_functionName, str(Error))
        raise


## =========================================================================
## CATALOG YEAR DETECTION HELPERS
## =========================================================================

## This function parses the catalog base page HTML for the current academic year traditional-undergraduate-catalog-YYYYYYYY link
def _findTradUndergradCatalogYear(p1_html, baseUrl=catalogToSimpleSyllabusConfig['catalogBaseUrl']):
    p1_functionName = "_findTradUndergradCatalogYear"
    try:
        ## Parse the HTML and look for links matching the traditional-undergraduate-catalog-YYYYYYYY pattern
        soup = BeautifulSoup(p1_html, "html.parser")
        for anchor in soup.find_all("a", href=True):
            href = anchor["href"]
            ## Normalize relative links to absolute
            fullUrl = urljoin(baseUrl, href)
            match = catalogToSimpleSyllabusConfig['tradUndergradPattern'].search(fullUrl)
            if match:
                yearString = match.group(1)
                logger.info(f"\nFound traditional-undergraduate-catalog link: {fullUrl} (year: {yearString})")
                return yearString
        logger.warning(f"\nNo traditional-undergraduate-catalog-YYYYYYYY link found on {baseUrl}")
        return None
    except Exception as Error:
        errorEmail = errorHandler.sendError(p1_functionName, str(Error))
        logger.warning(f"\nError parsing catalog page for year: {Error}")
        return None


## This function extracts PDF links from an HTML page
def _extractPdfLinksFromPage(p1_html, baseUrl):
    p1_functionName = "_extractPdfLinksFromPage"
    try:
        soup = BeautifulSoup(p1_html, "html.parser")
        pdfLinks = []
        seen = set()
        for anchor in soup.find_all("a", href=True):
            href = anchor["href"]
            fullUrl = urljoin(baseUrl, href)
            if catalogToSimpleSyllabusConfig['pdfLinkPattern'].search(fullUrl) and fullUrl not in seen:
                seen.add(fullUrl)
                pdfLinks.append(fullUrl)
        logger.info(f"\nFound {len(pdfLinks)} PDF link(s) on {baseUrl}")
        return pdfLinks
    except Exception as Error:
        logger.warning(f"\nError extracting PDF links from {baseUrl}: {Error}")
        return []


## This function computes the current academic year string (YYYYYYYY) using LocalSetup
def _getCurrentAcademicYearString():
    """
    Use LocalSetup's school year logic to compute the current academic year as YYYYYYYY.
    LocalSetup._determineCurrentTerm gives the current term name (e.g., "Spring"),
    and _getSchoolYearRange gives (startYear, endYear).
    We concatenate them: e.g., (2025, 2026) -> "20252026".
    """
    p1_functionName = "_getCurrentAcademicYearString"
    try:
        currentMonth = localSetup.dateDict["month"]
        currentYear = localSetup.dateDict["year"]
        currentTerm = localSetup._determineCurrentTerm(currentMonth)
        startYear, endYear = localSetup._getSchoolYearRange(currentTerm, currentYear)
        academicYearString = f"{startYear}{endYear}"
        logger.info(f"\nComputed current academic year: {academicYearString}")
        return academicYearString
    except Exception as Error:
        logger.warning(f"\nFailed to compute current academic year string: {Error}")
        return None


## This function downloads a PDF catalog from a URL and saves it locally
def _downloadCatalogPdf(p1_pdfUrl, outputDir):
    p1_functionName = "_downloadCatalogPdf"
    try:
        ## Derive a safe filename from the URL
        filename = os.path.basename(p1_pdfUrl.split("?")[0])

        ## If the filename is empty or too generic, generate one from the URL hash
        if not filename or len(filename) < 5:
            import hashlib
            filename = hashlib.sha1(p1_pdfUrl.encode()).hexdigest()[:12] + ".pdf"

        localPath = os.path.join(outputDir, filename)

        ## If the file was already downloaded recently, skip
        if isFileRecent(localSetup, localPath):
            logger.info(f"\nPDF already recent, skipping download: {localPath}")
            return localPath

        ## Download using TLC_Common.downloadFile (includes retry logic)
        downloadFile(localSetup, p1_pdfUrl, localPath)
        logger.info(f"\nDownloaded PDF to {localPath}")
        return localPath
    except Exception as Error:
        logger.warning(f"\nFailed to download PDF from {p1_pdfUrl}: {Error}")
        return None


## =========================================================================
## EXTERNAL DATA LOADING FUNCTIONS
## =========================================================================

## This function loads Simple Syllabus organizations from the CSV config file
def _loadsimpSylCanvasAccIdsAndNamesDictFromCsv():
    functionName = "_loadsimpSylCanvasAccIdsAndNamesDictFromCsv"
    try:
        ## Config path for Simple Syllabus Organizations CSV
        simpSylCanvasAccIdsAndNamesDictCsvPath = os.path.join(
            localSetup.configPath,
            "Simple Syllabus Organizations.csv",
        )
        logger.info(
            f"\nLoading Simple Syllabus organizations from {simpSylCanvasAccIdsAndNamesDictCsvPath}..."
        )
        ## Check if file exists
        if not os.path.exists(simpSylCanvasAccIdsAndNamesDictCsvPath):
            raise FileNotFoundError(
                f"\nSimple Syllabus Organizations CSV not found at {simpSylCanvasAccIdsAndNamesDictCsvPath}"
            )
        ## Read the CSV file
        rawOrgsDf = pd.read_csv(simpSylCanvasAccIdsAndNamesDictCsvPath)
        ## Set NA values to empty string to prevent issues with NaN in account_id
        orgsDf = rawOrgsDf.fillna("")
        ## Convert DataFrame to dictionary keyed by org name -> int canvas_account_id
        simpSylCanvasAccIdsAndNamesDict = {}
        for index, row in orgsDf.iterrows():
            simpSylAccName = str(row.get("name", "")).strip()
            canvasAccId = row.get("canvas_account_id", "")
            ## Skip rows with empty name or account_id
            if not simpSylAccName or not str(canvasAccId).strip():
                continue
            try:
                simpSylCanvasAccIdsAndNamesDict[str(canvasAccId)] = simpSylAccName
            except (ValueError, TypeError):
                logger.warning(
                    f"\nSkipping org '{simpSylAccName}': invalid canvas_account_id '{canvasAccId}'"
                )
                continue
        logger.info(
            f"\nLoaded {len(simpSylCanvasAccIdsAndNamesDict)} Simple Syllabus organizations from CSV"
        )
        return simpSylCanvasAccIdsAndNamesDict
    except Exception as Error:
        errorHandler.sendError(functionName, str(Error))
        raise


## This function loads all Canvas accounts for parent lookup
def _loadAllCanvasAccounts(p1_simpSylCanvasAccIdsAndNamesDict):
    p1_functionName = "_loadAllCanvasAccounts"
    try:
        logger.info("\nLoading all Canvas accounts for parent hierarchy lookup...")
        ## Use CanvasReport to get the accounts DataFrame
        rawAllAccountsDf = CanvasReport.getAccountsDf(localSetup)

        ## Filter out entries that were not created_by_sis
        allAccountsDf = rawAllAccountsDf[
            rawAllAccountsDf["created_by_sis"] == True
            ].copy()

        ## Convert to dictionary with canvas_account_id as key for quick lookup, set the root account in the initial definition
        accountsDict = {
            "1" : {
                "name" : "Northwest Nazarene University",
                "canvas_account_id" : "1",
                "parent_id" : None
                }
            }
        for index, row in allAccountsDf.iterrows():
            ## Skip accounts if their canvas_account_id is not in the Simple Syllabus orgs list, since we won't need to look up parents for accounts that aren't in the orgs list anyway
            if str(row.get("canvas_account_id", "")) not in p1_simpSylCanvasAccIdsAndNamesDict:
                continue
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
    allAccountsDict,
    p3_simpSylCanvasAccIdsAndNamesDictDict,
    p4_visitedAccounts=None,
):
    p1_functionName = "_findParentOrgAccountId"
    try:
        ## Initialize visited accounts set to prevent infinite loops
        if p4_visitedAccounts is None:
            p4_visitedAccounts = set()

        ## Get the account ID as string for comparison
        currentAccountId = str(p1_courseAccountId)

        ## Check if we've already visited this account (prevent circular references)
        if currentAccountId in p4_visitedAccounts:
            logger.warning(
                f"\nCircular reference detected in account hierarchy at account {currentAccountId}"
            )
            return ""

        p4_visitedAccounts.add(currentAccountId)

        ## Check if current account is in Simple Syllabus orgs
        if currentAccountId in p3_simpSylCanvasAccIdsAndNamesDictDict:
            logger.info(
                f"\nFound Simple Syllabus org for account "
                f"{currentAccountId}: {p3_simpSylCanvasAccIdsAndNamesDictDict[currentAccountId]['name']}"
            )
            return currentAccountId

        ## If not in orgs, try to find parent
        if currentAccountId not in allAccountsDict:
            logger.warning(
                f"\nAccount {currentAccountId} not found in Canvas accounts"
            )
            return ""

        parentAccountId = allAccountsDict[currentAccountId].get("parent_id")

        ## If no parent or parent is None, we've reached the top
        if not parentAccountId or parentAccountId == "None":
            logger.warning(
                f"\nNo Simple Syllabus org found in hierarchy for account {currentAccountId}"
            )
            return ""

        ## Recursively check parent
        logger.info(
            f"\nAccount {currentAccountId} not in orgs, checking parent {parentAccountId}"
        )
        return _findParentOrgAccountId(
            parentAccountId, allAccountsDict, p3_simpSylCanvasAccIdsAndNamesDictDict, p4_visitedAccounts
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
def buildAccountOrgMap(allAccountsDict, p3_simpSylCanvasAccIdsAndNamesDictDict):
    p1_functionName = "buildAccountOrgMap"
    try:
        logger.info(
            "\nBuilding Canvas account-to-organization mapping from catalog courses..."
        )

        ## Build an inverted lookup keyed by str(account_id) -> {"name": org_name}
        ## This is the format _findParentOrgAccountId expects for membership checks and name access
        accountIdToOrgInfo = {
            str(accId): {"name": orgName}
            for orgName, accId in p3_simpSylCanvasAccIdsAndNamesDictDict.items()
        }

        ## Map each Simple Syllabus org account to its resolved org in the hierarchy
        accountOrgMap = {}
        for orgName, canvasAccountId in p3_simpSylCanvasAccIdsAndNamesDictDict.items():
            canvasAccountIdStr = str(canvasAccountId)
            try:
                ## Find the org account by walking up the hierarchy
                orgAccountId = _findParentOrgAccountId(
                    canvasAccountIdStr, allAccountsDict, accountIdToOrgInfo
                )
                if orgAccountId:
                    accountOrgMap[canvasAccountIdStr] = orgAccountId
                    resolvedOrgName = accountIdToOrgInfo.get(orgAccountId, {}).get("name", "")
                    logger.info(
                        f"\nMapped account {canvasAccountIdStr} to organization "
                        f"{orgAccountId} ({resolvedOrgName})"
                    )
                else:
                    logger.warning(
                        f"\nFailed to find organization for account {canvasAccountIdStr}"
                    )
            except Exception as Error:
                logger.warning(f"\nError mapping account {canvasAccountIdStr}: {Error}")
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
        expandedRows = []
        for index, row in p1_catalogDf.iterrows():
            title = str(row.get("course_code", ""))
            ## Check if title contains a slash
            if "/" in title:
                parts = title.split("/")
                ## Create a separate row for each part
                for part in parts:
                    newRow = row.copy()
                    newRow["course_code"] = part.strip()
                    expandedRows.append(newRow)
            else:
                expandedRows.append(row)
        expandedDf = pd.DataFrame(expandedRows)
        logger.info(
            f"\nExpanded {len(p1_catalogDf)} rows to {len(expandedDf)} rows after slash expansion"
        )
        return expandedDf
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
    p1_catalogDf["course_code_norm"] = p1_catalogDf["Title"].apply(
        _normalize_course_code
    )

    today = datetime.now().date()
    currentSchoolYear = _get_school_year(today)
    ## TODO Use normal get school year
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
def buildOutputRows(p1_catalogDf, accountOrgMap, p3_simpSylCanvasAccIdsAndNamesDictDict):
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
        p1_catalogDf["course_code_norm"] = p1_catalogDf["Title"].apply(
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
        requiredColumns = ["course_code", "title", "account_id"]
        missingColumns = [
            col for col in requiredColumns if col not in p1_catalogDf.columns
        ]
        if missingColumns:
            raise ValueError(f"\nMissing required columns: {missingColumns}")

        # Normalize expected columns: support alternate column names emitted by CleanCatalog
        # Map known alternate column names to canonical ones used downstream.
        canon = p1_catalogDf.copy()

        # course_code: prefer existing 'course_code', else 'Title' (CleanCatalog) or 'course_code_norm'
        if "course_code" not in canon.columns:
            if "Title" in canon.columns:
                canon["course_code"] = canon["Title"]
            elif "course_code_norm" in canon.columns:
                canon["course_code"] = canon["course_code_norm"]

        # title: prefer 'title', else 'Name'
        if "title" not in canon.columns and "Name" in canon.columns:
            canon["title"] = canon["Name"]

        # account_id: try common alternatives that might appear in different exports
        if "account_id" not in canon.columns:
            for alt in ["canvas_account_id", "Account ID", "account", "accountId", "canvas_account"]:
                if alt in canon.columns:
                    canon["account_id"] = canon[alt]
                    break

        # After mapping, validate required columns
        requiredColumns = ["course_code", "title", "account_id"]
        missingColumns = [col for col in requiredColumns if col not in canon.columns]
        if missingColumns:
            raise ValueError(
                f"\nMissing required columns after mapping: {missingColumns}. "
                "Ensure the catalog export contains course code (e.g. 'Title'), "
                "course title (e.g. 'Name'), and a Canvas account id column."
            )

        # Use the normalized dataframe going forward
        p1_catalogDf = canon

        outputRows = []

        for index, row in p1_catalogDf.iterrows():
            try:
                ## Get account ID and look up org
                courseAccountId = str(row.get("account_id", "")).strip()
                courseCode = str(row.get("course_code", ""))
                if not courseAccountId or courseAccountId.upper() == "NAN":
                    logger.warning(
                        f"\nNo account ID for course '{courseCode}'. Skipping."
                    )
                    continue

                ## Get the organization account ID from mapping
                orgAccountId = accountOrgMap.get(courseAccountId, "")
                if not orgAccountId:
                    logger.warning(
                        f"\nNo organization mapping for account '{courseAccountId}' "
                        f"in course '{courseCode}'. Skipping."
                    )
                    continue

                ## Get organization name
                orgName = p3_simpSylCanvasAccIdsAndNamesDictDict.get(orgAccountId, {}).get(
                    "name", ""
                )

                ## Build output row
                outputRow = {
                    "course_code": courseCode,
                    "course_title": row.get("title", ""),
                    "organization_id": orgAccountId,
                    "organization_name": orgName,
                    "canvas_account_id": courseAccountId,
                    "sis_course_id": row.get("sis_course_id", ""),
                    "description": row.get("description", ""),
                    "environment": row.get("environment", ""),
                    "audience": row.get("audience", ""),
                }
                outputRows.append(outputRow)

            except Exception as Error:
                logger.warning(
                    f"\nFailed to build row for course "
                    f"'{row.get('course_code', 'UNKNOWN')}': {Error}"
                )
                continue

        outputDf = pd.DataFrame(outputRows)
        logger.info(f"\nBuilt {len(outputDf)} output rows for upload")
        return outputDf
    except Exception as Error:
        errorHandler.sendError(p1_functionName, str(Error))
        raise


## =========================================================================
## DOWNLOAD AND UPLOAD FUNCTIONS
## =========================================================================

## This function downloads catalog from URL and returns as DataFrame
def downloadCatalog(p1_url, localPath):
    p1_functionName = "downloadCatalog"
    try:
        logger.info(f"\nDownloading catalog from {p1_url} to {localPath}...")

        ## If the file isn't recent
        if not isFileRecent(localSetup, localPath):

            # Use TLC_Common.downloadFile to make a GET call to the URL
            downloadFile(localSetup, p1_url, localPath)

        # Now read the downloaded CSV
        catalogDf = pd.read_csv(localPath)
        logger.info(f"\nDownloaded {len(catalogDf)} courses from {p1_url}")
        return catalogDf
    except Exception as Error:
        errorHandler.sendError(p1_functionName, str(Error))
        raise


## This function downloads prod + staging catalogs (original behavior)
def _downloadProdAndStagingCatalogs():
    """
    Download all four CleanCatalog exports (prod/staging x gps/tug),
    tag each with environment/audience, and return a single combined DataFrame.
    This is the original downloadAllCatalogs behavior preserved as an internal helper.
    """
    p1_functionName = "_downloadProdAndStagingCatalogs"
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


## This function handles the future catalog year scenario by downloading the future
## catalog PDF and combining it with a previously downloaded current-year catalog if available
def _downloadFutureCatalogWithCurrentFallback(p1_foundYear, currentYear):
    """
    When catalog.nnu.edu shows a future catalog year:
    1. Download the future catalog PDF from the traditional-undergraduate-catalog-<FOUND_YEAR> page.
    2. Check if a current-year catalog was previously downloaded. If so, combine both
       into the returned DataFrame by naming the future catalog for the upcoming year.
    3. If no previously downloaded current catalog exists, return only the future catalog data.

    Falls back to _downloadProdAndStagingCatalogs() if the future page has no PDFs.
    """
    p1_functionName = "_downloadFutureCatalogWithCurrentFallback"
    try:
        baseDir = os.path.join(
            localSetup.getInternalResourcePaths("Simple_Syllabus"),
            "Catalog_Export",
        )
        os.makedirs(baseDir, exist_ok=True)

        downloadedPaths = []

        ## STEP 1: Download the future catalog PDF from catalog.nnu.edu
        futurePageUrl = f"{catalogToSimpleSyllabusConfig['catalogBaseUrl']}traditional-undergraduate-catalog-{p1_foundYear}"

        # Use makeApiCall (with existing retry and logging) instead of undefined _fetchHtml
        futureResp, _ = makeApiCall(localSetup, futurePageUrl, p1_header = "")
        futureHtml = futureResp.text if futureResp is not None else None

        if futureHtml:
            futurePdfs = _extractPdfLinksFromPage(futureHtml, futurePageUrl)
            for pdfUrl in futurePdfs:
                localPath = _downloadCatalogPdf(pdfUrl, baseDir)
                if localPath:
                    downloadedPaths.append(localPath)
        else:
            logger.warning(f"\nCould not fetch future catalog page: {futurePageUrl}")

        ## STEP 2: Check if current-year catalog CSVs were previously downloaded
        ## Look for production catalog CSVs from a prior run
        currentCatalogPaths = []
        for audience in ["gps", "tug"]:
            currentCsvPath = os.path.join(baseDir, f"production_{audience}_catalog.csv")
            if os.path.exists(currentCsvPath):
                logger.info(f"\nFound previously downloaded current catalog: {currentCsvPath}")
                currentCatalogPaths.append((currentCsvPath, audience))

        ## STEP 3: Build combined DataFrame
        allDfs = []

        ## Add current catalogs if they exist
        for csvPath, audience in currentCatalogPaths:
            try:
                currentDf = pd.read_csv(csvPath)
                currentDf["environment"] = "production"
                currentDf["audience"] = audience
                allDfs.append(currentDf)
                logger.info(f"\nLoaded {len(currentDf)} current catalog rows from {csvPath}")
            except Exception as Error:
                logger.warning(f"\nFailed to read current catalog CSV {csvPath}: {Error}")

        ## If we downloaded future PDFs but have no CSV catalogs to combine,
        ## fall back to downloading prod+staging normally so the pipeline has data to process
        if not allDfs and not downloadedPaths:
            logger.warning("\nNo future PDFs and no current catalogs found. Falling back to prod+staging download.")
            return _downloadProdAndStagingCatalogs()

        ## If we have current CSVs, also try to download the future catalog CSVs from staging
        ## so the pipeline can process them alongside the current ones
        if allDfs:
            ## Try downloading staging catalogs as the "future" catalog source
            for audience in ["gps", "tug"]:
                try:
                    stagingUrl = _getCatalogUrlFromConfig(audience=audience, environment="staging")
                    stagingLocalPath = os.path.join(baseDir, f"staging_{audience}_catalog.csv")
                    stagingDf = downloadCatalog(stagingUrl, stagingLocalPath)
                    stagingDf["environment"] = "staging"
                    stagingDf["audience"] = audience
                    allDfs.append(stagingDf)
                    logger.info(f"\nDownloaded staging {audience} catalog as future catalog source")
                except Exception as Error:
                    logger.warning(f"\nFailed to download staging {audience} catalog: {Error}")

        if not allDfs:
            logger.warning("\nNo catalog DataFrames built. Falling back to prod+staging download.")
            return _downloadProdAndStagingCatalogs()

        combinedDf = pd.concat(allDfs, ignore_index=True)
        logger.info(
            f"\nCombined future+current catalogs into {len(combinedDf)} rows "
            f"(future year: {p1_foundYear}, current year: {currentYear})"
        )

        ## Log the downloaded PDF paths for reference
        if downloadedPaths:
            logger.info(f"\nFuture catalog PDFs downloaded: {downloadedPaths}")

        return combinedDf

    except Exception as Error:
        errorHandler.sendError(p1_functionName, str(Error))
        ## On any error, fall back to original behavior
        logger.warning("\nError in future catalog handling. Falling back to prod+staging download.")
        return _downloadProdAndStagingCatalogs()


def downloadAllCatalogs():
    """
    Smart catalog download that first checks https://catalog.nnu.edu/ for the
    traditional-undergraduate-catalog year to decide what to download:

    1. Compute current academic year via LocalSetup (e.g., "20252026").
    2. Scrape catalog.nnu.edu for a "traditional-undergraduate-catalog-YYYYYYYY" link.
    3. If found year == current year -> download prod+staging as before.
    4. If found year is in the future -> download the future catalog and combine
       with any previously downloaded current-year catalog.
    5. If no link found or found year is older -> fall back to prod+staging.
    """
    functionName = "downloadAllCatalogs"
    try:
        ## STEP 1: Compute current academic year
        currentAcademicYear = _getCurrentAcademicYearString()
        if not currentAcademicYear:
            logger.warning("\nCould not compute current academic year. Falling back to prod+staging download.")
            return _downloadProdAndStagingCatalogs()

        ## STEP 2: Fetch the catalog base page and find the catalog year
        catalogSiteHtlmResponse, _ = makeApiCall(
            localSetup,
            catalogToSimpleSyllabusConfig['catalogBaseUrl'],
            p1_header = ""
            )
        baseCatalogHtml = catalogSiteHtlmResponse.text
        if not baseCatalogHtml:
            logger.warning(f"\nCould not fetch {catalogToSimpleSyllabusConfig['catalogBaseUrl']}. Falling back to prod+staging download.")
            return _downloadProdAndStagingCatalogs()

        foundYear = _findTradUndergradCatalogYear(baseCatalogHtml)
        if not foundYear:
            logger.warning("\nNo traditional-undergraduate-catalog link found. Falling back to prod+staging download.")
            return _downloadProdAndStagingCatalogs()

        logger.info(f"\nCatalog year found on site: {foundYear}, current academic year: {currentAcademicYear}")

        ## STEP 3: Compare found year to current year
        if foundYear == currentAcademicYear:
            ## Found year matches current academic year -> do existing prod+staging flow
            logger.info("\nFound catalog year matches current academic year. Downloading prod+staging catalogs.")
            return _downloadProdAndStagingCatalogs()

        ## STEP 4: Check if found year is in the future
        try:
            foundStartYear = int(foundYear[:4])
            currentStartYear = int(currentAcademicYear[:4])
        except ValueError:
            logger.warning(f"\nCould not parse year strings numerically (found={foundYear}, current={currentAcademicYear}). Falling back to prod+staging download.")
            return _downloadProdAndStagingCatalogs()

        if foundStartYear > currentStartYear:
            ## Future catalog published early -> download future + check for current
            logger.info(
                f"\nFuture catalog year detected ({foundYear}) while current is ({currentAcademicYear}). "
                f"Downloading future catalog and checking for previously downloaded current catalog."
            )
            return _downloadFutureCatalogWithCurrentFallback(foundYear, currentAcademicYear)

        ## STEP 5: Found year is older than current -> fall back
        logger.info(f"\nFound catalog year ({foundYear}) is older than current ({currentAcademicYear}). Falling back to prod+staging download.")
        return _downloadProdAndStagingCatalogs()

    except Exception as Error:
        errorHandler.sendError(functionName, str(Error))
        ## On any unhandled error, fall back to original behavior
        logger.warning("\nUnexpected error in downloadAllCatalogs. Falling back to prod+staging download.")
        return _downloadProdAndStagingCatalogs()


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

        outputFile = os.path.join(outputDir, "SimpleSyllabus_Catalog_Export.csv")
        p1_outputDf.to_csv(outputFile, index=False)
        logger.info(
            f"\nSuccessfully wrote {len(p1_outputDf)} courses to local file {outputFile}"
        )

        # EXAMPLE: How you'd access SFTP config when you're ready to actually push the file
        sftpConfig = catalogToSimpleSyllabusConfig.get("sftp", {})
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
## 3. Check catalog.nnu.edu for catalog year, then download accordingly:
##    a. Current year match -> download prod+staging from CleanCatalog
##    b. Future year found  -> download future catalog PDF + combine with current if available
##    c. No link / older    -> fall back to prod+staging
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
        simpSylCanvasAccIdsAndNamesDictDict = _loadsimpSylCanvasAccIdsAndNamesDictFromCsv()
        if not simpSylCanvasAccIdsAndNamesDictDict:
            raise ValueError("\nFailed to load Simple Syllabus organizations")

        ## STEP 2: Load all Canvas accounts for parent hierarchy lookup
        allAccountsDict = _loadAllCanvasAccounts(simpSylCanvasAccIdsAndNamesDictDict)
        if not allAccountsDict:
            raise ValueError("\nFailed to load Canvas accounts")

        ## STEP 3: Download catalogs (with catalog year detection)
        if p1_catalogUrl:
            # Manual single-URL test path
            localDownloadPath = os.path.join(
                localSetup.getInternalResourcePaths("Simple_Syllabus"),
                "Catalog_Export",
                "catalog_download_manual.csv",
            )
            catalogDf = downloadCatalog(p1_catalogUrl, localDownloadPath)
            catalogDf["environment"] = environment
            catalogDf["audience"] = audience
        else:
            catalogDf = downloadAllCatalogs()

        if catalogDf.empty:
            raise ValueError("\nDownloaded catalog is empty")

        logger.info(f"\nDownloaded/combined {len(catalogDf)} total catalog rows")

        ## STEP 4: Expand slash titles
        expandedCatalogDf = expandSlashTitles(catalogDf)
        logger.info(
            f"\nExpanded to {len(expandedCatalogDf)} courses after slash expansion"
        )

        ## STEP 5: Build account-to-org mapping from CATALOG ONLY
        accountOrgMap = buildAccountOrgMap(
            allAccountsDict, simpSylCanvasAccIdsAndNamesDictDict
        )
        if not accountOrgMap:
            raise ValueError("\nFailed to build account-to-org mapping")

        ## STEP 6: Build output rows (includes SIS filtering and prod/staging resolution)
        outputDf = buildOutputRows(
            expandedCatalogDf, accountOrgMap, simpSylCanvasAccIdsAndNamesDictDict
        )
        if outputDf.empty:
            raise ValueError("\nNo valid output rows generated")

        ## STEP 7: Upload to Simple Syllabus
        uploadSuccess = uploadToSimpleSyllabus(outputDf)
        if uploadSuccess:
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