# Author: Bryce Miller - brycezmiller@nnu.edu
# Last Updated by: Bryce Miller

## Import Generic Moduels

import os, sys, re, pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlparse

# Ensure ResourceModules are importable when running from ActionModules
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

try: ## Irregular try clause, do not comment out in testing
    from Local_Setup import LocalSetup
    from Canvas_Report import CanvasReport
    from Error_Email import errorEmail
    from TLC_Common import isPresent, isMissing, downloadFile, makeApiCall
    from TLC_Action import readCsvWithEncoding, uploadToSimpleSyllabus, hasChangedSinceLastUpload, writeSuccessTag, removeStaleSuccessTag
except ImportError:
    # Fallback to relative imports if package layout differs
    from ResourceModules.Local_Setup import LocalSetup
    from ResourceModules.Canvas_Report import CanvasReport
    from ResourceModules.Error_Email import errorEmail
    from ResourceModules.TLC_Common import isPresent, isMissing, downloadFile, makeApiCall
    from ResourceModules.TLC_Action import readCsvWithEncoding, uploadToSimpleSyllabus, hasChangedSinceLastUpload, writeSuccessTag, removeStaleSuccessTag

## Get catalogToSimpleSyllabusConfig from configs
from Common_Configs import (
    catalogToSimpleSyllabusConfig,
    undgTermsWordsToCodesDict,
    undgTermsCodesToWordsDict,
    gradTermsWordsToCodesDict,
    gradTermsCodesToWordsDict,
)

## Set working directory
os.chdir(os.path.dirname(__file__))

# Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = os.path.basename(__file__)

scriptPurpose = r"""
This script retrieves TUG and GPS catalog course reports from CleanCatalog, combines them,
formats the data into the Simple Syllabus Course Extract format, and uploads the result
to Simple Syllabus via SFTP.
"""
externalRequirements = r"""
This script requires the following external resources:
1. Access to the Canvas API for retrieving course and instructor data.
2. The ResourceModules and ActionModules directories in the Scripts TLC directory for additional functionality.
3. Access to the Simple Syllabus SFTP server via SSH private key authentication.
4. The SSH private key file and its password stored in the config path.
"""

## Initialize LocalSetup and helpers from ResourceModules
localSetup = LocalSetup(datetime.now(), __file__)
errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)


## ==============================================================================
## CSV Helpers (script-specific)
## ==============================================================================

## This function normalizes spacing and delimiters in prerequisite/corequisite strings from the catalog
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
    """
    if not text:
        return text

    ## Step 1: Split apart words jammed at lowercase→Uppercase boundary
    text = re.sub(r'([a-z])([A-Z][a-z])', r'\1, \2', text)

    ## Step 2: Insert space after period jammed against a capital letter
    text = re.sub(r'\.([A-Z])', r'. \1', text)

    ## Step 3: Clean up artifacts
    text = re.sub(r',\s*,+', ',', text)
    text = re.sub(r'\.\s*,', '.', text)
    text = re.sub(r',\s*\.', '.', text)
    text = re.sub(r'\s+', ' ', text).strip()
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
        p1_combinedCatalogDf (pd.DataFrame): The combined catalog DataFrame.
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
        def _combineTextFragments(*labeledFragments) -> str:
            """
            Combine labeled text fragments into a single string.
            Each argument is either a tuple of (label: str, value: str) or a plain str.
            First non-empty fragment appears as-is; subsequent fragments are prefixed with their label + ": ".
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
                        combined.append(cleaned)
                    else:
                        if label:
                            combined.append(f"{label}: {cleaned}")
                        else:
                            combined.append(cleaned)
            result = ". ".join(combined)
            result = re.sub(r'\s+', ' ', result).strip()
            result = re.sub(r'\.\s*\.', '.', result)
            return result

        ## ======================================================================
        ## STEP 1: Determine term codes for the catalog school year
        ## ======================================================================

        currentSchoolYear = localSetup.getCurrentSchoolYear()
        currentStartYear = int(currentSchoolYear.split("-")[0])
        catalogStartYear = int(p1_catalogSchoolYear.split("-")[0])

        if catalogStartYear == currentStartYear:
            targetTermCodes = localSetup.getCurrentSchoolYearTermCodes()
        elif catalogStartYear > currentStartYear:
            targetTermCodes = localSetup.getNextSchoolYearTermCodes()
        else:
            errorMsg = (
                f"{functionName}: Catalog school year '{p1_catalogSchoolYear}' is in the past "
                f"(current school year is '{currentSchoolYear}'). Halting."
            )
            localSetup.logger.error(errorMsg)
            errorHandler.sendError(functionName, ValueError(errorMsg))
            raise ValueError(errorMsg)

        localSetup.logger.info(f"{functionName}: Catalog school year={p1_catalogSchoolYear}, "
                               f"current school year={currentSchoolYear}, "
                               f"target term codes={targetTermCodes}")

        ## ======================================================================
        ## STEP 2: Retrieve the Canvas terms DataFrame and build lookup
        ## ======================================================================

        termsDf = CanvasReport.getTermsDf(localSetup)
        termCodeToNameDict = {}
        if isPresent(termsDf):
            for _, row in termsDf.iterrows():
                termSisId = _safe_strip(row.get("term_id", ""))
                termName = _safe_strip(row.get("name", ""))
                if termSisId and termSisId in targetTermCodes and termName:
                    termCodeToNameDict[termSisId] = termName

        localSetup.logger.info(f"{functionName}: Term code to name mapping: {termCodeToNameDict}")

        undgTermCodes = [tc for tc in targetTermCodes if tc[:2] in undgTermsCodesToWordsDict]
        gradTermCodes = [tc for tc in targetTermCodes if tc[:2] in gradTermsCodesToWordsDict]

        ## ======================================================================
        ## STEP 3: Load Simple Syllabus Organizations and Canvas Accounts
        ##         hierarchy to build the Parent Organization lookup
        ## ======================================================================

        simpleSyllabusOrgsPath = os.path.join(localSetup.configPath, "Simple Syllabus Organizations.csv")
        simpleSyllabusOrgsDf = readCsvWithEncoding(simpleSyllabusOrgsPath)

        validOrgCanvasIds = set()
        canvasIdToOrgNameDict = {}
        for _, orgRow in simpleSyllabusOrgsDf.iterrows():
            orgCanvasId = orgRow.get("canvas_account_id")
            orgName = _safe_strip(orgRow.get("name", ""))
            if pd.notna(orgCanvasId) and orgName:
                validOrgCanvasIds.add(int(orgCanvasId))
                canvasIdToOrgNameDict[int(orgCanvasId)] = orgName

        accountsDf = CanvasReport.getAccountsDf(localSetup)
        accountParentDict = {}
        accountNameDict = {}
        if isPresent(accountsDf):
            for _, accRow in accountsDf.iterrows():
                canvasAccId = accRow.get("canvas_account_id")
                canvasParentId = accRow.get("canvas_parent_id")
                accName = _safe_strip(accRow.get("name", ""))
                if pd.notna(canvasAccId):
                    canvasAccId = int(canvasAccId)
                    accountParentDict[canvasAccId] = int(canvasParentId) if pd.notna(canvasParentId) else None
                    accountNameDict[canvasAccId] = accName

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
                parentId = accountParentDict.get(currentId)
                if parentId is None or parentId == 1:
                    if 1 in canvasIdToOrgNameDict:
                        return canvasIdToOrgNameDict[1]
                    return ""
                currentId = parentId
            return ""

        ## ======================================================================
        ## STEP 4: Retrieve Canvas courses for each target term
        ## ======================================================================

        canvasCourseInfoByTerm = {}
        for termCode in targetTermCodes:
            try:
                coursesDf = CanvasReport.getCoursesDf(localSetup, termCode)
                courseInfo = {}
                if isPresent(coursesDf):
                    for _, crsRow in coursesDf.iterrows():
                        courseId = crsRow.get("course_id")
                        canvasAccId = crsRow.get("canvas_account_id")
                        if pd.isna(courseId) or "_" not in str(courseId):
                            continue
                        parts = str(courseId).split("_")
                        if len(parts) >= 2:
                            courseCode = parts[1]
                            if courseCode not in courseInfo:
                                courseInfo[courseCode] = canvasAccId
                canvasCourseInfoByTerm[termCode] = courseInfo
            except Exception as termError:
                localSetup.logger.warning(f"{functionName}: Could not retrieve courses for term {termCode}: {termError}")
                canvasCourseInfoByTerm[termCode] = {}

        ## ======================================================================
        ## STEP 4.5: Expand rows where Title contains multiple codes via "/"
        ## ======================================================================

        expandedRows = []
        for _, row in p1_combinedCatalogDf.iterrows():
            rawTitle = _safe_strip(row.get("Title", ""))
            if "/" in rawTitle:
                codes = [code.strip() for code in rawTitle.split("/") if code.strip()]
                for code in codes:
                    newRow = row.copy()
                    newRow["Title"] = code
                    expandedRows.append(newRow)
            else:
                expandedRows.append(row)

        p1_combinedCatalogDf = pd.DataFrame(expandedRows).reset_index(drop=True)
        localSetup.logger.info(f"{functionName}: After expanding multi-code rows: {len(p1_combinedCatalogDf)} rows")

        ## ======================================================================
        ## STEP 5: Process each catalog row — build prerequisites/corequisites,
        ##         expand into per-term rows, and filter by Canvas presence
        ## ======================================================================

        extractRows = []

        for _, catalogRow in p1_combinedCatalogDf.iterrows():

            title = _safe_strip(catalogRow.get("Title", ""))
            if len(title) < 5:
                localSetup.logger.warning(f"{functionName}: Skipping row with short/missing Title: '{title}'")
                continue

            subject = title[:4]
            courseNumber = title[4:]

            name = _safe_strip(catalogRow.get("Name", "")).upper()
            classProgram = _safe_strip(catalogRow.get("Class Program", ""))
            description = _safe_strip(catalogRow.get("Description", ""))
            credits = catalogRow.get("Credits", "")
            catalogType = _safe_strip(catalogRow.get("Catalog Type", "")).lower()

            ## ── PREREQUISITE / COREQUISITE PROCESSING ──
            rawPrerequisites = _normalizeRequisiteSpacing(_safe_strip(catalogRow.get("Prerequisites", "")))
            rawPrerequisiteCourses = _normalizeRequisiteSpacing(_safe_strip(catalogRow.get("Prerequisite Courses", "")))
            rawPrereqOrCoreq = _normalizeRequisiteSpacing(_safe_strip(catalogRow.get("Prerequisite or Corequisite", "")))
            rawRecommendedPrereqs = _normalizeRequisiteSpacing(_safe_strip(catalogRow.get("Recommended Prerequisites", "")))
            rawCorequisites = _normalizeRequisiteSpacing(_safe_strip(catalogRow.get("Corequisites", "")))
            rawCorequisiteCourses = _normalizeRequisiteSpacing(_safe_strip(catalogRow.get("Corequisite Courses", "")))
            rawConcurrent = _normalizeRequisiteSpacing(_safe_strip(catalogRow.get("Concurrent", "")))
            rawConcurrentRequisite = _normalizeRequisiteSpacing(_safe_strip(catalogRow.get("Concurrent Requisite", "")))

            ## ── Rule 2: Remove cross-column duplicates, prioritizing prereq columns ──
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

            basePrereq = _combineTextFragments(
                ("Prerequisites", rawPrerequisites),
                ("Prerequisite Courses", rawPrerequisiteCourses)
            )
            if basePrereq:
                prereqParts.append(("", basePrereq))

            if rawPrereqOrCoreq:
                prereqParts.append(("Prerequisite or Corequisite", rawPrereqOrCoreq))
            if rawRecommendedPrereqs:
                prereqParts.append(("Recommended Prerequisites", rawRecommendedPrereqs))

            ## ── Build combined Corequisites string (Rule 3) ──
            combinedCoreq = _combineTextFragments(
                ("Corequisites", rawCorequisites),
                ("Corequisite Courses", rawCorequisiteCourses),
                ("Concurrent", rawConcurrent),
                ("Concurrent Requisite", rawConcurrentRequisite)
            )

            ## ── Final prerequisite and corequisite strings ──
            if prereqParts:
                firstLabel, firstContent = prereqParts[0]
                if firstLabel:
                    finalPrerequisites = f'<span class="block-node block-prefix" style="font-weight: 700;">{firstLabel}:</span> {firstContent}'
                else:
                    finalPrerequisites = firstContent
                for label, content in prereqParts[1:]:
                    if label:
                        finalPrerequisites += f'<p><span class="block-node block-prefix" style="font-weight: 700;">{label}:</span> {content}</p>'
                    else:
                        finalPrerequisites += f"<p>{content}</p>"
            else:
                finalPrerequisites = ""

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

                courseCode = title
                termCourseInfo = canvasCourseInfoByTerm.get(termCode, {})
                if courseCode not in termCourseInfo:
                    continue

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

        ## ======================================================================
        ## STEP 6: Build the output DataFrame and save
        ## ======================================================================

        courseExtractDf = pd.DataFrame(extractRows, columns=[
            "Term", "Subject", "Course Number", "Title", "Parent Organization",
            "Class Program", "Description", "Credits", "Prerequisites", "Corequisites"
        ])

        courseExtractDf.drop_duplicates(inplace=True)
        courseExtractDf.reset_index(drop=True, inplace=True)

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


## This function determines the school year for the catalog based on the provided catalog links
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

            reportPageResponse, _ = makeApiCall(
                localSetup,
                p1_apiUrl=reportPageUrl,
                p1_apiCallType="get"
            )
            reportPageHtml = reportPageResponse.text

            soup = BeautifulSoup(reportPageHtml, 'html.parser')
            
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
            parsedReportPageUrl = urlparse(reportPageUrl)
            catalogHomeUrl = f"{parsedReportPageUrl.scheme}://{parsedReportPageUrl.netloc}{catalogHomeHref}"

            homeResponse, _ = makeApiCall(
                localSetup,
                p1_apiUrl=catalogHomeUrl,
                p1_apiCallType="get"
            )

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

## Retrieve the TUG and GPS catalog course reports from the urls in catalogProduction
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

        combinedCatalogCourseReportDf = pd.DataFrame()
        for catalogType, filePath in catalogCourseReportsDict.items():
            catalogCourseReportsDf = readCsvWithEncoding(filePath)
            catalogCourseReportsDf['Catalog Type'] = catalogType
            if isMissing(combinedCatalogCourseReportDf):
                combinedCatalogCourseReportDf = catalogCourseReportsDf
            else:
                combinedCatalogCourseReportDf = pd.concat([combinedCatalogCourseReportDf, catalogCourseReportsDf], ignore_index=True)

        ## ── Check whether the new combined catalog differs from the last successfully uploaded version ──
        combinedCsvPath = os.path.join(catalogRootPath, "Combined Catalog Course Report.csv")
        successTagPath = os.path.join(catalogRootPath, "Combined Catalog Course Report_UPLOAD_SUCCESS.txt")

        if not hasChangedSinceLastUpload(combinedCatalogCourseReportDf, combinedCsvPath, successTagPath, localSetup):
            return combinedCatalogCourseReportDf, catalogSchoolYear, False

        combinedCatalogCourseReportDf.to_csv(combinedCsvPath, index=False, encoding='utf-8')
        removeStaleSuccessTag(successTagPath, localSetup)

        return combinedCatalogCourseReportDf, catalogSchoolYear, True

    except Exception as Error:
        localSetup.logger.error(f"{functionName}: {Error}")
        raise

## This function retrieves the catalog course reports, combines them into a simple syllabus format, and uploads the result to Simple Syllabus
def processCatalogCoursesAndUploadToSimpleSyllabus():
    functionName = "processCatalogCoursesAndUploadToSimpleSyllabus"
    try:
        combinedCatalogCourseReportDf, catalogSchoolYear, hasChanges = retrieveCatalogCourseReportsDfs()

        if not hasChanges:
            localSetup.logger.info(
                f"{functionName}: No changes detected in catalog data since last successful upload. Exiting early."
            )
            return

        courseExtractDf = formatCombinedCatalogForSimpleSyllabus(combinedCatalogCourseReportDf, catalogSchoolYear)

        baseCatalogPath = localSetup.getInternalResourcePaths("Catalog")
        catalogPath = os.path.join(baseCatalogPath, catalogSchoolYear)
        courseExtractFilePath = os.path.join(catalogPath, "Course Extract.csv")

        ## Upload the processed file to Simple Syllabus via SFTP
        uploadToSimpleSyllabus(courseExtractFilePath, localSetup, p1_errorHandler=errorHandler, p1_writeSuccessTag=True)

        localSetup.logger.info(f"{functionName}: Successfully processed catalog courses and uploaded to Simple Syllabus")
    except Exception as Error:
        localSetup.logger.error(f"{functionName}: {Error}")
        raise
        
## If the script is being run directly, execute the main function
if __name__ == "__main__":
    functionName = "main"
    try:
        processCatalogCoursesAndUploadToSimpleSyllabus()
    except Exception as Error:
        localSetup.logger.error(f"{functionName}: {Error}")
        errorHandler.sendError(functionName, Error)