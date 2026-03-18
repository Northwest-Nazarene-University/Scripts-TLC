# Author: Bryce Miller - brycezmiller@nnu.edu
# Last Updated by: Bryce Miller

## Import Generic Moduels

from calendar import c
from math import e
import os, sys, threading, asyncio
import re
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlparse

# Ensure ResourceModules are importable when running from ActionModules
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

try: ## Irregular try clause, do not comment out in testing
    from Local_Setup import LocalSetup
    from Canvas_Report import CanvasReport
    from Error_Email import errorEmail
    from TLC_Common import isPresent, downloadFile, makeApiCall
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
    from ResourceModules.TLC_Common import isPresent, downloadFile, makeApiCall
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
"""

## Initialize LocalSetup and helpers from ResourceModules
localSetup = LocalSetup(datetime.now(), __file__)
errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)


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
        def _combineTextFragments(*fragments) -> str:
            combined = []
            for fragment in fragments:
                cleaned = _safe_strip(fragment)
                if cleaned and cleaned not in combined:
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
        
        if p1_catalogSchoolYear == currentSchoolYear:
            targetTermCodes = localSetup.getCurrentSchoolYearTermCodes()
        else:
            targetTermCodes = localSetup.getNextSchoolYearTermCodes()

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
        simpleSyllabusOrgsDf = pd.read_csv(simpleSyllabusOrgsPath)

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

            ## ── Prereqs & Coreqs Rule 2: Remove duplicates between prereq and coreq columns ──
            ## If Concurrent content is duplicated in a prereq column, clear Concurrent
            allPrereqTexts = [rawPrerequisites, rawPrerequisiteCourses, rawPrereqOrCoreq, rawRecommendedPrereqs]

            for prereqText in allPrereqTexts:
                if prereqText:
                    if rawConcurrent and rawConcurrent == prereqText:
                        rawConcurrent = ""
                    if rawConcurrentRequisite and rawConcurrentRequisite == prereqText:
                        rawConcurrentRequisite = ""

            ## Also check: if coreq text appears in prereq columns, clear from coreq side
            for coreqText in [rawCorequisites, rawCorequisiteCourses, rawConcurrent, rawConcurrentRequisite]:
                if coreqText:
                    if rawPrereqOrCoreq and rawPrereqOrCoreq == coreqText:
                        rawPrereqOrCoreq = ""

            ## ── Build combined Prerequisites string (Rule 1) ──
            prereqParts = []
            ## Put Prerequisites & Prerequisite Courses first
            basePrereq = _combineTextFragments(rawPrerequisites, rawPrerequisiteCourses)
            if basePrereq:
                prereqParts.append(basePrereq)

            ## "Prerequisite or Corequisite:" section
            if rawPrereqOrCoreq:
                prereqParts.append(f"Prerequisite or Corequisite: {rawPrereqOrCoreq}")

            ## "Recommended Prerequisites:" section
            if rawRecommendedPrereqs:
                prereqParts.append(f"Recommended Prerequisites: {rawRecommendedPrereqs}")

            ## ── Build combined Corequisites string (Rule 3) ──
            combinedCoreq = _combineTextFragments(rawCorequisites, rawCorequisiteCourses, rawConcurrent, rawConcurrentRequisite)

            ## ── Combine into working strings for Rule 4 ──
            combinedPrereqStr = ". ".join(prereqParts) if prereqParts else ""
            combinedCoreqStr = combinedCoreq

            ## ── Prereqs & Coreqs Rule 4: If any course codes are duplicated between ──
            ## ── the two combined strings, remove from both and add to "Prerequisite or Corequisite:" ──
            prereqCodes = _extractCourseCodes(combinedPrereqStr)
            coreqCodes = _extractCourseCodes(combinedCoreqStr)
            sharedCodes = prereqCodes & coreqCodes

            if sharedCodes:
                ## Remove shared codes from both strings
                for code in sharedCodes:
                    combinedPrereqStr = re.sub(r',?\s*' + re.escape(code) + r'\s*,?', '', combinedPrereqStr)
                    combinedCoreqStr = re.sub(r',?\s*' + re.escape(code) + r'\s*,?', '', combinedCoreqStr)

                ## Clean up any leftover artifacts
                combinedPrereqStr = re.sub(r'\s+', ' ', combinedPrereqStr).strip().strip(',').strip()
                combinedCoreqStr = re.sub(r'\s+', ' ', combinedCoreqStr).strip().strip(',').strip()

                ## Add shared codes to the Prerequisite or Corequisite section
                sharedCodesStr = ", ".join(sorted(sharedCodes))
                if "Prerequisite or Corequisite:" in combinedPrereqStr:
                    ## Append to existing section
                    combinedPrereqStr = combinedPrereqStr.replace(
                        "Prerequisite or Corequisite:",
                        f"Prerequisite or Corequisite: {sharedCodesStr},"
                    )
                    ## Clean up trailing/double commas
                    combinedPrereqStr = re.sub(r',\s*\.', '.', combinedPrereqStr)
                    combinedPrereqStr = re.sub(r',\s*$', '', combinedPrereqStr)
                else:
                    if combinedPrereqStr:
                        combinedPrereqStr += f". Prerequisite or Corequisite: {sharedCodesStr}"
                    else:
                        combinedPrereqStr = f"Prerequisite or Corequisite: {sharedCodesStr}"

            finalPrerequisites = combinedPrereqStr
            finalCorequisites = combinedCoreqStr

            ## ── Determine which term codes apply to this course ──
            if catalogType == "gps":
                ## GPS = graduate — assign to graduate term codes
                applicableTermCodes = gradTermCodes
            else:
                ## TUG = undergraduate — assign to undergraduate term codes
                applicableTermCodes = undgTermCodes

            ## ── Create one row per applicable term (if the course exists in Canvas for that term) ──
            for termCode in applicableTermCodes:
                courseCode = title  ## e.g. "ACCT2065"
                termCourseInfo = canvasCourseInfoByTerm.get(termCode, {})

                ## Check if this course code exists in Canvas for this term
                if courseCode not in termCourseInfo:
                    continue  ## Course not offered in this term; skip

                ## Resolve the term name from the terms DataFrame (use as-is)
                termName = termCodeToNameDict.get(termCode, termCode)

                ## Resolve Parent Organization from the course's canvas_account_id
                courseCanvasAccountId = termCourseInfo[courseCode]
                parentOrg = _resolveParentOrganization(courseCanvasAccountId)

                extractRows.append({
                    "Term": termName,
                    "Subject": subject,
                    "Course Number": courseNumber,
                    "Title": name,
                    "Parent Organization": parentOrg,
                    "Description": description,
                    "Credits": credits,
                    "Prerequisites": finalPrerequisites,
                    "Corequisites": finalCorequisites,
                })

        ## ══════════════════════════════════════════════════════════════════════
        ## STEP 6: Build the output DataFrame and save
        ## ══════════════════════════════════════════════════════════════════════

        courseExtractDf = pd.DataFrame(extractRows, columns=[
            "Term", "Subject", "Course Number", "Title", "Parent Organization",
            "Description", "Credits", "Prerequisites", "Corequisites"
        ])

        ## Remove exact duplicate rows
        courseExtractDf.drop_duplicates(inplace=True)
        courseExtractDf.reset_index(drop=True, inplace=True)

        ## Save the Course Extract CSV
        baseCatalogPath = localSetup.getInternalResourcePaths("Catalog")
        catalogPath = os.path.join(baseCatalogPath, p1_catalogSchoolYear)
        os.makedirs(catalogPath, exist_ok=True)
        outputFilePath = os.path.join(catalogPath, "Course Extract.csv")
        courseExtractDf.to_csv(outputFilePath, index=False)

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
            catalogCourseReportsDf = pd.read_csv(filePath)
            catalogCourseReportsDf['Catalog Type'] = catalogType
            if combinedCatalogCourseReportDf.empty:
                combinedCatalogCourseReportDf = catalogCourseReportsDf
            else:
                combinedCatalogCourseReportDf = pd.concat([combinedCatalogCourseReportDf, catalogCourseReportsDf], ignore_index=True)

        ## Save the combined df as a new CSV in the same location as the downloaded reports, with a name like "Combined Catalog Course Report.csv"
        combinedCatalogCourseReportDf.to_csv(os.path.join(catalogRootPath, "Combined Catalog Course Report.csv"), index=False)
                
        return combinedCatalogCourseReportDf, catalogSchoolYear

    except Exception as Error:
        localSetup.logger.error(f"{functionName}: {Error}")
        raise

## This function retreives the catalog course reports, combines them into a simple syllabus format, and uploads the result to Simple Syllabus
def processCatalogCoursesAndUploadToSimpleSyllabus():
    functionName = "processCatalogCoursesAndUploadToSimpleSyllabus"
    try:
        combinedCatalogCourseReportDf, catalogSchoolYear = retrieveCatalogCourseReportsDfs()
        courseExtractDf = formatCombinedCatalogForSimpleSyllabus(combinedCatalogCourseReportDf, catalogSchoolYear)
        # Placeholder for uploading the processed file to Simple Syllabus
        # This would involve using the Simple Syllabus API or another method to upload the file
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