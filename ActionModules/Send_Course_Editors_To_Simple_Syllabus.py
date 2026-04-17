# Author: Bryce Miller - brycezmiller@nnu.edu
# Last Updated by: Bryce Miller

## Import Generic Modules

import os, sys, pandas as pd
from datetime import datetime

# Ensure ResourceModules are importable when running from ActionModules
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

try: ## Irregular try clause, do not comment out in testing
    from Local_Setup import LocalSetup
    from Error_Email import errorEmail
    from TLC_Action import readCsvWithEncoding, uploadToSimpleSyllabus, removeStaleSuccessTag
    from TLC_Common import isPresent, isMissing
except ImportError:
    # Fallback to relative imports if package layout differs
    from ResourceModules.Local_Setup import LocalSetup
    from ResourceModules.Error_Email import errorEmail
    from ResourceModules.TLC_Action import readCsvWithEncoding, uploadToSimpleSyllabus, removeStaleSuccessTag
    from ResourceModules.TLC_Common import isPresent, isMissing

## Import the catalog helper that determines the school year path
try:
    from Send_Catalog_To_Simple_Syllabus import buildCatalogSchoolYearRelatedLocalPath
except ImportError:
    from ActionModules.Send_Catalog_To_Simple_Syllabus import buildCatalogSchoolYearRelatedLocalPath

## Get catalogToSimpleSyllabusConfig from configs
from Common_Configs import catalogToSimpleSyllabusConfig

## Set working directory
os.chdir(os.path.dirname(__file__))

# Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = os.path.basename(__file__)

scriptPurpose = r"""
This script reads Course Editor input CSVs from both the config path (term-agnostic base editors)
and the catalog school year path (year/term-specific editors), cross-references them against the
Course Extract CSV already being sent to Simple Syllabus, and builds a Course Editor file that
assigns those users as Course Editors for every matching term/subject/course_number combination.
The resulting file is then uploaded to Simple Syllabus via the same SFTP mechanism used for the
catalog Course Extract.
"""
externalRequirements = r"""
This script requires the following external resources:
1. One or more Course Editor input CSV files placed in the config path and/or the catalog
   school year directory. Accepted formats:
   a) Minimal: user_sis_id, course_code  (role defaults to "Course Editor", course_code auto-split)
   b) Full:    role, term, subject, course_number, instructor_identifier (SIS ID or Email)
   Any combination of these columns is accepted; missing columns will be auto-derived.
2. A previously generated Course Extract CSV from the Send_Catalog_To_Simple_Syllabus process.
3. Access to the Simple Syllabus SFTP server via SSH private key authentication.
4. The SSH private key file and its password stored in the config path.
"""

## Initialize LocalSetup and helpers from ResourceModules
localSetup = LocalSetup(datetime.now(), __file__)
errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)


## ══════════════════════════════════════════════════════════════════════════════
## Course Editor Input Discovery and Normalization
## ══════════════════════════════════════════════════════════════════════════════

def _findCourseEditorFiles(p1_searchDir: str) -> list:
    """
    Scan a directory for Course Editor input CSV files.
    Matches files whose name starts with 'nnu_course_editor_file' or equals
    'course editor input.csv' (case-insensitive).

    Args:
        p1_searchDir (str): Directory to search.

    Returns:
        list: List of full file paths found.
    """
    found = []
    if not os.path.isdir(p1_searchDir):
        return found
    for candidateName in os.listdir(p1_searchDir):
        candidateLower = candidateName.lower()
        if (candidateLower.startswith("nnu_course_editor_file") and candidateLower.endswith(".csv")) \
                or candidateLower == "course editor input.csv":
            found.append(os.path.join(p1_searchDir, candidateName))
    return found


def _normalizeCourseEditorDf(p1_rawDf: pd.DataFrame, p1_sourcePath: str) -> pd.DataFrame:
    """
    Normalize a Course Editor input DataFrame so it always has the five Simple Syllabus
    output columns regardless of the input format.

    Handles two formats:
        Minimal: user_sis_id, course_code  (optionally with role, term)
        Full:    role, term, subject, course_number, instructor_identifier (SIS ID or Email)

    Normalization rules:
        - 'course_code' (e.g. "ACT1121") is split into subject (first 4 chars) and
          course_number (remaining chars), unless subject + course_number already exist.
        - 'user_sis_id' is mapped to 'instructor_identifier (SIS ID or Email)' unless
          that column already exists.
        - 'role' defaults to "Course Editor" if absent or blank.
        - 'term' defaults to "" (blank) if absent, meaning "expand to all matching terms".

    Args:
        p1_rawDf (pd.DataFrame): The raw DataFrame read from a Course Editor CSV.
        p1_sourcePath (str): Path to the source file (for logging).

    Returns:
        pd.DataFrame: Normalized DataFrame with columns:
            role, term, subject, course_number, instructor_identifier (SIS ID or Email)
    """

    functionName = "_normalizeCourseEditorDf"

    df = p1_rawDf.copy()
    df.columns = [col.strip() for col in df.columns]

    ## Build a case-insensitive column lookup
    colMap = {col.lower(): col for col in df.columns}

    ## ── Resolve instructor identifier ──
    ## Priority: "instructor_identifier (SIS ID or Email)" > "instructor_identifier (sis id or email)" > "user_sis_id"
    targetIdCol = "instructor_identifier"
    if targetIdCol not in df.columns:
        if targetIdCol.lower() in colMap:
            df.rename(columns={colMap[targetIdCol.lower()]: targetIdCol}, inplace=True)
        elif "user_sis_id" in colMap:
            df.rename(columns={colMap["user_sis_id"]: targetIdCol}, inplace=True)
        else:
            localSetup.logger.error(
                f"{functionName}: File {p1_sourcePath} has no 'user_sis_id' or "
                f"'instructor_identifier' column. Skipping."
            )
            return pd.DataFrame()

    ## ── Resolve subject and course_number ──
    hasSubject = "subject" in colMap
    hasCourseNumber = "course_number" in colMap
    hasCourseCode = "course_code" in colMap

    if not hasSubject or not hasCourseNumber:
        if hasCourseCode:
            ## Split course_code into subject (first 4 chars) and course_number (remainder)
            courseCodeCol = colMap["course_code"]
            df["subject"] = df[courseCodeCol].astype(str).str.strip().str[:4].str.upper()
            df["course_number"] = df[courseCodeCol].astype(str).str.strip().str[4:]
            localSetup.logger.info(
                f"{functionName}: Auto-split 'course_code' into 'subject' + 'course_number' for {p1_sourcePath}"
            )
        else:
            localSetup.logger.error(
                f"{functionName}: File {p1_sourcePath} has neither 'subject'+'course_number' nor "
                f"'course_code'. Cannot determine courses. Skipping."
            )
            return pd.DataFrame()
    else:
        ## Rename to canonical casing if needed
        if colMap.get("subject") != "subject":
            df.rename(columns={colMap["subject"]: "subject"}, inplace=True)
        if colMap.get("course_number") != "course_number":
            df.rename(columns={colMap["course_number"]: "course_number"}, inplace=True)

    ## ── Resolve role ──
    if "role" not in [c.lower() for c in df.columns]:
        df["role"] = "Course Editor"
    else:
        roleCol = colMap.get("role", "role")
        if roleCol != "role":
            df.rename(columns={roleCol: "role"}, inplace=True)
        df["role"] = df["role"].fillna("Course Editor").astype(str).str.strip()
        df.loc[df["role"] == "", "role"] = "Course Editor"

    ## ── Resolve term ──
    if "term" not in [c.lower() for c in df.columns]:
        df["term"] = ""
    else:
        termCol = colMap.get("term", "term")
        if termCol != "term":
            df.rename(columns={termCol: "term"}, inplace=True)
        df["term"] = df["term"].fillna("").astype(str).str.strip()

    ## ── Select and return only the canonical columns ──
    outputDf = df[["role", "term", "subject", "course_number", targetIdCol]].copy()
    outputDf.columns = ["role", "term", "subject", "course_number", "instructor_identifier"]

    localSetup.logger.info(
        f"{functionName}: Normalized {len(outputDf)} rows from {p1_sourcePath}"
    )

    return outputDf


## ══════════════════════════════════════════════════════════════════════════════
## Course Editor File Builder
## ══════════════════════════════════════════════════════════════════════════════

def buildCourseEditorFile(p1_combinedEditorInputDf: pd.DataFrame, p1_courseExtractPath: str, p1_outputPath: str) -> str:
    """
    Takes a combined and normalized Course Editor input DataFrame and the Course Extract CSV,
    cross-references them to find all term/subject/course_number matches, and produces a
    Course Editor file in the format expected by Simple Syllabus.

    If the term column is blank for a row, the function expands to ALL matching terms
    found in the Course Extract for that subject + course_number.

    If the term is populated (e.g. "Spring 2026"), the function filters to terms whose
    name contains all the input words (so "Spring 2026" matches "UNDERGRADUATE SPRING SEMESTER 2026").

    Args:
        p1_combinedEditorInputDf (pd.DataFrame): Combined and normalized editor input DataFrame
            with columns: role, term, subject, course_number, instructor_identifier (SIS ID or Email)
        p1_courseExtractPath (str): Path to the Course Extract CSV.
        p1_outputPath (str): Path where the output Course Editor CSV will be saved.

    Returns:
        str: The file path of the generated Course Editor CSV.
    """

    functionName = "buildCourseEditorFile"

    try:
        localSetup.logger.info(f"{functionName}: Starting Course Editor file build")
        localSetup.logger.info(f"{functionName}: Course Extract file: {p1_courseExtractPath}")
        localSetup.logger.info(f"{functionName}: Combined input rows: {len(p1_combinedEditorInputDf)}")

        ## ── Validate Course Extract exists ──
        if not os.path.exists(p1_courseExtractPath):
            raise FileNotFoundError(f"{functionName}: Course Extract file not found: {p1_courseExtractPath}")

        ## ── Read the Course Extract CSV ──
        courseExtractDf = readCsvWithEncoding(p1_courseExtractPath)

        requiredExtractColumns = ["Term", "Subject", "Course Number"]
        missingExtractCols = [col for col in requiredExtractColumns if col not in courseExtractDf.columns]
        if missingExtractCols:
            raise ValueError(
                f"{functionName}: Course Extract CSV is missing required columns: {missingExtractCols}. "
                f"Expected at least: Term, Subject, Course Number"
            )

        ## ── Normalize Course Extract for matching ──
        courseExtractDf["_subject_norm"] = courseExtractDf["Subject"].astype(str).str.strip().str.upper()
        courseExtractDf["_course_number_norm"] = courseExtractDf["Course Number"].astype(str).str.strip()

        ## ── Build the Course Editor output rows ──
        editorOutputRows = []

        for _, inputRow in p1_combinedEditorInputDf.iterrows():
            role = str(inputRow.get("role", "Course Editor")).strip()
            inputTerm = str(inputRow.get("term", "")).strip() if pd.notna(inputRow.get("term", "")) else ""
            inputSubject = str(inputRow.get("subject", "")).strip().upper()
            inputCourseNumber = str(inputRow.get("course_number", "")).strip()
            instructorId = str(inputRow.get("instructor_identifier", "")).strip()

            if not inputSubject or not inputCourseNumber or not instructorId:
                localSetup.logger.warning(
                    f"{functionName}: Skipping row with missing subject/course_number/instructor_identifier: "
                    f"subject='{inputSubject}', course_number='{inputCourseNumber}', id='{instructorId}'"
                )
                continue

            ## ── Find matching courses in the Course Extract ──
            matchMask = (
                (courseExtractDf["_subject_norm"] == inputSubject) &
                (courseExtractDf["_course_number_norm"] == inputCourseNumber)
            )

            ## If the input has a specific term, also filter by term
            ## "Spring 2026" matches "UNDERGRADUATE SPRING SEMESTER 2026" by checking all input words appear
            if inputTerm:
                inputTermWords = inputTerm.upper().split()
                termMatchMask = courseExtractDf["Term"].astype(str).str.upper().apply(
                    lambda extractTerm: all(word in extractTerm for word in inputTermWords)
                )
                matchMask = matchMask & termMatchMask

            matchingCourses = courseExtractDf[matchMask]

            if isMissing(matchingCourses):
                localSetup.logger.warning(
                    f"{functionName}: No matching courses found in Course Extract for "
                    f"subject='{inputSubject}', course_number='{inputCourseNumber}', term='{inputTerm}'. Skipping."
                )
                continue

            ## ── Expand: one output row per matching term ──
            for _, extractRow in matchingCourses.iterrows():
                editorOutputRows.append({
                    "role": role,
                    "term": extractRow["Term"],
                    "subject": extractRow["Subject"],
                    "course_number": extractRow["Course Number"],
                    "instructor_identifier": instructorId,
                })

        ## ── Build the output DataFrame ──
        if not editorOutputRows:
            localSetup.logger.warning(f"{functionName}: No matching Course Editor rows were generated. Output file will be empty.")

        editorOutputDf = pd.DataFrame(editorOutputRows, columns=[
            "role", "term", "subject", "course_number", "instructor_identifier"
        ])

        editorOutputDf.drop_duplicates(inplace=True)
        editorOutputDf.reset_index(drop=True, inplace=True)

        os.makedirs(os.path.dirname(p1_outputPath), exist_ok=True)
        editorOutputDf.to_csv(p1_outputPath, index=False, encoding='utf-8')
        localSetup.logger.info(
            f"{functionName}: Successfully built Course Editor file with {len(editorOutputDf)} rows. "
            f"Saved to {p1_outputPath}"
        )

        return p1_outputPath

    except Exception as Error:
        localSetup.logger.error(f"{functionName}: {Error}")
        raise


## ══════════════════════════════���═══════════════════════════════════════════════
## Main Process Function
## ══════════════════════════════════════════════════════════════════════════════

def processCourseEditorsAndUploadToSimpleSyllabus():
    """
    Main function that:
        1. Determines the current catalog school year and locates the Course Extract.
        2. Searches BOTH the config path and the catalog school year directory for
           Course Editor input CSVs and combines them.
           - Config path files are treated as term-agnostic base editors (expand to all terms).
           - Catalog year path files may have specific term values.
        3. Normalizes all input files to a common format (handles minimal user_sis_id/course_code
           format as well as the full Simple Syllabus format).
        4. Cross-references the combined input against the Course Extract to build the output.
        5. Skips the upload if the newly built output is identical to the last successfully
           uploaded version (detected via a success tag file).
        6. Uploads the resulting Course Editor file to Simple Syllabus via SFTP.
    """

    functionName = "processCourseEditorsAndUploadToSimpleSyllabus"

    try:
        localSetup.logger.info(f"{functionName}: Starting Course Editor processing")

        ## ── Determine the catalog school year and build paths ──
        catalogLinks = catalogToSimpleSyllabusConfig.get("catalogProduction", {})
        if not catalogLinks or not isinstance(catalogLinks, dict):
            raise ValueError(f"{functionName}: catalogToSimpleSyllabusConfig['catalogProduction'] missing or invalid")

        catalogRootPath, catalogSchoolYear = buildCatalogSchoolYearRelatedLocalPath(catalogLinks)

        ## ── Locate the Course Extract CSV ──
        baseCatalogPath = localSetup.getInternalResourcePaths("Catalog")
        catalogPath = os.path.join(baseCatalogPath, catalogSchoolYear)
        courseExtractPath = os.path.join(catalogPath, "Course Extract.csv")

        if not os.path.exists(courseExtractPath):
            raise FileNotFoundError(
                f"{functionName}: Course Extract CSV not found at {courseExtractPath}. "
                f"Please run processCatalogCoursesAndUploadToSimpleSyllabus first to generate the Course Extract."
            )

        ## ── Discover Course Editor input files from BOTH locations ──
        ## Config path: term-agnostic base editors (no term column or blank term → expand to all terms)
        ## Catalog year path: year/term-specific editors (may have populated term column)
        configEditorFiles = _findCourseEditorFiles(localSetup.configPath)
        catalogYearEditorFiles = _findCourseEditorFiles(catalogPath)

        allEditorFiles = configEditorFiles + catalogYearEditorFiles

        if not allEditorFiles:
            raise FileNotFoundError(
                f"{functionName}: No Course Editor input CSV files found. "
                f"Please place file(s) named 'NNU_Course_Editor_File*.csv' or 'Course Editor Input.csv' "
                f"in {localSetup.configPath} (term-agnostic) and/or {catalogPath} (year/term-specific)."
            )

        localSetup.logger.info(
            f"{functionName}: Found {len(configEditorFiles)} config-path editor file(s) and "
            f"{len(catalogYearEditorFiles)} catalog-year editor file(s)"
        )
        for f in allEditorFiles:
            localSetup.logger.info(f"{functionName}:   -> {f}")

        ## ── Read, normalize, and combine all input files ──
        normalizedDfs = []
        for editorFilePath in allEditorFiles:
            try:
                rawDf = readCsvWithEncoding(editorFilePath)
                if isMissing(rawDf):
                    localSetup.logger.warning(f"{functionName}: File is empty, skipping: {editorFilePath}")
                    continue
                normalizedDf = _normalizeCourseEditorDf(rawDf, editorFilePath)
                if isPresent(normalizedDf):
                    normalizedDfs.append(normalizedDf)
            except Exception as fileError:
                localSetup.logger.warning(
                    f"{functionName}: Could not process editor file {editorFilePath}: {fileError}. Skipping."
                )

        if not normalizedDfs:
            raise ValueError(
                f"{functionName}: None of the discovered Course Editor input files could be processed successfully."
            )

        combinedEditorInputDf = pd.concat(normalizedDfs, ignore_index=True)
        localSetup.logger.info(
            f"{functionName}: Combined {len(combinedEditorInputDf)} total editor input rows "
            f"from {len(normalizedDfs)} file(s)"
        )

        ## ── Build the Course Editor output file ──
        courseEditorOutputPath = os.path.join(catalogPath, "Course Editor.csv")
        successTagPath = os.path.join(catalogPath, "Course Editor_UPLOAD_SUCCESS.txt")

        ## ── Read the previous output BEFORE overwriting, if a success tag exists ──
        previousOutputDf = None
        if os.path.exists(courseEditorOutputPath) and os.path.exists(successTagPath):
            try:
                previousOutputDf = readCsvWithEncoding(courseEditorOutputPath)
            except Exception as readError:
                localSetup.logger.warning(
                    f"{functionName}: Could not read previous Course Editor output ({readError}). Proceeding."
                )

        buildCourseEditorFile(
            p1_combinedEditorInputDf=combinedEditorInputDf,
            p1_courseExtractPath=courseExtractPath,
            p1_outputPath=courseEditorOutputPath,
        )

        ## ── Verify the output file has content before uploading ──
        outputDf = readCsvWithEncoding(courseEditorOutputPath)
        if isMissing(outputDf):
            localSetup.logger.warning(
                f"{functionName}: Course Editor output file is empty. No matching courses were found. Skipping upload."
            )
            return

        ## ── Check whether the new output differs from the last successfully uploaded version ──
        if previousOutputDf is not None:
            try:
                newSorted = outputDf.sort_values(
                    by=list(outputDf.columns)
                ).reset_index(drop=True)
                prevSorted = previousOutputDf.sort_values(
                    by=list(previousOutputDf.columns)
                ).reset_index(drop=True)

                if newSorted.equals(prevSorted):
                    localSetup.logger.info(
                        f"{functionName}: No changes detected in Course Editor output since last successful upload. Skipping."
                    )
                    return
                else:
                    localSetup.logger.info(
                        f"{functionName}: Changes detected in Course Editor output since last successful upload. Proceeding."
                    )
            except Exception as compareError:
                localSetup.logger.warning(
                    f"{functionName}: Could not compare with previous Course Editor output ({compareError}). Proceeding with upload."
                )
        else:
            localSetup.logger.info(
                f"{functionName}: No previous successfully uploaded Course Editor output found. Proceeding."
            )

        ## ── Remove stale success tag before uploading ──
        removeStaleSuccessTag(successTagPath, localSetup)

        ## ── Upload the Course Editor file to Simple Syllabus via SFTP ──
        ## p1_writeSuccessTag=True — uploadToSimpleSyllabus now derives the tag name from the
        ## filename, so "Course Editor.csv" automatically produces "Course Editor_UPLOAD_SUCCESS.txt"
        uploadToSimpleSyllabus(courseEditorOutputPath, localSetup, p1_errorHandler=errorHandler, p1_writeSuccessTag=True)

        localSetup.logger.info(
            f"{functionName}: Successfully processed and uploaded Course Editor file to Simple Syllabus"
        )

    except Exception as Error:
        localSetup.logger.error(f"{functionName}: {Error}")
        raise


## ══════════════════════════════════════════════════════════════════════════════
## Script Entry Point
## ══════════════════════════════════════════════════════════════════════════════

## If the script is being run directly, execute the main function
if __name__ == "__main__":
    functionName = "main"
    try:
        processCourseEditorsAndUploadToSimpleSyllabus()
    except Exception as Error:
        localSetup.logger.error(f"{functionName}: {Error}")
        errorHandler.sendError(functionName, Error)