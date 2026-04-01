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
    from TLC_Action import readCsvWithEncoding, uploadToSimpleSyllabus
except ImportError:
    # Fallback to relative imports if package layout differs
    from ResourceModules.Local_Setup import LocalSetup
    from ResourceModules.Error_Email import errorEmail
    from ResourceModules.TLC_Action import readCsvWithEncoding, uploadToSimpleSyllabus

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
This script reads a Course Editor input CSV containing SIS IDs and course codes,
cross-references them against the Course Extract CSV already being sent to Simple Syllabus,
and builds a Course Editor file that assigns those SIS IDs as Course Editors for every
matching term/subject/course_number combination. The resulting file is then uploaded to
Simple Syllabus via the same SFTP mechanism used for the catalog Course Extract.
"""
externalRequirements = r"""
This script requires the following external resources:
1. A Course Editor input CSV file with columns:
   role, term, subject, course_number, instructor_identifier (SIS ID or Email)
2. A previously generated Course Extract CSV from the Send_Catalog_To_Simple_Syllabus process.
3. Access to the Simple Syllabus SFTP server via SSH private key authentication.
4. The SSH private key file and its password stored in the config path.
"""

## Initialize LocalSetup and helpers from ResourceModules
localSetup = LocalSetup(datetime.now(), __file__)
errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)


## ══════════════════════════════════════════════════════════════════════════════
## Course Editor File Builder
## ══════════════════════════════════════════════════════════════════════════════

def buildCourseEditorFile(p1_courseEditorInputPath: str, p1_courseExtractPath: str, p1_outputPath: str) -> str:
    """
    Reads the Course Editor input CSV and the Course Extract CSV, cross-references
    them to find all term/subject/course_number matches, and produces a Course Editor
    file in the format expected by Simple Syllabus.

    The Course Editor input CSV is expected to have columns:
        role, term, subject, course_number, instructor_identifier (SIS ID or Email)

    If the term column is blank or general (e.g. "Spring 2026"), the function expands
    to ALL matching terms found in the Course Extract for that subject + course_number.

    Args:
        p1_courseEditorInputPath (str): Path to the Course Editor input CSV.
        p1_courseExtractPath (str): Path to the Course Extract CSV.
        p1_outputPath (str): Path where the output Course Editor CSV will be saved.

    Returns:
        str: The file path of the generated Course Editor CSV.
    """

    functionName = "buildCourseEditorFile"

    try:
        localSetup.logger.info(f"{functionName}: Starting Course Editor file build")
        localSetup.logger.info(f"{functionName}: Input file: {p1_courseEditorInputPath}")
        localSetup.logger.info(f"{functionName}: Course Extract file: {p1_courseExtractPath}")

        ## ── Validate inputs exist ──
        if not os.path.exists(p1_courseEditorInputPath):
            raise FileNotFoundError(f"{functionName}: Course Editor input file not found: {p1_courseEditorInputPath}")
        if not os.path.exists(p1_courseExtractPath):
            raise FileNotFoundError(f"{functionName}: Course Extract file not found: {p1_courseExtractPath}")

        ## ── Read the Course Editor input CSV ──
        editorInputDf = readCsvWithEncoding(p1_courseEditorInputPath)
        editorInputDf.columns = [col.strip() for col in editorInputDf.columns]

        ## Validate required columns exist (case-insensitive fallback)
        requiredInputColumns = ["role", "subject", "course_number", "instructor_identifier (SIS ID or Email)"]
        missingColumns = [col for col in requiredInputColumns if col not in editorInputDf.columns]
        if missingColumns:
            colMap = {col.lower(): col for col in editorInputDf.columns}
            for reqCol in missingColumns:
                if reqCol.lower() in colMap:
                    editorInputDf.rename(columns={colMap[reqCol.lower()]: reqCol}, inplace=True)
            missingColumns = [col for col in requiredInputColumns if col not in editorInputDf.columns]
            if missingColumns:
                raise ValueError(
                    f"{functionName}: Course Editor input CSV is missing required columns: {missingColumns}. "
                    f"Expected: role, term, subject, course_number, instructor_identifier (SIS ID or Email)"
                )

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

        for _, inputRow in editorInputDf.iterrows():
            role = str(inputRow.get("role", "Course Editor")).strip()
            inputTerm = str(inputRow.get("term", "")).strip() if pd.notna(inputRow.get("term", "")) else ""
            inputSubject = str(inputRow.get("subject", "")).strip().upper()
            inputCourseNumber = str(inputRow.get("course_number", "")).strip()
            instructorId = str(inputRow.get("instructor_identifier (SIS ID or Email)", "")).strip()

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

            if matchingCourses.empty:
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
                    "instructor_identifier (SIS ID or Email)": instructorId,
                })

        ## ── Build the output DataFrame ──
        if not editorOutputRows:
            localSetup.logger.warning(f"{functionName}: No matching Course Editor rows were generated. Output file will be empty.")

        editorOutputDf = pd.DataFrame(editorOutputRows, columns=[
            "role", "term", "subject", "course_number", "instructor_identifier (SIS ID or Email)"
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


## ══════════════════════════════════════════════════════════════════════════════
## Main Process Function
## ══════════════════════════════════════════════════════════════════════════════

def processCourseEditorsAndUploadToSimpleSyllabus():
    """
    Main function that:
        1. Determines the current catalog school year and locates the Course Extract.
        2. Looks for a Course Editor input CSV in the catalog school year directory or config path.
        3. Cross-references the input against the Course Extract to build the output.
        4. Uploads the resulting Course Editor file to Simple Syllabus via SFTP.
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

        ## ── Locate the Course Editor input CSV ──
        ## Look in the catalog school year directory first, then the config path as fallback
        courseEditorInputPath = None
        for searchDir in [catalogPath, localSetup.configPath]:
            if not os.path.isdir(searchDir):
                continue
            for candidateName in os.listdir(searchDir):
                candidateLower = candidateName.lower()
                if candidateLower.startswith("nnu_course_editor_file") and candidateLower.endswith(".csv"):
                    courseEditorInputPath = os.path.join(searchDir, candidateName)
                    break
                if candidateLower == "course editor input.csv":
                    courseEditorInputPath = os.path.join(searchDir, candidateName)
                    break
            if courseEditorInputPath:
                break

        if courseEditorInputPath is None:
            raise FileNotFoundError(
                f"{functionName}: No Course Editor input CSV found. "
                f"Please place a file named 'NNU_Course_Editor_File*.csv' or 'Course Editor Input.csv' "
                f"in {catalogPath} or {localSetup.configPath}."
            )

        localSetup.logger.info(f"{functionName}: Found Course Editor input file: {courseEditorInputPath}")

        ## ── Build the Course Editor output file ──
        courseEditorOutputPath = os.path.join(catalogPath, "Course Editor.csv")

        buildCourseEditorFile(
            p1_courseEditorInputPath=courseEditorInputPath,
            p1_courseExtractPath=courseExtractPath,
            p1_outputPath=courseEditorOutputPath,
        )

        ## ── Verify the output file has content before uploading ──
        outputDf = readCsvWithEncoding(courseEditorOutputPath)
        if outputDf.empty:
            localSetup.logger.warning(
                f"{functionName}: Course Editor output file is empty. No matching courses were found. Skipping upload."
            )
            return

        ## ── Upload the Course Editor file to Simple Syllabus via SFTP ──
        ## No success tag needed for Course Editor files — that's catalog-specific
        uploadToSimpleSyllabus(courseEditorOutputPath, localSetup, p1_errorHandler=errorHandler, p1_writeSuccessTag=False)

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