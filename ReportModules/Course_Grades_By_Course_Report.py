## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Copilot

from datetime import datetime
import os
import re
import sys
from typing import Any

import pandas as pd

## Add ResourceModules to the system path
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

## New resource modules
from Local_Setup import LocalSetup
from TLC_Common import makeApiCall, flattenApiObjectToJsonList, isPresent
from Canvas_Report import CanvasReport
from TLC_Action import determineCourseWeek, runCourseGradeExportsThreaded
from Common_Configs import coreCanvasApiUrl
from Error_Email import errorEmail

## Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = os.path.basename(__file__).replace(".py", "")

scriptPurpose = r"""
Generate one Excel file per Canvas SIS course containing grades for all active SIS-enrolled students
across all published assignments, saved to an account/instructor/SIS-metadata path structure.
"""
externalRequirements = r"""
To function properly this script requires Canvas API access and local SIS enrollment/course files.
"""

## Create the local setup variable
localSetup = LocalSetup(datetime.now(), __file__)

## Setup the error handler
errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)


## Sanitize a value for safe use as a file-system path component
def sanitizePathComponent(rawValue: Any, fallback: str = "Unknown") -> str:
    """
    Sanitize a raw value so it can be used as a folder or file name component.

    Args:
        rawValue (Any): Source value to sanitize.
        fallback (str): Default value used when the source is blank.

    Returns:
        str: Sanitized path-safe text.
    """
    rawText = str(rawValue).strip() if rawValue is not None else ""
    if not rawText:
        rawText = fallback
    sanitized = re.sub(r'[<>:"/\\|?*]+', "_", rawText)
    sanitized = sanitized.replace("..", "_")
    sanitized = re.sub(r"\s+", " ", sanitized).strip().strip(".")
    return sanitized or fallback


## Build unique assignment column names so duplicate assignment titles do not collide
def uniqueAssignmentColumnNames(assignments: list[dict]) -> list[str]:
    """
    Create unique, sanitized CSV column names for assignment titles.

    Args:
        assignments (list[dict]): Assignment objects returned by Canvas.

    Returns:
        list[str]: Unique assignment column names in input order.
    """
    seen: dict[str, int] = {}
    columnNames: list[str] = []

    for assignment in assignments:
        assignmentName = sanitizePathComponent(assignment.get("name", ""), fallback="Unnamed Assignment")
        baseName = assignmentName

        if baseName in seen:
            seen[baseName] += 1
            assignmentName = f"{baseName}_{seen[baseName]}"
        else:
            seen[baseName] = 1

        columnNames.append(assignmentName)

    return columnNames


## Format instructor folder text using complete instructor names
def formatInstructorFullNames(instructorNames: list[str], defaultName: str = "Unknown Instructor Name") -> str:
    """
    Format instructor names using full names while preserving input order and uniqueness.

    Args:
        instructorNames (list[str]): Instructor full names.
        defaultName (str): Fallback text when no valid names are available.

    Returns:
        str: Human-readable full-name list for folder naming.
    """
    fullNames = [str(name).strip() for name in instructorNames if str(name).strip() and str(name).strip().lower() != "nan"]
    uniqueFullNames = list(dict.fromkeys(fullNames))

    if not uniqueFullNames:
        return defaultName
    if len(uniqueFullNames) == 1:
        return uniqueFullNames[0]
    return ", ".join(uniqueFullNames[:-1]) + f", and {uniqueFullNames[-1]}"


## Determine if a course is currently within four weeks of finals week
def isWithinFinalsWindow(p1_startDate: str, p2_endDate: str, p1_referenceDate: datetime | None = None) -> bool:
    """
    Determine if a course falls within the final-four-weeks reporting window.

    Args:
        p1_startDate (str): Course start date in mm/dd/yyyy format.
        p2_endDate (str): Course end date in mm/dd/yyyy format.
        p1_referenceDate (datetime | None): Optional reference date used for week calculation.

    Returns:
        bool: True when course is within 0-4 weeks of finals, else False.
    """
    if not str(p1_startDate).strip() or not str(p2_endDate).strip():
        return False
    try:
        courseWeek, courseFinalWeek = determineCourseWeek(
            p1_startDate,
            p2_endDate,
            p1_referenceDate=p1_referenceDate,
        )
    except Exception:
        return False
    weeksUntilFinals = courseFinalWeek - courseWeek
    return 0 <= weeksUntilFinals <= 4


## Join path components under a trusted root and reject path traversal
def safeJoinUnderRoot(rootPath: str, *components: str) -> str:
    """
    Safely join path components under a fixed root directory.

    Args:
        rootPath (str): Root directory that output paths must stay under.
        *components (str): Child path components to join.

    Returns:
        str: Absolute safe path under rootPath.

    Raises:
        ValueError: If the resolved path escapes rootPath.
    """
    absoluteRoot = os.path.abspath(rootPath)
    candidatePath = os.path.abspath(os.path.join(absoluteRoot, *components))
    if not candidatePath.startswith(absoluteRoot + os.sep):
        raise ValueError(f"Unsafe output path resolved outside root: {candidatePath}")
    return candidatePath


## Retrieve all published assignments for a Canvas SIS course
def getCourseAssignments(courseId: str) -> list[dict]:
    """
    Retrieve published assignments for a Canvas course identified by SIS course ID.

    Args:
        courseId (str): Canvas SIS course ID.

    Returns:
        list[dict]: Published assignment objects.
    """
    assignmentsUrl = f"{coreCanvasApiUrl}courses/sis_course_id:{courseId}/assignments"
    apiCallResult = makeApiCall(localSetup, p1_apiUrl=assignmentsUrl)
    if not apiCallResult:
        return []
    assignmentsResponse, assignmentResponsePages = apiCallResult
    assignmentObjects = [assignmentsResponse] + assignmentResponsePages if isPresent(assignmentResponsePages) else [assignmentsResponse]
    assignmentList = flattenApiObjectToJsonList(localSetup, assignmentObjects, assignmentsUrl)
    return [assignment for assignment in assignmentList if assignment.get("published") is True]


## Retrieve all submissions for one assignment in a Canvas SIS course
def getAssignmentSubmissions(courseId: str, assignmentId: Any) -> list[dict]:
    """
    Retrieve submissions for a single assignment in a Canvas SIS course.

    Args:
        courseId (str): Canvas SIS course ID.
        assignmentId (int): Canvas assignment ID.

    Returns:
        list[dict]: Submission objects for the assignment.
    """
    submissionsUrl = f"{coreCanvasApiUrl}courses/sis_course_id:{courseId}/assignments/{assignmentId}/submissions"
    submissionsPayload = {"include[]": ["user"]}
    apiCallResult = makeApiCall(
        localSetup,
        p1_apiUrl=submissionsUrl,
        p1_payload=submissionsPayload,
    )
    if not apiCallResult:
        return []
    submissionsResponse, submissionsPages = apiCallResult
    submissionObjects = [submissionsResponse] + submissionsPages if isPresent(submissionsPages) else [submissionsResponse]
    return flattenApiObjectToJsonList(localSetup, submissionObjects, submissionsUrl)


## Build the final output folder path for a course export CSV
def buildCourseOutputPath(
    courseId: str,
    courseAccountId: int | None,
    instructorNames: list[str],
    sisCourseRow: pd.Series | None,
    accountsDf: pd.DataFrame,
) -> str:
    """
    Build a hierarchical output path for a course grade export file.

    Args:
        courseId (str): Canvas SIS course ID.
        courseAccountId (int | None): Canvas account ID for hierarchy lookup.
        instructorNames (list[str]): Instructor names associated with the course.
        sisCourseRow (pd.Series | None): SIS course row for term/course metadata.
        accountsDf (pd.DataFrame): Canvas accounts dataframe used for hierarchy resolution.

    Returns:
        str: Final output folder path under the Canvas internal resources root.
    """
    rootOutputPath = os.path.join(localSetup.getInternalResourcePaths("Canvas"), "Course_Grade_Exports")
    hierarchyComponents: list[str] = []

    if courseAccountId is not None:
        structureDict = CanvasReport.determineCollegeDepartmentDiscipline(localSetup, courseAccountId, accountsDf=accountsDf)
        hierarchyComponents = [sanitizePathComponent(component) for component in structureDict.get("Path_Components", []) if str(component).strip()]

    instructorFolder = sanitizePathComponent(
        formatInstructorFullNames(instructorNames, defaultName="Unknown Instructor Name"),
        fallback="Unknown Instructor Name",
    )

    return safeJoinUnderRoot(rootOutputPath, *hierarchyComponents, instructorFolder)


## Generate one grades CSV per course for active SIS student enrollments
def CourseGradesByCourseReport() -> dict[str, str]:
    """
    Build per-course grade export Excel files for active SIS student enrollments.
    """
    functionName = "Generate Course Grades By Course Report"
    try:
        ## Step 1: Load SIS enrollment and course input files
        localSetup.logger.info("Beginning course grade CSV pipeline.")

        sisPath = localSetup.getExternalResourcePath("SIS")
        sisEnrollmentsPath = os.path.join(sisPath, "canvas_enroll.csv")
        sisCoursesPath = os.path.join(sisPath, "canvas_course.csv")

        sisEnrollmentsDf = pd.read_csv(sisEnrollmentsPath, dtype=str).fillna("")
        sisCoursesDf = pd.read_csv(sisCoursesPath, dtype=str).fillna("")

        ## Step 2: Keep only active SIS student enrollments with valid course/user IDs
        activeSisEnrollmentsDf = sisEnrollmentsDf[
            (sisEnrollmentsDf["role"].str.lower() == "student")
            & (sisEnrollmentsDf["status"].str.lower() == "active")
            & (sisEnrollmentsDf["course_id"].str.strip() != "")
            & (sisEnrollmentsDf["user_id"].str.strip() != "")
        ].copy()

        if activeSisEnrollmentsDf.empty:
            localSetup.logger.warning("No active SIS student enrollments found. No files created.")
            return {}

        ## Step 3: Restrict SIS enrollments to detected current terms
        currentTerms = sorted(localSetup.getCurrentTerms())
        if currentTerms:
            activeSisEnrollmentsDf = activeSisEnrollmentsDf[
                activeSisEnrollmentsDf["course_id"].str.startswith(tuple(currentTerms), na=False)
            ].copy()

        if activeSisEnrollmentsDf.empty:
            localSetup.logger.warning("No active SIS student enrollments found for current terms. No files created.")
            return {}

        ## Step 4: Load Canvas enrollment data for each current term
        canvasEnrollmentFrames: list[pd.DataFrame] = []

        for termCode in currentTerms:
            termEnrollmentsDf = CanvasReport.getEnrollmentsDf(localSetup, term=termCode, includeDeleted=False)
            if isPresent(termEnrollmentsDf):
                canvasEnrollmentFrames.append(termEnrollmentsDf)

        if not canvasEnrollmentFrames:
            localSetup.logger.warning("No Canvas enrollment datasets were loaded for detected terms.")
            return {}

        ## Step 5: Normalize Canvas enrollments and keep student enrollment matches only
        canvasEnrollmentsDf = pd.concat(canvasEnrollmentFrames, ignore_index=True)
        canvasEnrollmentsDf = canvasEnrollmentsDf.fillna("")
        canvasEnrollmentsDf["course_id"] = canvasEnrollmentsDf["course_id"].astype(str).str.strip()
        canvasEnrollmentsDf["user_id"] = canvasEnrollmentsDf["user_id"].astype(str).str.strip()
        if "canvas_user_id" in canvasEnrollmentsDf.columns:
            canvasEnrollmentsDf["canvas_user_id"] = canvasEnrollmentsDf["canvas_user_id"].astype(str).str.strip()
        canvasEnrollmentsDf["role"] = canvasEnrollmentsDf["role"].astype(str).str.lower()
        canvasEnrollmentsDf["status"] = canvasEnrollmentsDf["status"].astype(str).str.lower()

        baseStudentCols = ["course_id", "user_id", "canvas_user_id", "canvas_course_id", "status"]
        finalGradeCols = [
            "current_score",
            "final_score",
            "current_grade",
            "final_grade",
        ]
        keepCols = [col for col in (baseStudentCols + finalGradeCols) if col in canvasEnrollmentsDf.columns]

        matchingCanvasStudentEnrollmentsDf = canvasEnrollmentsDf[
            (canvasEnrollmentsDf["role"] == "student")
            & (canvasEnrollmentsDf["status"].isin(["active", "completed", "concluded"]))
            & (canvasEnrollmentsDf["course_id"].str.strip() != "")
            & (canvasEnrollmentsDf["user_id"].str.strip() != "")
        ][keepCols].drop_duplicates()

        mergedStudentEnrollmentsDf = activeSisEnrollmentsDf.merge(
            matchingCanvasStudentEnrollmentsDf,
            on=["course_id", "user_id"],
            how="inner",
            suffixes=("_sis", "_canvas"),
        )

        if mergedStudentEnrollmentsDf.empty:
            localSetup.logger.warning("No active SIS enrollments matched Canvas enrollments. No files created.")
            return {}

        ## Step 6: Load supporting datasets (courses, users, accounts)
        coursesDf = pd.DataFrame()
        for termCode in currentTerms:
            termCoursesDf = CanvasReport.getCoursesDf(localSetup, term=termCode).fillna("")
            coursesDf = pd.concat([coursesDf, termCoursesDf], ignore_index=True)
        usersDf = CanvasReport.getUsersDf(localSetup).fillna("")
        if "canvas_user_id" in usersDf.columns:
            usersDf["canvas_user_id"] = usersDf["canvas_user_id"].astype(str).str.strip()
        accountsDf = CanvasReport.getAccountsDf(localSetup).fillna("")

        ## Step 7: Finals-week filtering disabled to include all current-term courses
        # referenceDate = localSetup.initialDateTime
        # if {"course_id", "start_date", "end_date"}.issubset(coursesDf.columns):
        #     finalsWindowCoursesDf = coursesDf[
        #         coursesDf["course_id"].astype(str).str.startswith(tuple(currentTerms), na=False)
        #     ].copy() if currentTerms else coursesDf.copy()
        #     finalsWindowCoursesDf = finalsWindowCoursesDf[
        #         finalsWindowCoursesDf.apply(
        #             lambda row: isWithinFinalsWindow(
        #                 row.get("start_date", ""),
        #                 row.get("end_date", ""),
        #                 p1_referenceDate=referenceDate,
        #             ),
        #             axis=1,
        #         )
        #     ]
        #     finalsWindowCourseIds = set(finalsWindowCoursesDf["course_id"].astype(str).str.strip().tolist())
        #     mergedStudentEnrollmentsDf = mergedStudentEnrollmentsDf[
        #         mergedStudentEnrollmentsDf["course_id"].isin(finalsWindowCourseIds)
        #     ].copy()

        if mergedStudentEnrollmentsDf.empty:
            localSetup.logger.warning("No matched SIS/Canvas enrollments found for current terms. No files created.")
            return {}

        ## Step 7: Build lookup tables
        usersByCanvasId = usersDf.set_index("canvas_user_id", drop=False) if "canvas_user_id" in usersDf.columns else pd.DataFrame()
        sisCourseIdColumn = ""
        if "course_id" in sisCoursesDf.columns:
            sisCourseIdColumn = "course_id"
        elif "sis_course_id" in sisCoursesDf.columns:
            sisCourseIdColumn = "sis_course_id"
        sisCoursesByCourseId = sisCoursesDf.set_index(sisCourseIdColumn, drop=False) if sisCourseIdColumn else pd.DataFrame()
        canvasCoursesBySisId = coursesDf.set_index("course_id", drop=False) if "course_id" in coursesDf.columns else pd.DataFrame()

        ## Step 8: Build and write one output Excel file per course using threaded helper actions
        outputFilesByCourseId = runCourseGradeExportsThreaded(
            p1_localSetup=localSetup,
            p1_errorHandler=errorHandler,
            p2_mergedStudentEnrollmentsDf=mergedStudentEnrollmentsDf,
            p2_canvasEnrollmentsDf=canvasEnrollmentsDf,
            p2_usersByCanvasId=usersByCanvasId,
            p2_sisCoursesByCourseId=sisCoursesByCourseId,
            p2_canvasCoursesBySisId=canvasCoursesBySisId,
            p2_accountsDf=accountsDf,
            p2_maxWorkers=100,
        )

        # Non-threaded testing call (single course) - keep commented out unless actively testing
        # from TLC_Action import _processSingleCourseGradeExport
        # testCourseId = "GS2026_BSNS6902_2N"
        # resultCourseId, outputFilePath = _processSingleCourseGradeExport(
        #     p1_localSetup=localSetup,
        #     p2_errorHandler=errorHandler,
        #     p3_courseId=testCourseId,
        #     p4_mergedStudentEnrollmentsDf=mergedStudentEnrollmentsDf,
        #     p5_canvasEnrollmentsDf=canvasEnrollmentsDf,
        #     p6_usersByCanvasId=usersByCanvasId,
        #     p7_sisCoursesByCourseId=sisCoursesByCourseId,
        #     p8_canvasCoursesBySisId=canvasCoursesBySisId,
        #     p9_accountsDf=accountsDf,
        # )
        # outputFilesByCourseId = {resultCourseId: outputFilePath} if outputFilePath else {}

        ## Step 9: Return file map for downstream usage/logging
        localSetup.logger.info(f"Completed course grade CSV pipeline with {len(outputFilesByCourseId)} files.")
        return outputFilesByCourseId

    except Exception as Error:
        errorHandler.sendError(functionName, Error)
        return {}


if __name__ == "__main__":
    CourseGradesByCourseReport()