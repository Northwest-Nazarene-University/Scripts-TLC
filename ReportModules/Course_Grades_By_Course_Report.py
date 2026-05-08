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
from TLC_Common import makeApiCall, flattenApiObjectToJsonList, formatInstructorFirstNames, isPresent
from Canvas_Report import CanvasReport
from TLC_Action import determineCourseWeek
from Common_Configs import coreCanvasApiUrl
from Error_Email import errorEmail

## Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = os.path.basename(__file__).replace(".py", "")

scriptPurpose = r"""
Generate one CSV per Canvas SIS course containing grades for all active SIS-enrolled students
across all published assignments, saved to an account/instructor/SIS-metadata path structure.
"""
externalRequirements = r"""
To function properly this script requires Canvas API access and local SIS enrollment/course files.
"""

## Create the local setup variable
localSetup = LocalSetup(datetime.now(), __file__)

## Setup the error handler
errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)


def sanitizePathComponent(rawValue: Any, fallback: str = "Unknown") -> str:
    rawText = str(rawValue).strip() if rawValue is not None else ""
    if not rawText:
        rawText = fallback
    sanitized = re.sub(r'[<>:"/\\|?*]+', "_", rawText)
    sanitized = sanitized.replace("..", "_")
    sanitized = re.sub(r"\s+", " ", sanitized).strip().strip(".")
    return sanitized or fallback


def uniqueAssignmentColumnNames(assignments: list[dict]) -> list[str]:
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


def isWithinFinalsWindow(p1_startDate: str, p2_endDate: str, p1_referenceDate: datetime | None = None) -> bool:
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


def safeJoinUnderRoot(rootPath: str, *components: str) -> str:
    absoluteRoot = os.path.abspath(rootPath)
    candidatePath = os.path.abspath(os.path.join(absoluteRoot, *components))
    if not candidatePath.startswith(absoluteRoot + os.sep):
        raise ValueError(f"Unsafe output path resolved outside root: {candidatePath}")
    return candidatePath


def getCourseAssignments(courseId: str) -> list[dict]:
    assignmentsUrl = f"{coreCanvasApiUrl}courses/sis_course_id:{courseId}/assignments"
    assignmentsResponse, assignmentResponsePages = makeApiCall(localSetup, p1_apiUrl=assignmentsUrl)
    assignmentObjects = [assignmentsResponse] + assignmentResponsePages if isPresent(assignmentResponsePages) else [assignmentsResponse]
    assignmentList = flattenApiObjectToJsonList(localSetup, assignmentObjects, assignmentsUrl)
    return [assignment for assignment in assignmentList if assignment.get("published") is True]


def getAssignmentSubmissions(courseId: str, assignmentId: int) -> list[dict]:
    submissionsUrl = f"{coreCanvasApiUrl}courses/sis_course_id:{courseId}/assignments/{assignmentId}/submissions"
    submissionsPayload = {"include[]": ["user"]}
    submissionsResponse, submissionsPages = makeApiCall(
        localSetup,
        p1_apiUrl=submissionsUrl,
        p1_payload=submissionsPayload,
    )
    submissionObjects = [submissionsResponse] + submissionsPages if isPresent(submissionsPages) else [submissionsResponse]
    return flattenApiObjectToJsonList(localSetup, submissionObjects, submissionsUrl)


def buildCourseOutputPath(
    courseId: str,
    courseAccountId: int | None,
    instructorNames: list[str],
    sisCourseRow: pd.Series | None,
    accountsDf: pd.DataFrame,
) -> str:
    rootOutputPath = os.path.join(localSetup.getInternalResourcePaths("Canvas"), "Course_Grade_Exports")
    hierarchyComponents: list[str] = []

    if courseAccountId is not None:
        structureDict = CanvasReport.determineCollegeDepartmentDiscipline(localSetup, courseAccountId, accountsDf=accountsDf)
        hierarchyComponents = [sanitizePathComponent(component) for component in structureDict.get("Path_Components", []) if str(component).strip()]

    instructorFolder = sanitizePathComponent(
        formatInstructorFirstNames(instructorNames, defaultName="Instructor"),
        fallback="Instructor",
    )

    termId = ""
    courseCodeNorm = ""
    section = ""
    if sisCourseRow is not None:
        termId = str(
            sisCourseRow.get("term_id", "")
            or sisCourseRow.get("sis_term_id", "")
            or sisCourseRow.get("enrollment_term_id", "")
        ).strip()
        courseCodeNorm = str(
            sisCourseRow.get("course_code_norm", "")
        ).strip()

    splitCourseId = str(courseId).split("_")
    if len(splitCourseId) >= 3:
        if not termId:
            termId = splitCourseId[0]
        if not courseCodeNorm:
            courseCodeNorm = splitCourseId[1]
        section = splitCourseId[2]

    if not termId:
        termId = "Unknown_Term"
    if not courseCodeNorm:
        courseCodeNorm = "Unknown_Course"
    if not section:
        section = "Unknown_Section"

    sisMetadataFolder = sanitizePathComponent(f"{termId}_{courseCodeNorm}_{section}", fallback="Unknown_Metadata")

    return safeJoinUnderRoot(rootOutputPath, *hierarchyComponents, instructorFolder, sisMetadataFolder)


def CourseGradesByCourseReport() -> dict[str, str]:
    functionName = "Generate Course Grades By Course Report"
    try:
        localSetup.logger.info("Beginning course grade CSV pipeline.")

        sisPath = localSetup.getExternalResourcePath("SIS")
        sisEnrollmentsPath = os.path.join(sisPath, "canvas_enroll.csv")
        sisCoursesPath = os.path.join(sisPath, "canvas_course.csv")

        sisEnrollmentsDf = pd.read_csv(sisEnrollmentsPath, dtype=str).fillna("")
        sisCoursesDf = pd.read_csv(sisCoursesPath, dtype=str).fillna("")

        activeSisEnrollmentsDf = sisEnrollmentsDf[
            (sisEnrollmentsDf["role"].str.lower() == "student")
            & (sisEnrollmentsDf["status"].str.lower() == "active")
            & (sisEnrollmentsDf["course_id"].str.strip() != "")
            & (sisEnrollmentsDf["user_id"].str.strip() != "")
        ].copy()

        if activeSisEnrollmentsDf.empty:
            localSetup.logger.warning("No active SIS student enrollments found. No files created.")
            return {}

        currentTerms = sorted(localSetup.getCurrentTerms())
        if currentTerms:
            activeSisEnrollmentsDf = activeSisEnrollmentsDf[
                activeSisEnrollmentsDf["course_id"].str.startswith(tuple(currentTerms), na=False)
            ].copy()

        if activeSisEnrollmentsDf.empty:
            localSetup.logger.warning("No active SIS student enrollments found for current terms. No files created.")
            return {}

        canvasEnrollmentFrames: list[pd.DataFrame] = []

        for termCode in currentTerms:
            termEnrollmentsDf = CanvasReport.getEnrollmentsDf(localSetup, term=termCode, includeDeleted=True)
            if isPresent(termEnrollmentsDf):
                canvasEnrollmentFrames.append(termEnrollmentsDf)

        if not canvasEnrollmentFrames:
            localSetup.logger.warning("No Canvas enrollment datasets were loaded for detected terms.")
            return {}

        canvasEnrollmentsDf = pd.concat(canvasEnrollmentFrames, ignore_index=True)
        canvasEnrollmentsDf = canvasEnrollmentsDf.fillna("")
        canvasEnrollmentsDf["course_id"] = canvasEnrollmentsDf["course_id"].astype(str).str.strip()
        canvasEnrollmentsDf["user_id"] = canvasEnrollmentsDf["user_id"].astype(str).str.strip()
        canvasEnrollmentsDf["role"] = canvasEnrollmentsDf["role"].astype(str).str.lower()
        canvasEnrollmentsDf["status"] = canvasEnrollmentsDf["status"].astype(str).str.lower()

        matchingCanvasStudentEnrollmentsDf = canvasEnrollmentsDf[
            (canvasEnrollmentsDf["role"] == "student")
            & (canvasEnrollmentsDf["status"].isin(["active", "completed", "concluded"]))
            & (canvasEnrollmentsDf["course_id"].str.strip() != "")
            & (canvasEnrollmentsDf["user_id"].str.strip() != "")
        ][["course_id", "user_id", "canvas_user_id", "canvas_course_id", "status"]].drop_duplicates()

        mergedStudentEnrollmentsDf = activeSisEnrollmentsDf.merge(
            matchingCanvasStudentEnrollmentsDf,
            on=["course_id", "user_id"],
            how="inner",
            suffixes=("_sis", "_canvas"),
        )

        if mergedStudentEnrollmentsDf.empty:
            localSetup.logger.warning("No active SIS enrollments matched Canvas enrollments. No files created.")
            return {}

        coursesDf = CanvasReport.getCoursesDf(localSetup, term="All").fillna("")
        usersDf = CanvasReport.getUsersDf(localSetup).fillna("")
        accountsDf = CanvasReport.getAccountsDf(localSetup).fillna("")

        referenceDate = localSetup.initialDateTime
        if {"course_id", "start_date", "end_date"}.issubset(coursesDf.columns):
            finalsWindowCoursesDf = coursesDf[
                coursesDf["course_id"].astype(str).str.startswith(tuple(currentTerms), na=False)
            ].copy() if currentTerms else coursesDf.copy()
            finalsWindowCoursesDf = finalsWindowCoursesDf[
                finalsWindowCoursesDf.apply(
                    lambda row: isWithinFinalsWindow(
                        row.get("start_date", ""),
                        row.get("end_date", ""),
                        p1_referenceDate=referenceDate,
                    ),
                    axis=1,
                )
            ]
            finalsWindowCourseIds = set(finalsWindowCoursesDf["course_id"].astype(str).str.strip().tolist())
            mergedStudentEnrollmentsDf = mergedStudentEnrollmentsDf[
                mergedStudentEnrollmentsDf["course_id"].isin(finalsWindowCourseIds)
            ].copy()

        if mergedStudentEnrollmentsDf.empty:
            localSetup.logger.warning("No matched enrollments are within four weeks of finals week. No files created.")
            return {}

        usersByCanvasId = usersDf.set_index("canvas_user_id", drop=False) if "canvas_user_id" in usersDf.columns else pd.DataFrame()
        sisCourseIdColumn = ""
        if "course_id" in sisCoursesDf.columns:
            sisCourseIdColumn = "course_id"
        elif "sis_course_id" in sisCoursesDf.columns:
            sisCourseIdColumn = "sis_course_id"
        sisCoursesByCourseId = sisCoursesDf.set_index(sisCourseIdColumn, drop=False) if sisCourseIdColumn else pd.DataFrame()
        canvasCoursesBySisId = coursesDf.set_index("course_id", drop=False) if "course_id" in coursesDf.columns else pd.DataFrame()

        outputFilesByCourseId: dict[str, str] = {}
        uniqueCourseIds = sorted(mergedStudentEnrollmentsDf["course_id"].dropna().astype(str).str.strip().unique().tolist())

        for courseId in uniqueCourseIds:
            localSetup.logger.info(f"Processing course {courseId}")

            courseStudentEnrollmentsDf = mergedStudentEnrollmentsDf[
                mergedStudentEnrollmentsDf["course_id"] == courseId
            ].drop_duplicates(subset=["course_id", "user_id"])

            if courseStudentEnrollmentsDf.empty:
                continue

            assignments = getCourseAssignments(courseId)
            assignments = sorted(assignments, key=lambda assignment: str(assignment.get("position", assignment.get("id", ""))))
            assignmentColumnNames = uniqueAssignmentColumnNames(assignments)

            assignmentColumnById = {
                assignment.get("id"): assignmentColumn
                for assignment, assignmentColumn in zip(assignments, assignmentColumnNames)
            }

            studentRows = []
            for _, enrollmentRow in courseStudentEnrollmentsDf.iterrows():
                sisUserId = str(enrollmentRow["user_id"]).strip()
                canvasUserId = str(enrollmentRow.get("canvas_user_id", "")).strip()
                studentName = ""

                if isPresent(usersByCanvasId) and canvasUserId and canvasUserId in usersByCanvasId.index:
                    userRow = usersByCanvasId.loc[canvasUserId]
                    if isinstance(userRow, pd.DataFrame):
                        userRow = userRow.iloc[0]
                    studentName = str(userRow.get("name", "")).strip()

                rowData = {
                    "course_id": courseId,
                    "student_sis_id": sisUserId,
                    "student_canvas_id": canvasUserId,
                    "student_name": studentName,
                }

                for assignmentColumn in assignmentColumnNames:
                    rowData[assignmentColumn] = ""

                studentRows.append(rowData)

            outputDf = pd.DataFrame(studentRows)
            rowIndexByCanvasUserId = {
                str(row["student_canvas_id"]).strip(): idx
                for idx, row in outputDf.iterrows()
                if str(row["student_canvas_id"]).strip()
            }

            for assignment in assignments:
                assignmentId = assignment.get("id")
                assignmentColumn = assignmentColumnById.get(assignmentId)
                if not assignmentColumn:
                    continue

                assignmentSubmissions = getAssignmentSubmissions(courseId, assignmentId)
                for submission in assignmentSubmissions:
                    submissionCanvasUserId = str(submission.get("user_id", "")).strip()
                    if not submissionCanvasUserId or submissionCanvasUserId not in rowIndexByCanvasUserId:
                        continue

                    gradeValue = submission.get("score")
                    if gradeValue in [None, ""]:
                        gradeValue = submission.get("entered_score")
                    if gradeValue in [None, ""]:
                        gradeValue = submission.get("grade")

                    outputDf.at[rowIndexByCanvasUserId[submissionCanvasUserId], assignmentColumn] = gradeValue if gradeValue is not None else ""

            instructorNames = []
            courseInstructorEnrollmentsDf = canvasEnrollmentsDf[
                (canvasEnrollmentsDf["course_id"] == courseId)
                & (canvasEnrollmentsDf["role"] == "teacher")
                & (canvasEnrollmentsDf["status"] != "deleted")
            ]
            if isPresent(courseInstructorEnrollmentsDf):
                for _, instructorEnrollment in courseInstructorEnrollmentsDf.iterrows():
                    instructorCanvasId = str(instructorEnrollment.get("canvas_user_id", "")).strip()
                    if isPresent(usersByCanvasId) and instructorCanvasId and instructorCanvasId in usersByCanvasId.index:
                        userRow = usersByCanvasId.loc[instructorCanvasId]
                        if isinstance(userRow, pd.DataFrame):
                            userRow = userRow.iloc[0]
                        instructorName = str(userRow.get("name", "")).strip()
                        if instructorName:
                            instructorNames.append(instructorName)

            sisCourseRow = None
            if isPresent(sisCoursesByCourseId) and courseId in sisCoursesByCourseId.index:
                sisCourseRow = sisCoursesByCourseId.loc[courseId]
                if isinstance(sisCourseRow, pd.DataFrame):
                    sisCourseRow = sisCourseRow.iloc[0]

            courseAccountId: int | None = None
            if isPresent(canvasCoursesBySisId) and courseId in canvasCoursesBySisId.index:
                canvasCourseRow = canvasCoursesBySisId.loc[courseId]
                if isinstance(canvasCourseRow, pd.DataFrame):
                    canvasCourseRow = canvasCourseRow.iloc[0]
                rawAccountId = canvasCourseRow.get("canvas_account_id", "")
                if rawAccountId in ["", None]:
                    rawAccountId = canvasCourseRow.get("account_id", "")
                try:
                    courseAccountId = int(float(rawAccountId))
                except (TypeError, ValueError):
                    courseAccountId = None

            outputFolder = buildCourseOutputPath(
                courseId=courseId,
                courseAccountId=courseAccountId,
                instructorNames=instructorNames,
                sisCourseRow=sisCourseRow,
                accountsDf=accountsDf,
            )
            os.makedirs(outputFolder, exist_ok=True)

            outputFilePath = os.path.join(outputFolder, f"{sanitizePathComponent(courseId)}.csv")
            outputDf.to_csv(outputFilePath, index=False, encoding="utf-8")
            outputFilesByCourseId[courseId] = outputFilePath

        localSetup.logger.info(f"Completed course grade CSV pipeline with {len(outputFilesByCourseId)} files.")
        return outputFilesByCourseId

    except Exception as Error:
        errorHandler.sendError(functionName, Error)
        return {}

if __name__ == "__main__":
    CourseGradesByCourseReport()
