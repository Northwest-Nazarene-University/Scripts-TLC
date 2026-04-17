## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

## Import necessary modules
import os, sys, re, pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from dateutil import parser as dateutil_parser

## Import necessary functions from local modules
## Add Script repository to syspath
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

## New resource modules
try:
    from Local_Setup import LocalSetup
    from TLC_Common import makeApiCall, flattenApiObjectToJsonList, isPresent
    from Canvas_Report import CanvasReport
    from Error_Email import errorEmail
    from Core_Microsoft_Api import sendOutlookEmail
except ImportError:
    from ResourceModules.Local_Setup import LocalSetup
    from ResourceModules.TLC_Common import makeApiCall, flattenApiObjectToJsonList, isPresent
    from ResourceModules.Canvas_Report import CanvasReport
    from ResourceModules.Error_Email import errorEmail
    from ResourceModules.Core_Microsoft_Api import sendOutlookEmail

## Create the localsetup variable
localSetup = LocalSetup(datetime.now(), __file__)  ## sets cwd, paths, logs, date parts

## Import configs
from Common_Configs import coreCanvasApiUrl, scriptLibrary, serviceEmailAccount

## Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = os.path.basename(__file__).replace(".py", "")

scriptPurpose = r"""
This script identifies and removes Canvas courses and enrollments that were created by the SIS
(created_by_sis == True) but are no longer present in the current SIS feed. It only operates on
records that Canvas currently considers "active". Courses with existing gradebook scores are
flagged for manual review via email rather than being deleted.
"""
externalRequirements = r"""
To function properly, this script requires:
1. Access to NNU's SIS feed files (canvas_course.csv, canvas_enroll.csv, and canvas_term.csv) via the SIS external resource path.
2. A valid Canvas API access token.
3. Access to the Canvas provisioning reports for courses, enrollments, and terms.
4. The ability to send notification emails via the Microsoft Outlook API.
"""

## Setup the error handler
errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)

## ─────────────────────────────────────────────────────────────────────────────
## Helper: Parse a date string safely, returning None on failure
## ─────────────────────────────────────────────────────────────────────────────
def _safeParseDatetime(dtStr):
    """Return a datetime from *dtStr*, or None if blank / unparseable."""
    functionName = "_safeParseDatetime"
    try:
        if pd.isna(dtStr) or str(dtStr).strip() == "":
            return None
        dt = dateutil_parser.parse(str(dtStr))
        ## Strip timezone info so all comparisons use naive datetimes (matching datetime.now())
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc)
        return dt.replace(tzinfo=None)
    except Exception:
        return None


## ─────────────────────────────────────────────────────────────────────────────
## Helper: Build a term‑date lookup dict from the Canvas terms report
## ─────────────────────────────────────────────────────────────────────────────
def buildTermDateLookup():
    """
    Return a dict  {term_id: {"start_date": datetime|None, "end_date": datetime|None}}
    built from the Canvas terms provisioning report.
    """
    functionName = "buildTermDateLookup"
    try:
        termsDf = CanvasReport.getTermsDf(localSetup)
        termDateDict = {}

        if termsDf is not None and not termsDf.empty:
            for _, row in termsDf.iterrows():
                termId = row.get("term_id") or row.get("canvas_term_id")
                if pd.isna(termId):
                    continue
                termId = str(termId)
                termDateDict[termId] = {
                    "start_date": _safeParseDatetime(row.get("start_date")),
                    "end_date":   _safeParseDatetime(row.get("end_date")),
                }

        localSetup.logger.info(f"Built term date lookup with {len(termDateDict)} terms")
        return termDateDict

    except Exception as Error:
        errorHandler.sendError(functionName, Error)
        return {}


## ─────────────────────────────────────────────────────────────────────────────
## Helper: Build a list of effective date intervals from SIS courses
## ─────────────────────────────────────────────────────────────────────────────
def buildSisDateIntervals(sisCoursesDf, canvasTermDateDict):
    """
    Return a set of (start_dt, end_dt) tuples, one per SIS course whose
    effective date range can be resolved. Falls back to Canvas term dates
    (which include all terms, active and legacy) when a course row has no
    course-level dates. Courses whose dates cannot be resolved are skipped.
    """
    functionName = "buildSisDateIntervals"
    try:
        intervals = set()
        for _, row in sisCoursesDf.iterrows():
            startDt = _safeParseDatetime(row.get("start_date"))
            endDt   = _safeParseDatetime(row.get("end_date"))
            if startDt is None or endDt is None:
                termId    = str(row.get("term_id", "")).strip()
                termDates = canvasTermDateDict.get(termId, {})
                if startDt is None:
                    startDt = termDates.get("start_date")
                if endDt is None:
                    endDt = termDates.get("end_date")
            if startDt is not None and endDt is not None:
                intervals.add((startDt, endDt))
        localSetup.logger.info(
            f"Built {len(intervals)} unique SIS date pair(s) from {len(sisCoursesDf)} SIS course rows"
        )
        return intervals
    except Exception as Error:
        errorHandler.sendError(functionName, Error)
        return set()


## ─────────────────────────────────────────────────────────────────────────────
## Helper: Resolve a course's effective start / end dates
## ─────────────────────────────────────────────────────────────────────────────
def resolveCourseDates(courseRow, termDateDict):
    """
    Return (startDatetime, endDatetime) for a course row.
    Prefers course-level dates; falls back to Canvas term dates (which include
    all terms, past and present, from CanvasReport.getTermsDf).
    """
    functionName = "resolveCourseDates"
    try:
        startDt = _safeParseDatetime(courseRow.get("start_date"))
        endDt   = _safeParseDatetime(courseRow.get("end_date"))

        ## Fall back to Canvas term dates when course‑level dates are missing
        if startDt is None or endDt is None:
            termId = str(courseRow.get("term_id", ""))
            termDates = termDateDict.get(termId, {})
            if startDt is None:
                startDt = termDates.get("start_date")
            if endDt is None:
                endDt = termDates.get("end_date")

        return startDt, endDt

    except Exception as Error:
        errorHandler.sendError(functionName, Error)
        return None, None


## ─────────────────────────────────────────────────────────────────────────────
## Helper: Check whether a course has any graded submissions
## ─────────────────────────────────────────────────────────────────────────────
def courseHasGradedSubmissions(canvasCourseId):
    """
    Return (hasGrades: bool, scoredCount: int) by querying the Submissions API.
    """
    functionName = "courseHasGradedSubmissions"
    try:
        submissionsUrl = f"{coreCanvasApiUrl}courses/{canvasCourseId}/students/submissions"
        payload = {"student_ids[]": "all", "per_page": 100}

        apiResult = makeApiCall(
            localSetup,
            p1_apiUrl=submissionsUrl,
            p1_payload=payload,
        )
        response, responseList = apiResult if apiResult is not None else (None, None)

        ## Flatten all pages into a single list
        allSubmissions = flattenApiObjectToJsonList(
            localSetup,
            responseList if responseList else ([response] if response else []),
            submissionsUrl,
        )

        ## Count submissions with a non‑null score
        scoredCount = sum(1 for sub in allSubmissions if sub.get("score") is not None)
        return scoredCount > 0, scoredCount

    except Exception as Error:
        errorHandler.sendError(functionName, Error)
        ## Err on the side of caution — treat as "has grades" so we don't delete
        return True, -1


## ─────────────────────────────────────────────────────────────────────────────
## Process a single orphaned course (runs inside its own thread)
## ─────────────────────────────────────────────────────────────────────────────
def processOrphanedCourse(courseRow, activeEnrollmentsDf, summary):
    """
    For one orphaned course:
      1. Check for gradebook scores.
      2. If grades exist -> email + skip.
      3. If no grades and has enrollments -> delete enrollments, delete course, email.
      4. If no grades and no enrollments -> delete course silently.
    *summary* is a dict with thread-safe lists for aggregating counts.
    """
    functionName = "processOrphanedCourse"
    try:
        canvasCourseId  = courseRow["canvas_course_id"]
        courseId        = courseRow.get("course_id", "")
        shortName       = courseRow.get("short_name", "")
        longName        = courseRow.get("long_name", "")

        ## ── Step A: Grade check ──────────────────────────────────────────────
        hasGrades, scoredCount = courseHasGradedSubmissions(canvasCourseId)

        ## ── Step B: Course HAS grades → notify, skip ─────────────────────────
        if hasGrades:
            localSetup.logger.warning(
                f"Orphaned course {courseId} (canvas_course_id={canvasCourseId}) has "
                f"{scoredCount} scored submission(s) -- skipping deletion, sending email"
            )

            emailSubject = f"Orphaned SIS Course Has Grades -- Manual Review Required: {courseId}"
            emailBody = (
                f"<!DOCTYPE html><html><body>"
                f"<p>The following SIS-created Canvas course is no longer in the SIS feed but contains "
                f"graded student submissions and was <b>not</b> deleted.</p>"
                f"<ul>"
                f"<li><b>Course SIS ID:</b> {courseId}</li>"
                f"<li><b>Short Name:</b> {shortName}</li>"
                f"<li><b>Long Name:</b> {longName}</li>"
                f"<li><b>Scored Submissions Found:</b> {scoredCount}</li>"
                f"</ul>"
                f"<p>Please review this course manually in Canvas.</p>"
                f"</body></html>"
            )

            sendOutlookEmail(
                p1_microsoftUserName=serviceEmailAccount,
                p1_subject=emailSubject,
                p1_body=emailBody,
                p1_recipientEmailList=f"{scriptLibrary}@nnu.edu",
                p1_shared_mailbox=f"{scriptLibrary}@nnu.edu",
            )

            summary["skipped_grades"].append(courseId)
            return

        ## ── Step C: Course has NO grades ─────────────────────────────────────
        ## Find active enrollments that belong to this course
        courseEnrollmentsDf = activeEnrollmentsDf[
            activeEnrollmentsDf["canvas_course_id"] == canvasCourseId
        ]
        enrollmentCount = len(courseEnrollmentsDf)

        if enrollmentCount > 0:
            ## Delete each enrollment first
            for _, enrollRow in courseEnrollmentsDf.iterrows():
                enrollmentId = enrollRow["canvas_enrollment_id"]
                deleteEnrollmentUrl = f"{coreCanvasApiUrl}courses/{canvasCourseId}/enrollments/{enrollmentId}"
                apiResult = makeApiCall(
                    localSetup,
                    p1_apiUrl=deleteEnrollmentUrl,
                    p1_payload={"task": "delete"},
                    p1_apiCallType="delete",
                )
                response, _ = apiResult if apiResult is not None else (None, None)
                if response and response.status_code == 200:
                    localSetup.logger.info(
                        f"Deleted enrollment {enrollmentId} (user={enrollRow.get('user_id', '')}) "
                        f"from orphaned course {courseId}"
                    )
                else:
                    statusCode = response.status_code if response else "N/A"
                    localSetup.logger.warning(
                        f"Failed to delete enrollment {enrollmentId} from orphaned course {courseId}. "
                        f"Status: {statusCode}"
                    )

            ## Delete the course
            deleteCourseUrl = f"{coreCanvasApiUrl}courses/{canvasCourseId}"
            apiResult = makeApiCall(
                localSetup,
                p1_apiUrl=deleteCourseUrl,
                p1_payload={"event": "delete"},
                p1_apiCallType="delete",
            )
            response, _ = apiResult if apiResult is not None else (None, None)

            if response and response.status_code == 200:
                localSetup.logger.info(
                    f"Deleted orphaned course {courseId} (canvas_course_id={canvasCourseId}) "
                    f"after removing {enrollmentCount} enrollment(s)"
                )
            else:
                statusCode = response.status_code if response else "N/A"
                localSetup.logger.warning(
                    f"Failed to delete orphaned course {courseId}. Status: {statusCode}"
                )

            ## Always send notification for courses that had enrollments
            emailSubject = f"Orphaned SIS Course With Enrollments Deleted: {courseId}"
            emailBody = (
                f"<!DOCTYPE html><html><body>"
                f"<p>The following SIS-created Canvas course was no longer in the SIS feed, "
                f"contained no graded submissions, and has been <b>deleted</b>.</p>"
                f"<ul>"
                f"<li><b>Course SIS ID:</b> {courseId}</li>"
                f"<li><b>Short Name:</b> {shortName}</li>"
                f"<li><b>Long Name:</b> {longName}</li>"
                f"<li><b>Enrollments Deleted:</b> {enrollmentCount}</li>"
                f"</ul>"
                f"<p>The course and all of its enrollments have been removed from Canvas.</p>"
                f"</body></html>"
            )

            sendOutlookEmail(
                p1_microsoftUserName=serviceEmailAccount,
                p1_subject=emailSubject,
                p1_body=emailBody,
                p1_recipientEmailList=f"{scriptLibrary}@nnu.edu",
                p1_shared_mailbox=f"{scriptLibrary}@nnu.edu",
            )

            summary["deleted_with_enrollments"].append(courseId)

        else:
            ## No enrollments, no grades — delete silently
            deleteCourseUrl = f"{coreCanvasApiUrl}courses/{canvasCourseId}"
            apiResult = makeApiCall(
                localSetup,
                p1_apiUrl=deleteCourseUrl,
                p1_payload={"event": "delete"},
                p1_apiCallType="delete",
            )
            response, _ = apiResult if apiResult is not None else (None, None)

            if response and response.status_code == 200:
                localSetup.logger.info(
                    f"Silently deleted orphaned course {courseId} (canvas_course_id={canvasCourseId}) -- "
                    f"no enrollments, no grades"
                )
            else:
                statusCode = response.status_code if response else "N/A"
                localSetup.logger.warning(
                    f"Failed to silently delete orphaned course {courseId}. Status: {statusCode}"
                )

            summary["deleted_silently"].append(courseId)

    except Exception as Error:
        errorHandler.sendError(functionName, Error)


## ─────────────────────────────────────────────────────────────────────────────
## Process a single orphaned enrollment in a still‑active course (thread target)
## ─────────────────────────────────────────────────────────────────────────────
def processOrphanedEnrollment(enrollRow, summary):
    """Delete one orphaned enrollment unconditionally. No grade check, no email."""
    functionName = "processOrphanedEnrollment"
    try:
        canvasCourseId = enrollRow["canvas_course_id"]
        enrollmentId   = enrollRow["canvas_enrollment_id"]
        userId         = enrollRow.get("user_id", "")
        courseId       = enrollRow.get("course_id", "")

        deleteUrl = f"{coreCanvasApiUrl}courses/{canvasCourseId}/enrollments/{enrollmentId}"
        apiResult = makeApiCall(
            localSetup,
            p1_apiUrl=deleteUrl,
            p1_payload={"task": "delete"},
            p1_apiCallType="delete",
        )
        response, _ = apiResult if apiResult is not None else (None, None)

        if response and response.status_code == 200:
            localSetup.logger.info(
                f"Deleted orphaned enrollment {enrollmentId} "
                f"(user={userId}, course={courseId})"
            )
            summary["orphaned_enrollments_deleted"].append(enrollmentId)
        else:
            statusCode = response.status_code if response else "N/A"
            localSetup.logger.warning(
                f"Failed to delete orphaned enrollment {enrollmentId} "
                f"(user={userId}, course={courseId}). Status: {statusCode}"
            )

    except Exception as Error:
        errorHandler.sendError(functionName, Error)


## ─────────────────────────────────────────────────────────────────────────────
## Main orchestration function
## ─────────────────────────────────────────────────────────────────────────────
def removeOrphanedSisItems():

    functionName = "removeOrphanedSisItems"

    try:

        ## ══════════════════════════════════════════════════════════════════════
        ## 1. Read the SIS feed files
        ## ══════════════════════════════════════════════════════════════════════
        sisResourcePath = localSetup.getExternalResourcePath("SIS")

        sisCoursesDf = pd.read_csv(
            os.path.join(sisResourcePath, "canvas_course.csv"),
            dtype=str,
        )
        sisEnrollDf = pd.read_csv(
            os.path.join(sisResourcePath, "canvas_enroll.csv"),
            dtype=str,
        )

        ## ══════════════════════════════════════════════════════════════════════
        ## 2. Build Canvas term date lookup (all terms, including legacy)
        ## ══════════════════════════════════════════════════════════════════════
        termDateDict     = buildTermDateLookup()
        sisDateIntervals = buildSisDateIntervals(sisCoursesDf, termDateDict)

        ## Build the set of all known SIS course IDs (any status)
        ## If the SIS has a course at all, it will manage its own deletion; this script
        ## only removes courses that are completely absent from the SIS feed.
        activeSisCourseIds = set(sisCoursesDf["course_id"].dropna())
        localSetup.logger.info(f"SIS feed: {len(activeSisCourseIds)} known course IDs (all statuses)")

        ## Build the set of all known SIS enrollment keys (any status)
        ## If the SIS has an enrollment at all, it will manage its own deletion; this script
        ## only removes enrollments that are completely absent from the SIS feed.
        activeEnrollKeys = set(
            zip(
                sisEnrollDf["course_id"].str.lower().str.strip(),
                sisEnrollDf["user_id"].str.strip(),
                sisEnrollDf["role"].str.lower().str.strip(),
            )
        )
        localSetup.logger.info(f"SIS feed: {len(activeEnrollKeys)} known enrollment keys (all statuses)")

        ## Derive a broad span from the SIS date pairs purely to limit which Canvas
        ## term provisioning reports we need to pull.  The actual per-course date
        ## gate in Step 5 uses exact (start, end) pair matching, not this window.
        if sisDateIntervals:
            windowStart = min(s for s, e in sisDateIntervals)
            windowEnd   = max(e for s, e in sisDateIntervals)
        else:
            localSetup.logger.warning(
                "No resolvable SIS date intervals found -- no Canvas terms will be queried"
            )
            return

        localSetup.logger.info(
            f"SIS date span for term pre-filter: {windowStart.date()} → {windowEnd.date()} "
            f"({len(sisDateIntervals)} unique SIS date pair(s))"
        )

        ## Determine which Canvas terms overlap with our SIS date window
        ## Only accept NNU term codes: 2-letter prefix + 2-digit year
        _VALID_TERM_RE = re.compile(r"^(FA|SP|SU|GF|GS|SG)\d{2}$")

        relevantTermIds = []
        skippedTermIds  = []
        for termId, dates in termDateDict.items():
            tStart = dates.get("start_date")
            tEnd   = dates.get("end_date")
            ## If either date is unknown, exclude the term to be safe
            if tStart is None or tEnd is None:
                continue
            ## Skip non-standard term IDs (e.g. MASTER, AF25, AS26, SA26)
            if not _VALID_TERM_RE.match(termId):
                skippedTermIds.append(termId)
                continue
            ## Check overlap: term ends after windowStart AND term starts before windowEnd
            if tEnd >= windowStart and tStart <= windowEnd:
                relevantTermIds.append(termId)

        if skippedTermIds:
            localSetup.logger.info(
                f"Skipped {len(skippedTermIds)} non-standard Canvas term(s): {', '.join(skippedTermIds)}"
            )

        localSetup.logger.info(
            f"Identified {len(relevantTermIds)} Canvas term(s) overlapping the SIS date window"
        )

        ## ══════════════════════════════════════════════════════════════════════
        ## 3. Retrieve Canvas courses, enrollments, and sections across relevant terms
        ## ══════════════════════════════════════════════════════════════════════
        allCoursesDfs     = []
        allEnrollmentsDfs = []
        allSectionsDfs    = []

        for termId in relevantTermIds:
            termCoursesDf = CanvasReport.getCoursesDf(localSetup, termId)
            if termCoursesDf is not None and not termCoursesDf.empty:
                allCoursesDfs.append(termCoursesDf)

            termEnrollDf = CanvasReport.getEnrollmentsDf(localSetup, termId, includeDeleted=False)
            if termEnrollDf is not None and not termEnrollDf.empty:
                allEnrollmentsDfs.append(termEnrollDf)

            termSectionsDf = CanvasReport.getSectionsDf(localSetup, termId)
            if termSectionsDf is not None and not termSectionsDf.empty:
                allSectionsDfs.append(termSectionsDf)

        canvasCoursesDf = pd.concat(allCoursesDfs, ignore_index=True) if allCoursesDfs else pd.DataFrame()
        canvasEnrollDf  = pd.concat(allEnrollmentsDfs, ignore_index=True) if allEnrollmentsDfs else pd.DataFrame()
        canvasSectionsDf = pd.concat(allSectionsDfs, ignore_index=True) if allSectionsDfs else pd.DataFrame()

        if canvasCoursesDf.empty:
            localSetup.logger.info("No Canvas courses found across relevant terms -- nothing to do")
            return
        if canvasEnrollDf.empty:
            localSetup.logger.info("No Canvas enrollments found across relevant terms")

        ## ══════════════════════════════════════════════════════════════════════
        ## Step 3.5: Build crosslist mapping from sections
        ## ══════════════════════════════════════════════════════════════════════
        ## Maps original_course_id → set of parent_course_ids
        ## so we can recognize crosslisted enrollments

        crosslistOrigToParent = {}   # SIS course_id → parent SIS course_id
        crosslistedAwayCourseIds = set()  # course SIS IDs whose sections were crosslisted away

        if not canvasSectionsDf.empty:
            ## Filter to rows where the course_id is not in the name and the course_id is present
            ## In a normal section the section name contains the originating course SIS ID.
            ## When a section has been cross-listed into a parent course, its canvas_course_id
            ## changes to the parent course but the name still reflects the original course,
            ## so the current course_id field no longer appears in the name.
            crosslistedSections = canvasSectionsDf[
                canvasSectionsDf.apply(
                    lambda row: (
                        isPresent(row.get("course_id"))
                        and str(row.get("course_id", "")).strip() != ""
                        and str(row.get("course_id", "")).strip()
                            not in str(row.get("name", ""))
                    ),
                    axis=1,
                )
            ].copy()
            

            for _, secRow in crosslistedSections.iterrows():
                ## The section now lives in the course identified by canvas_course_id
                parentCanvasCourseId = str(secRow.get("canvas_course_id", "")).strip()

                ## Find the course row for the parent course to get its SIS ID
                parentCourseRow = canvasCoursesDf[
                    canvasCoursesDf["canvas_course_id"].astype(str) == parentCanvasCourseId
                ]
                parentSisId = parentCourseRow["course_id"].values[0] if not parentCourseRow.empty else None
                
                ## Resolve SIS course_ids from the canvas_course_ids
                ## Look up the original course SIS ID from the section name or from courses df
                sectionName = str(secRow.get("name", ""))

                ## Seperate out the original course ID from the section name. It is the last thing after seperating by space
                origSisId = sectionName.split(" ")[-1].strip() if sectionName else None

                if origSisId and parentSisId:
                    crosslistOrigToParent[origSisId.lower()] = parentSisId.lower()
                    crosslistedAwayCourseIds.add(origSisId)

            localSetup.logger.info(
                f"Built crosslist mapping: {len(crosslistOrigToParent)} original→parent course mappings, "
                f"{len(crosslistedAwayCourseIds)} courses with sections crosslisted away"
            )

            ## For each SIS enrollment where the course was crosslisted, also add a key
            ## with the parent course_id so Canvas enrollments under the parent match
            crosslistExpandedKeys = set()
            for courseId, userId, role in activeEnrollKeys:
                ## If this course has been crosslisted into a parent, add the parent variant
                if courseId in crosslistOrigToParent:
                    parentCourseId = crosslistOrigToParent[courseId]
                    crosslistExpandedKeys.add((parentCourseId, userId, role))

            activeEnrollKeys.update(crosslistExpandedKeys)
            localSetup.logger.info(
                f"After crosslist expansion: {len(activeEnrollKeys)} active enrollment keys "
                f"(added {len(crosslistExpandedKeys)} crosslisted variants)"
            )

        ## ══════════════════════════════════════════════════════════════════════
        ## Step 4: Active‑status pre‑filter
        ## ══════════════════════════════════════════════════════════════════════
        canvasCoursesDf = canvasCoursesDf[canvasCoursesDf["status"].astype(str).str.lower() == "active"].copy()
        if not canvasEnrollDf.empty:
            canvasEnrollDf = canvasEnrollDf[canvasEnrollDf["status"].astype(str).str.lower() == "active"].copy()

        localSetup.logger.info(
            f"After active-status filter: {len(canvasCoursesDf)} active courses, "
            f"{len(canvasEnrollDf)} active enrollments"
        )

        ## ══════════════════════════════════════════════════════════════════════
        ## Step 5: SIS exact-date filter on courses
        ## ══════════════════════════════════════════════════════════════════════
        ## A Canvas course is in scope only if its resolved (start, end) date pair
        ## exactly matches one of the date pairs present in the SIS feed.  This
        ## avoids treating courses that legitimately ended mid-window (e.g. Quad 1)
        ## as out-of-scope simply because their end date predates the overall span.
        ## Courses whose dates cannot be resolved are kept conservatively.
        inWindowMask = []
        for _, row in canvasCoursesDf.iterrows():
            startDt, endDt = resolveCourseDates(row, termDateDict)
            if startDt is None or endDt is None:
                inWindowMask.append(True)
            else:
                inWindowMask.append((startDt, endDt) in sisDateIntervals)

        canvasCoursesDf = canvasCoursesDf[inWindowMask].copy()
        localSetup.logger.info(
            f"After SIS exact-date filter: {len(canvasCoursesDf)} active courses with matching SIS date pair"
        )

        ## Build the set of canvas_course_ids whose term has not yet ended.
        ## Enrollments in ended-term courses are excluded from orphan detection
        ## because the SIS feed no longer carries historical enrollment records.
        now = datetime.now()
        enrollmentScopeCanvasIds = set()
        for _, row in canvasCoursesDf.iterrows():
            _, endDt = resolveCourseDates(row, termDateDict)
            if endDt is None or endDt >= now:
                enrollmentScopeCanvasIds.add(row["canvas_course_id"])

        localSetup.logger.info(
            f"Enrollment orphan scope: {len(enrollmentScopeCanvasIds)} course(s) in non-ended terms "
            f"(excluded {len(canvasCoursesDf) - len(enrollmentScopeCanvasIds)} ended-term course(s))"
        )

        ## ══════════════════════════════════════════════════════════════════════
        ## Step 6: Identify orphaned courses
        ## ══════════════════════════════════════════════════════════════════════
        orphanedCoursesDf = canvasCoursesDf[
            (canvasCoursesDf["created_by_sis"].astype(str).str.lower() == "true")
            & (canvasCoursesDf["status"].astype(str).str.lower() == "active")
            & (~canvasCoursesDf["course_id"].isin(activeSisCourseIds))
            & (~canvasCoursesDf["course_id"].isin(crosslistedAwayCourseIds))  ## NEW: exclude crosslisted courses
        ].copy()

        localSetup.logger.info(f"Identified {len(orphanedCoursesDf)} orphaned course(s)")

        ## Build a set of orphaned course canvas IDs for exclusion in enrollment step
        orphanedCanvasCourseIds = set(orphanedCoursesDf["canvas_course_id"])

        ## ══════════════════════════════════════════════════════════════════════
        ## Step 7: Identify orphaned enrollments in still‑active courses
        ## ══════════════════════════════════════════════════════════════════════
        orphanedEnrollRows = []
        if not canvasEnrollDf.empty:
            for _, eRow in canvasEnrollDf.iterrows():
                ## Skip any that are not SIS‑created and active
                if (
                    str(eRow.get("created_by_sis", "")).lower() != "true"
                    or str(eRow.get("status", "")).lower() != "active"
                ):
                    continue
                ## Parent course must NOT be in the orphaned‑courses set
                if eRow["canvas_course_id"] in orphanedCanvasCourseIds:
                    continue
                ## Skip enrollments in ended-term courses — SIS no longer carries those records
                if eRow["canvas_course_id"] not in enrollmentScopeCanvasIds:
                    continue
                ## Check if the enrollment key exists in the SIS feed
                enrollKey = (
                    str(eRow.get("course_id", "")).lower().strip(),
                    str(eRow.get("user_id", "")).strip(),
                    str(eRow.get("role", "")).lower().strip(),
                )
                if enrollKey not in activeEnrollKeys:
                    orphanedEnrollRows.append(eRow)

        localSetup.logger.info(
            f"Identified {len(orphanedEnrollRows)} orphaned enrollment(s) in still-active courses"
        )
        ## save the orpahned enroll rows to a csv file for reference
        outputResourcePath = localSetup.getInternalResourcePaths("Canvas")
        orphanEnrollmentsOutputPath = os.path.join(outputResourcePath, "orphaned_enrollments.csv")
        pd.DataFrame(orphanedEnrollRows).to_csv(orphanEnrollmentsOutputPath, index=False)


        ## ═══════════════════════════════════════════════���══════════════════════
        ## Step 8 & 9: Process orphaned courses and enrollments in batches
        ## ══════════════════════════════════════════════════════════════════════
        ## Thread‑safe summary (lists are append‑safe in CPython)
        summary = {
            "deleted_silently":             [],
            "deleted_with_enrollments":     [],
            "skipped_grades":               [],
            "orphaned_enrollments_deleted": [],
        }

        MAX_WORKERS = 25  ## Max concurrent threads in the pool

        ## ── Orphaned courses ─────────────────────────────────────────────────
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [
                executor.submit(processOrphanedCourse, courseRow, canvasEnrollDf, summary)
                for _, courseRow in orphanedCoursesDf.iterrows()
            ]
            for i, future in enumerate(as_completed(futures), 1):
                if i % MAX_WORKERS == 0:
                    localSetup.logger.info(f"Orphaned courses: {i} threads completed")
            localSetup.logger.info(f"Orphaned courses: all {len(futures)} threads completed")

        ## ── Orphaned enrollments ─────────────────────────────────────────────
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [
                executor.submit(processOrphanedEnrollment, enrollRow, summary)
                for enrollRow in orphanedEnrollRows
            ]
            for i, future in enumerate(as_completed(futures), 1):
                if i % MAX_WORKERS == 0:
                    localSetup.logger.info(f"Orphaned enrollments: {i} threads completed")
            localSetup.logger.info(f"Orphaned enrollments: all {len(futures)} threads completed")

        ## ════════════════════════════════════════════════════════════════��═════
        ## Step 10: Final summary log
        ## ══════════════════════════════════════════════════════════════════════
        localSetup.logger.info("===============================================")
        localSetup.logger.info("        Remove Orphaned SIS Items -- Summary")
        localSetup.logger.info("===============================================")
        localSetup.logger.info(
            f"  Courses deleted silently (no enrollments, no grades): "
            f"{len(summary['deleted_silently'])}"
        )
        localSetup.logger.info(
            f"  Courses deleted after enrollment cleanup (email sent): "
            f"{len(summary['deleted_with_enrollments'])}"
        )
        localSetup.logger.info(
            f"  Courses skipped -- grades found (email sent): "
            f"{len(summary['skipped_grades'])}"
        )
        localSetup.logger.info(
            f"  Orphaned enrollments deleted (in still-active courses): "
            f"{len(summary['orphaned_enrollments_deleted'])}"
        )
        localSetup.logger.info("===============================================")

        ## Send a summary email if anything was actually deleted
        totalDeleted = (
            len(summary["deleted_silently"])
            + len(summary["deleted_with_enrollments"])
            + len(summary["orphaned_enrollments_deleted"])
        )
        if totalDeleted > 0:
            def _bulletList(items):
                return "".join(f"<li>{item}</li>" for item in items) if items else "<li>(none)</li>"

            summaryEmailBody = (
                f"<!DOCTYPE html><html><body>"
                f"<h2>Remove Orphaned SIS Items — Run Summary</h2>"
                f"<p><b>Courses deleted silently</b> (no enrollments, no grades): "
                f"{len(summary['deleted_silently'])}</p>"
                f"<ul>{_bulletList(summary['deleted_silently'])}</ul>"
                f"<p><b>Courses deleted after enrollment cleanup:</b> "
                f"{len(summary['deleted_with_enrollments'])}</p>"
                f"<ul>{_bulletList(summary['deleted_with_enrollments'])}</ul>"
                f"<p><b>Courses skipped — grades found (manual review required):</b> "
                f"{len(summary['skipped_grades'])}</p>"
                f"<ul>{_bulletList(summary['skipped_grades'])}</ul>"
                f"<p><b>Orphaned enrollments deleted (in still-active courses):</b> "
                f"{len(summary['orphaned_enrollments_deleted'])}</p>"
                f"<ul>{_bulletList(summary['orphaned_enrollments_deleted'])}</ul>"
                f"</body></html>"
            )

            sendOutlookEmail(
                p1_microsoftUserName=serviceEmailAccount,
                p1_subject="Remove Orphaned SIS Items — Run Summary",
                p1_body=summaryEmailBody,
                p1_recipientEmailList=f"{scriptLibrary}@nnu.edu",
                p1_shared_mailbox=f"{scriptLibrary}@nnu.edu",
            )
            localSetup.logger.info("Summary email sent")

    except Exception as Error:
        errorHandler.sendError(functionName, Error)


## ────────────────────────────────────────────────────────────────────────────��
## Entry point
## ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    os.chdir(os.path.dirname(__file__))
    removeOrphanedSisItems()
    input("Press enter to exit")