## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

## External libraries
import os, sys, json, pandas as pd, shutil, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Optional

## Add ResourceModules to the system path
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

## New resource modules
from Local_Setup import LocalSetup
from TLC_Common import makeApiCall, isPresent, isMissing
from Canvas_Report import CanvasReport
from Common_Configs import coreCanvasApiUrl, canvasAccessToken
from Error_Email import errorEmail


## Define the script name, purpose, and external requirements for logging and Error reporting purposes
scriptName = os.path.basename(__file__).replace(".py", "")

scriptPurpose = r"""
Retrieve enrolled student's course level grade and activity information
"""
externalRequirements = r"""
To function properly, this script requires access to NNU's current enrollment list, the the corresponding Canvas enrollment list, Canvas API
and the "{SISResourcePath}\\output\\pharos" folder
"""

## Create the localsetup variable
localSetup = LocalSetup(datetime.now(), __file__)  ## sets cwd, paths, logs, date parts

## Setup the Error handler
ErrorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)

def _parse_iso_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        # handle trailing 'Z' (UTC) by converting to +00:00 for fromisoformat
        if dt_str.endswith("Z"):
            dt_str = dt_str[:-1] + "+00:00"
        return datetime.fromisoformat(dt_str)
    except Exception:
        return None

## This function recursively parses discussion post replies to search for a particular user's latest reply
def getStuMostRecentGradedDiscussionPostDateRecursive(p2_stuCanvasId, p1_stuLastGradedDiscussionPostDate, p1_post):
    """
    Recursively search p1_post and its replies for replies by p2_stuCanvasId.
    Return the most recent 'updated_at' value (string) found for that user,
    or p1_stuLastGradedDiscussionPostDate if nothing newer is found.
    Comparison is done on strings (assumes consistent ISO-8601 format).
    """
    functionName = "Get a Student's Most Recent Graded Discussion Post Date Recursive"

    currentBestStr = p1_stuLastGradedDiscussionPostDate if p1_stuLastGradedDiscussionPostDate else None

    replies = p1_post.get("replies") or []
    for reply in replies:
        replyUser = reply.get("user_id")
        replyUpdatedStr = reply.get("updated_at")

        # If this reply is from the target student, compare timestamps as strings
        if replyUser == p2_stuCanvasId and replyUpdatedStr:
            if currentBestStr is None or replyUpdatedStr > currentBestStr:
                currentBestStr = replyUpdatedStr

        # Recurse into nested replies and use its returned result
        recResult = getStuMostRecentGradedDiscussionPostDateRecursive(
            p2_stuCanvasId,
            currentBestStr,
            reply
        )
        if recResult:
            if currentBestStr is None or recResult > currentBestStr:
                currentBestStr = recResult

    return currentBestStr

## This function returns the user's most recent graded discussion post date
def getStuMostRecentGradedDiscussionPostDate(p1_stuDiscussionListAPIUrl, p1_stuCanvasId):
    functionName = "Get a Student's Most Recent Graded Discussion Post Date"
    stuDiscussionListObject, _ = makeApiCall(localSetup, p1_apiUrl=p1_stuDiscussionListAPIUrl)

    if stuDiscussionListObject.status_code == 200:
        stuDiscussionList = json.loads(stuDiscussionListObject.text)
        stuDiscussionListObject.close()
        stuLastGradedDiscussionPostDate = None

        for discussion in stuDiscussionList:
            if "assignment" in discussion:
                discussionCanvasId = discussion["id"]
                viewUrl = f"{p1_stuDiscussionListAPIUrl}/{discussionCanvasId}/view"
                viewObject, _ = makeApiCall(localSetup, p1_apiUrl=viewUrl)

                if viewObject.status_code == 200:
                    viewDict = json.loads(viewObject.text)
                    viewObject.close()
                    for post in viewDict.get("view", []):
                        if post.get("user_id") == p1_stuCanvasId:
                            if not stuLastGradedDiscussionPostDate or stuLastGradedDiscussionPostDate < post["updated_at"]:
                                stuLastGradedDiscussionPostDate = post["updated_at"]
                            getStuMostRecentGradedDiscussionPostDateRecursive(p1_stuCanvasId, stuLastGradedDiscussionPostDate, post)

        return stuLastGradedDiscussionPostDate

## This function updates the end date of a Canvas course to unconclude or reconclude it
## and returns the original end date before the update
def updateCourseEndDate(courseId, newEndDate):
    functionName = "Update Course End Date"

    ## Make the API call to retrieve the current course object
    courseObject, _ = makeApiCall(localSetup, p1_apiUrl=f"{coreCanvasApiUrl}/courses/{courseId}")

    ## If retrieval fails, log and return blank values
    if courseObject.status_code != 200:
        localSetup.logger.warning(
            f"Failed to retrieve course {courseId} before updating end date. "
            f"Status Code: {courseObject.status_code}, Message: {courseObject.text}"
        )
        return "", ""

    ## Parse the current course data
    courseData = json.loads(courseObject.text)
    courseObject.close()

    ## Extract the original end date
    originalEndDate = courseData.get("end_at", "")

    ## Define the payload with the new end date
    payload = {
        "course": {
            "end_at": newEndDate
        }
    }

    ## Make the API call to update the course end date
    updateResponse, _ = makeApiCall(localSetup,
        p1_apiUrl=f"{coreCanvasApiUrl}/courses/{courseId}",
        p1_payload=payload,
        p1_apiCallType="put",
    )

    ## Log the result of the update
    if updateResponse.status_code == 200:
        localSetup.logger.info(f"Successfully updated end date for course {courseId} to {newEndDate}")
    else:
        localSetup.logger.warning(
            f"Failed to update end date for course {courseId}. "
            f"Status Code: {updateResponse.status_code}, Message: {updateResponse.text}"
        )

    ## Return the original end date and the update response
    return originalEndDate, updateResponse

## This function retrieves the Canvas enrollment object for a student
## and returns the enrollment object and original course end date if the course was concluded
def getEnrollmentApiObject(enrollmentId, courseId, parentCourseId, stuId, enrollmentDeleted):
    functionName = "Get Enrollment API Object"
    try:
        ## If the enrollment was previously deleted
        if enrollmentDeleted:
            ## Determine target course and section
            targetCourseId = parentCourseId or courseId
            enrollmentApiUrl = f"{coreCanvasApiUrl}/courses/sis_course_id:{targetCourseId}/enrollments"

            payload = {
                "enrollment[user_id]": f"sis_user_id:{stuId}",
                "enrollment[type]": "StudentEnrollment",
                "enrollment[enrollment_state]": "active"
            }

            ## If parent course exists, ## try to find matching section
            if parentCourseId:
                sectionApiUrl = f"{coreCanvasApiUrl}/courses/sis_course_id:{parentCourseId}/sections"
                sectionResponse, _ = makeApiCall(localSetup, p1_apiUrl=sectionApiUrl)

                if sectionResponse.status_code != 200:
                    localSetup.logger.warning(f"Failed to retrieve sections for parent course {parentCourseId}")
                    ErrorHandler.sendError(functionName, f"Section retrieval failed for parent course {parentCourseId}")
                    return False, ""

                sectionData = json.loads(sectionResponse.text)
                sectionResponse.close()

                crosslistedSectionId = None
                for section in sectionData:
                    if courseId in section.get("name", ""):
                        crosslistedSectionId = section.get("id")
                        break

                if crosslistedSectionId:
                    payload["enrollment[course_section_id]"] = crosslistedSectionId

            ## Attempt to re-enroll the student
            enrollmentObject, _ = makeApiCall(localSetup, 
                p1_apiUrl=enrollmentApiUrl,
                p1_payload=payload,
                p1_apiCallType="post",
            )

            ## If course is concluded, update end date temporarily
            if enrollmentObject.status_code == 400:
                Error400Message = json.loads(enrollmentObject.content.decode('utf-8')).get('message', '')
                if Error400Message == "Can't add an enrollment to a concluded course.":
                    futureEndDate = (
                        datetime.combine(
                            datetime.utcnow().date() + timedelta(days=1)
                            , datetime.min.time()) + timedelta(hours=23, minutes=59)
                        ).strftime("%Y-%m-%dT%H:%M:%SZ")
                    originalEndDate, _ = updateCourseEndDate(f"sis_course_id:{targetCourseId}", futureEndDate)

                    ## Retry enrollment after unconcluding
                    enrollmentObject, _ = makeApiCall(localSetup, 
                        p1_apiUrl=enrollmentApiUrl,
                        p1_payload=payload,
                        p1_apiCallType="post",
                    )

                    ## Return enrollment object and original end date
                    return enrollmentObject, originalEndDate

            ## Return enrollment object and blank end date if no end date update was needed
            return enrollmentObject, ""

        ## If enrollment is not deleted, retrieve it normally
        else:
            enrollmentApiUrl = f"{coreCanvasApiUrl}/accounts/1/enrollments/{enrollmentId}"
            enrollmentObject, _ = makeApiCall(localSetup, 
                p1_apiUrl=enrollmentApiUrl,
            )
            return enrollmentObject, ""

    except Exception as Error:
        ErrorHandler.sendError(functionName, Error)
        return False, ""

## This function retrieves assignment analytics for a student and returns:
## - last submission date
## - number of missed assignments
## - number of assignments graded 0
def getStudentAssignmentAnalytics(courseApiUrl, stuId):
    functionName = "Get Student Assignment Analytics"
    stuLastSubmissionDateTime = ""
    stuNumOfMissedAssignments = 0
    stuNumOfAssignmentsGradedZero = 0

    analyticsUrl = f"{courseApiUrl}analytics/users/sis_user_id:{stuId}/assignments"
    analyticsObject, _ = makeApiCall(localSetup, p1_apiUrl=analyticsUrl)

    if analyticsObject.status_code == 200:
        analyticsDict = json.loads(analyticsObject.text)
        analyticsObject.close()

        for submission in analyticsDict:
            if "submission" not in submission:
                continue

            submittedAt = submission["submission"].get("submitted_at")
            if submittedAt:
                currentSubmissionDate = datetime.strptime(submittedAt, "%Y-%m-%dT%H:%M:%SZ")
                if not stuLastSubmissionDateTime or stuLastSubmissionDateTime < currentSubmissionDate:
                    stuLastSubmissionDateTime = currentSubmissionDate

            if submission.get("status") == "missing":
                stuNumOfMissedAssignments += 1

            if submission["submission"].get("score") == 0:
                stuNumOfAssignmentsGradedZero += 1

    return stuLastSubmissionDateTime, stuNumOfMissedAssignments, stuNumOfAssignmentsGradedZero

## This function retrieves the student's last participation date from Canvas analytics
def getStudentParticipationDate(courseApiUrl, stuId):
    functionName = "Get Student Participation Date"
    participationDate = ""
    activityUrl = f"{courseApiUrl}analytics/users/sis_user_id:{stuId}/activity"
    activityObject, _ = makeApiCall(localSetup, p1_apiUrl=activityUrl)

    if activityObject.status_code == 200:
        activityDict = json.loads(activityObject.text)
        activityObject.close()
        participations = activityDict.get("participations", [])
        if participations:
            _, rawDate = list(participations[-1].items())[0]
            participationDate = datetime.strptime(rawDate, "%Y-%m-%dT%H:%M:%SZ")

    return participationDate

## This function determines the final course activity and participation dates
def resolveFinalActivityAndParticipationDates(
    lastSubmissionDate,
    lastActivityDate,
    lastGradedDiscussionDate
):
    functionName = "Resolve Final Activity and Participation Dates"
    convertedActivityDate = ""
    convertedParticipationDate = ""

    if lastSubmissionDate and lastGradedDiscussionDate:
        if lastSubmissionDate >= lastGradedDiscussionDate:
            convertedParticipationDate = lastSubmissionDate.strftime("%m-%d")
        else:
            convertedParticipationDate = lastGradedDiscussionDate.strftime("%m-%d")
    elif lastSubmissionDate:
        convertedParticipationDate = lastSubmissionDate.strftime("%m-%d")

    if convertedParticipationDate and lastActivityDate:
        if lastSubmissionDate and lastSubmissionDate >= lastActivityDate:
            convertedActivityDate = lastSubmissionDate.strftime("%m-%d")
        else:
            convertedActivityDate = lastActivityDate.strftime("%m-%d")
    elif lastActivityDate:
        convertedActivityDate = lastActivityDate.strftime("%m-%d")

    return convertedActivityDate, convertedParticipationDate

## This function updates the student's course data dictionary with final values
def updateStudentCourseData(
    stuCoursesData,
    targetCourseSisId,
    convertedActivityDate,
    convertedParticipationDate,
    missedAssignments,
    zeroGrades
):
    functionName = "Update Student Course Data"
    stuCoursesData[targetCourseSisId]["Last Course Activity"] = convertedActivityDate
    stuCoursesData[targetCourseSisId]["Last Course Participation"] = convertedParticipationDate
    stuCoursesData[targetCourseSisId]["Number of Missed Assignments"] = str(missedAssignments)
    stuCoursesData[targetCourseSisId]["Number of Assignments graded 0"] = str(zeroGrades)


## This function deletes a reactivated enrollment if needed
## and restores the original course end date if it was temporarily changed
def handleEnrollmentDeletion(stuId, enrollmentId, courseId, originalEndDate=""):
    functionName = "Handle Enrollment Deletion"
    try:
        ## Construct the deletion API URL
        deletionUrl = f"{coreCanvasApiUrl}/accounts//enrollments/{enrollmentId}"
        payload = {"task": "delete"}

        ## Attempt to delete the enrollment
        deletionResponse, _ = makeApiCall(localSetup, 
            p1_apiUrl=deletionUrl,
            p1_payload=payload,
            p1_apiCallType="delete",
        )
        
        ## Restore original end date if provided
        if originalEndDate:
            _, restoreResponse = updateCourseEndDate(f"sis_course_id:{courseId}", originalEndDate)
            if hasattr(restoreResponse, "status_code") and restoreResponse.status_code == 200:
                localSetup.logger.info(f"Successfully restored original end date for course {courseId} to {originalEndDate}")
            else:
                localSetup.logger.warning(
                    f"Failed to restore original end date for course {courseId}. "
                    f"Status Code: {getattr(restoreResponse, 'status_code', 'No response')}, "
                    f"Message: {getattr(restoreResponse, 'text', '')}"
                )

        ## If deletion is successful
        if deletionResponse and deletionResponse.status_code == 200:
            localSetup.logger.info(f"Successfully deleted enrollment {enrollmentId} for student {stuId} in course {courseId}")

            return True

        ## If deletion failed
        localSetup.logger.warning(
            f"Enrollment deletion failed for {enrollmentId} in course {courseId} for student {stuId}. "
            f"Status Code: {deletionResponse.status_code if deletionResponse else 'No response'}"
        )
        return False

    except Exception as Error:
        ErrorHandler.sendError(functionName, Error)
        return False

## This function determines whether the course is published or not
def determineCoursePublicationStatus(targetCourseSisId, parentCourseId, unpublishedCoursesList):
    functionName = "Determine Course Publication Status"
    if targetCourseSisId not in unpublishedCoursesList:
        return "Yes"
    elif parentCourseId and parentCourseId not in unpublishedCoursesList:
        return "Yes"
    return "No"

## This function checks if the student is enrolled in the target course and returns enrollment data or a skip signal
def validateStudentEnrollment(stuId, targetCourseSisId, canvasEnrollmentsDf):
    functionName = "Validate Student Enrollment"
    ## Look for a match of the SIS enrollment within the student's Canvas enrollment list
    enrollmentDf = canvasEnrollmentsDf[
        (canvasEnrollmentsDf["course_id"] == targetCourseSisId) &
        (canvasEnrollmentsDf["user_id"] == stuId)
    ]
    return enrollmentDf

## This function gets the student's most recent course specific activity and grade data
def getStuCourseData(
    p2_stuId,
    p1_sisCourseIds,
    p2_stuCoursesData,
    p1_canvasEnrollmentsDf,
    p1_canvasUserId,
    p1_targetCourseId,
    p2_unpublishedCoursesList
):
    functionName = "Get Stu Course Data"
    try:

        ## Validate enrollment
        enrollmentDf = validateStudentEnrollment(p2_stuId, p1_targetCourseId, p1_canvasEnrollmentsDf)

        ## Skip if already processed
        if p1_targetCourseId in p2_stuCoursesData and "Published" in p2_stuCoursesData[p1_targetCourseId]:
            return "Completed Unenrolled Entry"

        ## Initialize course data
        p2_stuCoursesData[p1_targetCourseId] = {
            "Published": "No",
            "Current Grade": "",
            "Number of Missed Assignments": "",
            "Number of Assignments graded 0": "",
            "Last Course Activity": "",
            "Last Course Participation": ""
        }

        ## Determine publication status
        parentCourseId = ""
    
        ## Check for crosslisting by looking for parent course ID
        if isMissing(enrollmentDf):
            for secondaryCourseId in p1_canvasEnrollmentsDf["course_id"].unique():
                if not parentCourseId:
                    sectionApiUrl = f"{coreCanvasApiUrl}/courses/sis_course_id:{secondaryCourseId}/sections"
                    sectionResponse, _ = makeApiCall(localSetup, p1_apiUrl=sectionApiUrl)
                    if sectionResponse.status_code == 200:
                        sectionData = json.loads(sectionResponse.text)
                        for section in sectionData:
                            if p1_targetCourseId in section.get("name"):
                                parentCourseId = secondaryCourseId
                                break
                else:
                    enrollmentDf = validateStudentEnrollment(p2_stuId, parentCourseId, p1_canvasEnrollmentsDf)
                    break
    
        ## If not enrolled, skip
        if isMissing(enrollmentDf):
            localSetup.logger.warning(f"Student {p2_stuId} is not enrolled in course {p1_targetCourseId}")
            del p2_stuCoursesData[p1_targetCourseId]
            return "Remove"

        ## Get enrollment ID
        ## Prefer an active (non-deleted) enrollment row when both exist in the includeDeleted report
        activeDf = enrollmentDf[enrollmentDf["status"].str.lower() != "deleted"]
        selectedDf = activeDf if isPresent(activeDf) else enrollmentDf

        enrollmentId = selectedDf["canvas_enrollment_id"].values[0]
        p2_stuCoursesData[p1_targetCourseId]["canvas_enrollment_id"] = enrollmentId

        ## Check if enrollment is deleted (only true when NO active row exists)
        enrollmentDeleted = isMissing(activeDf)

        ## Get enrollment object
        enrollmentObject, oldCourseEndDate = getEnrollmentApiObject(enrollmentId, p1_targetCourseId, parentCourseId,  p2_stuId, enrollmentDeleted)

        ## If failed
        if not enrollmentObject or enrollmentObject.status_code != 200:
            return "Incomplete"

        ## Retrieve the publication status
        publicationStatus = determineCoursePublicationStatus(p1_targetCourseId, parentCourseId, p2_unpublishedCoursesList)

        ## Set the publication status to "Unenrolled" if enrollment was deleted, otherwise set to actual status
        if enrollmentDeleted:
            p2_stuCoursesData[p1_targetCourseId]["Published"] = "Unenrolled"
        else:
            p2_stuCoursesData[p1_targetCourseId]["Published"] = publicationStatus

        ## If unpublished, skip
        if publicationStatus == "No":
            return "Completed"

        ## Get enrollment data
        enrollmentData = json.loads(enrollmentObject.text)
        enrollmentObject.close()

        ## Get grade
        if "grades" in enrollmentData and enrollmentData["grades"]:
            p2_stuCoursesData[p1_targetCourseId]["Current Grade"] = str(enrollmentData["grades"].get("current_score", ""))

        ## Get last activity date
        lastActivityRaw = enrollmentData.get("last_activity_at")
        lastActivityDate = datetime.strptime(lastActivityRaw, "%Y-%m-%dT%H:%M:%SZ") if lastActivityRaw else ""

        ## Determine course API URL
        courseApiUrl = f"{coreCanvasApiUrl}/courses/sis_course_id:{parentCourseId or p1_targetCourseId}/"

        ## Get assignment analytics
        lastSubmissionDate, missedAssignments, zeroGrades = getStudentAssignmentAnalytics(courseApiUrl, p2_stuId)

        ## Get participation date
        participationDate = getStudentParticipationDate(courseApiUrl, p2_stuId)

        ## Get graded discussion post date
        discussionListUrl = f"{courseApiUrl}discussion_topics"
        lastGradedDiscussionDateRaw = getStuMostRecentGradedDiscussionPostDate(discussionListUrl, p1_canvasUserId)
        lastGradedDiscussionDate = datetime.strptime(lastGradedDiscussionDateRaw, "%Y-%m-%dT%H:%M:%SZ") if lastGradedDiscussionDateRaw else ""

        ## Resolve final activity and participation dates
        convertedActivityDate, convertedParticipationDate = resolveFinalActivityAndParticipationDates(
            lastSubmissionDate,
            lastActivityDate,
            lastGradedDiscussionDate
        )

        ## Update course data
        updateStudentCourseData(
            p2_stuCoursesData,
            p1_targetCourseId,
            convertedActivityDate,
            convertedParticipationDate,
            missedAssignments,
            zeroGrades
        )

        ## Update global last canvas activity if needed
        if p2_stuCoursesData.get("Last Canvas Activity", "") < convertedActivityDate:
            p2_stuCoursesData["Last Canvas Activity"] = convertedActivityDate

        ## Handle re-deletion if enrollment was reactivated
        if enrollmentDeleted:
            handleEnrollmentDeletion(p2_stuId, enrollmentId, parentCourseId or p1_targetCourseId, oldCourseEndDate)

    except Exception as Error:
         ErrorHandler.sendError("functionName", Error)

## This threaded function gets each student's course data related to each of their enrollments for the current term
def getStuCurrentCoursesData(
    p1_stuId,
    p1_stuCoursesDataDict,
    p1_filteredCanvasEnrollmentsDf,
    p1_unpublishedCoursesList,
    p1_filteredSisEnrollmentsDf,
    p1_deletedSisCourseIds
):
    functionName = "Get Stu Current Course Data"
    try:

        ## Retrieve the student's SIS course enrollment list
        sisCourseIdsDf = p1_filteredSisEnrollmentsDf[
            p1_filteredSisEnrollmentsDf["user_id"] == p1_stuId
        ]["course_id"]
        sisCourseIdsDf = list(set(sisCourseIdsDf))  ## Remove duplicates

        ## Retrieve the student's Canvas course enrollment list but exclude any courses that are deleted in SIS
        canvasEnrollmentsDf = p1_filteredCanvasEnrollmentsDf[
            (p1_filteredCanvasEnrollmentsDf["user_id"] == str(p1_stuId)) &
            (~p1_filteredCanvasEnrollmentsDf["course_id"].isin(p1_deletedSisCourseIds))
        ]

        loopCounter = 0

        ## While there are still course entries missing "Published" and loop count is under limit
        while (
            not all(
                "Published" in courseData
                for courseData in p1_stuCoursesDataDict.values()
                if isinstance(courseData, dict)
            )
            and loopCounter < 10
        ):
            loopCounter += 1
            ## Fetch course data concurrently
            with ThreadPoolExecutor(max_workers=25) as executor:
                for targetCourseId in sisCourseIdsDf:
                    if (targetCourseId in p1_stuCoursesDataDict.keys() 
                        and "Published" not in p1_stuCoursesDataDict[targetCourseId].keys()):
                        executor.submit(
                            getStuCourseData,
                            p1_stuId,
                            sisCourseIdsDf,
                            p1_stuCoursesDataDict,
                            canvasEnrollmentsDf,
                            p1_stuCoursesDataDict["stuCanvasId"],
                            targetCourseId,
                            p1_unpublishedCoursesList,
                        )

            ## Remove any entries that are not dicts or missing "Published"
            p1_stuCoursesDataDict = {
                key: value
                for key, value in p1_stuCoursesDataDict.items()
                if not isinstance(value, dict) or "Published" in value
            }

        ## Log completion
        localSetup.logger.info(f"{p1_stuId} completed")

    except Exception as Error:
         ErrorHandler.sendError("functionName", Error)

## This function retrieves and returns a dict of students' ids and last Canvas access report data points
def getStuLastCanvasAccessPoints(p1_stuIdsList):
    functionName = "Retrieve List of Student Last Canvas Access Report Data Points"

    try:

        ## Load the last Canvas access report
        lastCanvasAccessDf = CanvasReport.getCanvasUserLastAccessDf(localSetup)

        ## Initialize a dictionary to hold the last Canvas access data for each student
        lastCanvasAccessData = {}

        ## Iterate through the list of student IDs
        for p1_stuId in p1_stuIdsList:

            ## Retrieve the user's most recent Canvas Activity Date
            stuLastActivityDf = lastCanvasAccessDf[
                lastCanvasAccessDf["user sis id"] == str(p1_stuId)
            ]["last access at"]

            ## Convert the DataFrame to a list
            stuLastActivityList = stuLastActivityDf.tolist()

            ## Initialize the converted last canvas activity date
            convertedLastCanvasActivity = ""

            ## If the student doesn't have any last activity dates
            if not stuLastActivityList or str(stuLastActivityList[0]) == "nan":
                lastCanvasAccessData[p1_stuId] = ""
            else:
                ## Convert the last activity date to a datetime object
                rawLastActivity = datetime.strptime(str(stuLastActivityList[0]), "%Y-%m-%dT%H:%M:%S%z")

                ## Convert the last activity date to a month-day format
                convertedLastCanvasActivity = rawLastActivity.strftime("%m-%d")

                ## Add the converted last activity date to the dictionary
                lastCanvasAccessData[p1_stuId] = convertedLastCanvasActivity

        ## Return the last Canvas access data
        return lastCanvasAccessData

    except Exception as Error:
        ErrorHandler.sendError(functionName, Error)

## This threaded function gets each student's courses and adds them as keys with empty dicts to the student's course data dict
def getStuCoursesData(
    p1_stuId,
    p1_stuCoursesData,
    p1_filteredCanvasEnrollmentsDf,
    p1_filteredSisEnrollmentsDf
):
    functionName = "Get Stu Courses"
    try:

        ## Get Canvas user ID for the student
        ## Robustly handle user_id values that can be numeric strings or emails/non-numeric values
        numericUserIds = pd.to_numeric(p1_filteredCanvasEnrollmentsDf["user_id"], errors="coerce")
        maskNumeric = numericUserIds == pd.to_numeric(p1_stuId, errors="coerce")

        if maskNumeric.any():
            canvasIdDf = p1_filteredCanvasEnrollmentsDf.loc[maskNumeric, "canvas_user_id"]
        else:
            ## Fallback: compare string equality (safe when user_id is stored as string)
            canvasIdDf = p1_filteredCanvasEnrollmentsDf.loc[
                p1_filteredCanvasEnrollmentsDf["user_id"].astype(str) == str(p1_stuId),
                "canvas_user_id"
            ]

        ## If Canvas ID is found, store it
        if isPresent(canvasIdDf):
            p1_stuCoursesData["stuCanvasId"] = canvasIdDf.values[0]

        ## If no Canvas ID is found, skip
        if (
            not p1_stuCoursesData
            or "stuCanvasId" not in p1_stuCoursesData
            or not p1_stuCoursesData["stuCanvasId"]
            ):
            return

        ## Retrieve SIS course enrollment list and filter out the deleted
        p2_sisCourseIds = p1_filteredSisEnrollmentsDf[
            (p1_filteredSisEnrollmentsDf["user_id"] == p1_stuId) &
            (p1_filteredSisEnrollmentsDf["status"] != "deleted")
        ]["course_id"]
        p2_sisCourseIds = list(set(p2_sisCourseIds))  ## Remove duplicates

        ## For each SIS-enrolled course
        for p2_targetCourseId in p2_sisCourseIds:
            ## Skip if already added or if it's a chapel course
            if p2_targetCourseId in p1_stuCoursesData or "CHPL1000_01" in p2_targetCourseId:
                continue

            ## Create a placeholder for parent course ID (if crosslisted)
            parentCourseId = ""

            ## Initialize course entry
            p1_stuCoursesData[p2_targetCourseId] = {}

    except Exception as Error:
         ErrorHandler.sendError("functionName", Error)

## This function takes a list of current NNU enrollments and gets their Canvas enrollment related activity and grade information
def getNighthawk360Data(p1_oldEnrollmentDataDf):
    functionName = "Get NightHawk 360 Data"
    try:

        currentTerms = list(localSetup.getCurrentTerms())
        currentTermCodes = list(localSetup.getCurrentTermCodes())

        completeStudentEnrollmentDataDict = {}

        ## Retrieve the sis courses from the external input path that have a status of deleted
        sisCourseIdsDf = pd.read_csv(os.path.join(localSetup.getExternalResourcePath("SIS"), "canvas_course.csv"))
        deletedSisCourseIds = sisCourseIdsDf[
            sisCourseIdsDf["status"] == "deleted"
            ]["course_id"].tolist()


        ## Load Canvas enrollments
        undgCanvasEnrollmentsDf = CanvasReport.getEnrollmentsDf(localSetup, term=currentTermCodes[0], includeDeleted=True)
        gradCanvasEnrollmentsDf = CanvasReport.getEnrollmentsDf(localSetup, term=currentTermCodes[1], includeDeleted=True)
        combinedCanvasEnrollmentsDf = pd.concat([undgCanvasEnrollmentsDf, gradCanvasEnrollmentsDf], ignore_index=True)

        filteredCanvasEnrollmentsDf = combinedCanvasEnrollmentsDf[
            (combinedCanvasEnrollmentsDf["role"] == "student") &
            (~combinedCanvasEnrollmentsDf["course_id"].str.contains("CHPL1000_01", na=False)) &
            (
                combinedCanvasEnrollmentsDf["course_id"].str.contains(currentTerms[0]) |
                combinedCanvasEnrollmentsDf["course_id"].str.contains(currentTerms[1])
            )
        ]


        ## Load unpublished courses
        undgUnpublishedCoursesDf = CanvasReport.getUnpublishedCoursesDf(localSetup, term=currentTermCodes[0])
        gradUnpublishedCoursesDf = CanvasReport.getUnpublishedCoursesDf(localSetup, term=currentTermCodes[1])
        combinedUnpublishedCoursesDf = pd.concat([undgUnpublishedCoursesDf, gradUnpublishedCoursesDf], ignore_index=True)
        unpublishedCoursesList = combinedUnpublishedCoursesDf["sis id"].tolist()

        ## Load SIS enrollments
        sisEnrollmentsDf = pd.read_csv(f"{localSetup.getExternalResourcePath('SIS')}canvas_enroll.csv")
        filteredSisEnrollmentsDf = sisEnrollmentsDf[
            (sisEnrollmentsDf["role"] == "student") &
            (~sisEnrollmentsDf["course_id"].str.contains("CHPL1000_01")) &
            (
                sisEnrollmentsDf["course_id"].str.contains(currentTerms[0]) |
                sisEnrollmentsDf["course_id"].str.contains(currentTerms[1])
            )
        ].drop_duplicates(subset=["course_id", "user_id"]).copy()

        ## Set the user_id to string type to avoid mismatches
        filteredSisEnrollmentsDf["user_id"] = filteredSisEnrollmentsDf["user_id"].astype(str)

        ## Get unique student IDs
        uniqueStuIds = filteredSisEnrollmentsDf["user_id"].unique()

        ## Get last Canvas access data
        stuLastCanvasAccessData = getStuLastCanvasAccessPoints(uniqueStuIds.tolist())

        ## Initialize student data dict
        studentDataDict = {
            stuId: {"Last Canvas Activity": stuLastCanvasAccessData.get(stuId, "")}
            for stuId in uniqueStuIds
        }

        ## Seperate deleted enrollments
        deletedEnrollmentsDf = filteredSisEnrollmentsDf[filteredSisEnrollmentsDf["status"] == "deleted"].copy()

        if isPresent(p1_oldEnrollmentDataDf):

            ## Make sure oldEnrollmentDataDf keys are strings
            p1_oldEnrollmentDataDf["Student ID"]   = p1_oldEnrollmentDataDf["Student ID"].astype(str).str.strip()
            p1_oldEnrollmentDataDf["Course Number"] = p1_oldEnrollmentDataDf["Course Number"].astype(str).str.strip()

            ## If your old files might use dashes, normalize to underscores
            p1_oldEnrollmentDataDf["Course Number"] = p1_oldEnrollmentDataDf["Course Number"].str.replace("-", "_", regex=False)

            ## Normalize course_id to underscores as expected in left keys
            deletedEnrollmentsDf["course_id"] = deletedEnrollmentsDf["course_id"].str.replace("-", "_", regex=False)

            ## Make sure deletedEnrollmentsDf keys are strings
            deletedEnrollmentsDf["user_id"]  = deletedEnrollmentsDf["user_id"].astype(str).str.strip()
            deletedEnrollmentsDf["course_id"] = deletedEnrollmentsDf["course_id"].astype(str).str.strip()

            ## Merge
            unEnrolledStudentData = p1_oldEnrollmentDataDf.merge(
                deletedEnrollmentsDf[["user_id", "course_id"]],
                left_on=["Student ID", "Course Number"],
                right_on=["user_id", "course_id"],
                how="inner"
            ).drop(columns=["user_id", "course_id"])

            unEnrolledStudentData.fillna("", inplace=True)

            for p2_stuId in unEnrolledStudentData["Student ID"].astype(str).unique():
                stuDataDf = unEnrolledStudentData[unEnrolledStudentData["Student ID"] == p2_stuId]
                for p2_courseId in stuDataDf["Course Number"].unique():
                    studentDataDict[p2_stuId][p2_courseId] = {}
                    courseData = stuDataDf[stuDataDf["Course Number"] == p2_courseId].iloc[0]
                    for column in stuDataDf.columns:
                        if column == "Published":
                            studentDataDict[p2_stuId][p2_courseId][column] = "Unenrolled"
                        else:
                            studentDataDict[p2_stuId][p2_courseId][column] = courseData[column]

        ## Initialize course data for each student
        for stuId, coursesData in studentDataDict.items():
            ##if stuId ==	"132104": ## Test student value
                if "Published" not in coursesData:
                    getStuCoursesData(
                        p1_stuId=stuId,
                        p1_stuCoursesData=coursesData,
                        p1_filteredCanvasEnrollmentsDf=filteredCanvasEnrollmentsDf,
                        p1_filteredSisEnrollmentsDf=filteredSisEnrollmentsDf
                    )

        ## Threaded course data collection
        loopCounter = 0
        MAX_WORKERS = 100

        while (
            not all(
                "Published" in courseData
                for stuData in studentDataDict.values()
                for courseData in stuData.values()
                if isinstance(courseData, dict)
            )
            and loopCounter < 100
        ):
            loopCounter += 1

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = []
                for stuId, coursesData in studentDataDict.items():
                    if not all(
                        "Published" in courseData
                        for courseData in coursesData.values()
                        if isinstance(courseData, dict)
                    ):
                        if "Published" not in coursesData or not coursesData["Published"]:
                            futures.append(
                                executor.submit(
                                    getStuCurrentCoursesData,
                                    stuId,
                                    coursesData,
                                    filteredCanvasEnrollmentsDf,
                                    unpublishedCoursesList,
                                    filteredSisEnrollmentsDf,
                                    deletedSisCourseIds,
                                )
                            )

                completedCount = 0
                for future in as_completed(futures):
                    completedCount += 1
                    if completedCount % MAX_WORKERS == 0:
                        localSetup.logger.info(f"{completedCount} threads have been completed")

            localSetup.logger.info(f"{completedCount} threads have been completed")

        ## Merge into complete dict
        if not completeStudentEnrollmentDataDict:
            completeStudentEnrollmentDataDict = studentDataDict
        else:
            for stuId, coursesData in studentDataDict.items():
                if stuId not in completeStudentEnrollmentDataDict:
                    completeStudentEnrollmentDataDict[stuId] = coursesData
                else:
                    completeStudentEnrollmentDataDict[stuId].update(coursesData)

        return completeStudentEnrollmentDataDict

    except Exception as Error:
         ErrorHandler.sendError("functionName", Error)

## This function contains the start and end of the NightHawk 360 data report
def Nighthawk360CanvasReport():
    functionName = "Nighthawk 360 Canvas Report"
    try:

        localSetup.logger.info("\nBeginning the Nighthawk 360 Canvas Report")

        ## Load previous enrollment data if available
        oldEnrollmentDataDf = pd.DataFrame()
        activityPath = os.path.join (localSetup.getExternalResourcePath('Pharos'), "Enrollment_Data_Activity.csv")
        submissionPath = os.path.join (localSetup.getExternalResourcePath('Pharos'), "Enrollment_Data_Submissions.csv")

        if os.path.exists(activityPath) and os.path.exists(submissionPath):
            oldActivityDf = pd.read_csv(activityPath, sep="|")
            oldSubmissionDf = pd.read_csv(submissionPath, sep="|")
            oldEnrollmentDataDf = pd.merge(
                oldActivityDf,
                oldSubmissionDf,
                on=["Student ID", "Course Number"],
                how="outer"
            )

        ## Get current enrollment data
        enrollmentDataDict = getNighthawk360Data(oldEnrollmentDataDf)

        ## Prepare output files
        activityFilePath = os.path.join (localSetup.getInternalResourcePaths("Canvas"), "Enrollment_Data_Activity.csv")
        submissionFilePath = os.path.join (localSetup.getInternalResourcePaths("Canvas"), "Enrollment_Data_Submissions.csv")

        with open(activityFilePath, 'w', newline='') as activityCsv, \
             open(submissionFilePath, 'w', newline='') as submissionCsv:

            ## Write headers (pipe-delimited, quoted)
            activityCsv.write('Student ID|"Course Number"|"Last Canvas Activity"|"Published"|"Last Course Activity"|"Last Course Participation"\n')
            submissionCsv.write('Student ID|"Course Number"|"Current Grade"|"Number of Missed Assignments"|"Number of Assignments graded 0"\n')

            ## Write data rows
            for p1_stuId, p1_dataPoints in enrollmentDataDict.items():
                for p1_courseKey in p1_dataPoints:
                    if p1_courseKey in ["Last Canvas Activity", "stuCanvasId"] or not p1_dataPoints[p1_courseKey]:
                        continue

                    try: ## Irregular try clause, do not comment out in testing 
                        ## Format values for activity
                        formattedCourseCode = p1_courseKey.replace("_", "-")
                        formattedLastCanvasActivity = p1_dataPoints.get("Last Canvas Activity", "")
                        formattedPublished = p1_dataPoints[p1_courseKey].get("Published", "")
                        formattedLastCourseActivity = p1_dataPoints[p1_courseKey].get("Last Course Activity", "")
                        formattedLastCourseParticipation = p1_dataPoints[p1_courseKey].get("Last Course Participation", "")

                        ## Format values for submission
                        formattedCurrentGrade = p1_dataPoints[p1_courseKey].get("Current Grade", "")
                        formattedMissedAssignments = p1_dataPoints[p1_courseKey].get("Number of Missed Assignments", "")
                        formattedZeroGrades = p1_dataPoints[p1_courseKey].get("Number of Assignments graded 0", "")

                        ## Write activity row
                        activityCsv.write(
                            f'{p1_stuId}|"'
                            f'{formattedCourseCode}"|"'
                            f'{formattedLastCanvasActivity}"|"'
                            f'{formattedPublished}"|"'
                            f'{formattedLastCourseActivity}"|"'
                            f'{formattedLastCourseParticipation}"\n'
                        )

                        ## Write submission row
                        submissionCsv.write(
                            f'{p1_stuId}|"'
                            f'{formattedCourseCode}"|"'
                            f'{formattedCurrentGrade}"|"'
                            f'{formattedMissedAssignments}"|"'
                            f'{formattedZeroGrades}"\n'
                        )

                    except Exception as Error: ## Irregular try clause, do not comment out in testing
                        localSetup.logger.warning(
                            f"Error: {Error}\nOccurred while processing {p1_courseKey}:{p1_dataPoints[p1_courseKey]} for Student ID: {str(p1_stuId)}"
                        )
                        ErrorHandler.sendError("functionName", p1_ErrorInfo=...)

        ## Copy files to external output path
        shutil.copy(activityFilePath, activityPath)
        shutil.copy(submissionFilePath, submissionPath)

        localSetup.logger.info("\nActivity and Data CSVs saved to internal and external paths")

    except Exception as Error:
         ErrorHandler.sendError(f"{functionName}", Error)

if __name__ == "__main__":

    ## Start the NightHawk 360 data report
    Nighthawk360CanvasReport ()

    input("Press enter to exit")
