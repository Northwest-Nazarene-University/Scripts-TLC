## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Copilot

## Import Generic Modules
import os
import sys
import re
from datetime import datetime
import pandas as pd

## Ensure ResourceModules are importable when running from ActionModules
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

try:  ## Irregular try clause, do not comment out in testing
    from Local_Setup import LocalSetup
    from Error_Email import errorEmail
    from TLC_Common import getDesignatorSettingsDict, getDesignatorFilesByType, formatInstructorFirstNames
    from TLC_Action import (
        retrieveDataForRelevantCommunication,
    )
    from Core_Microsoft_Api import sendOutlookEmail
    from Canvas_Report import CanvasReport
except ImportError:
    from ResourceModules.Local_Setup import LocalSetup
    from ResourceModules.Error_Email import errorEmail
    from ResourceModules.TLC_Common import getDesignatorSettingsDict, getDesignatorFilesByType, formatInstructorFirstNames
    from ResourceModules.TLC_Action import (
        retrieveDataForRelevantCommunication,
    )
    from ResourceModules.Core_Microsoft_Api import sendOutlookEmail
    from ResourceModules.Canvas_Report import CanvasReport

## Set working directory
os.chdir(os.path.dirname(__file__))

scriptName = __file__
scriptPurpose = r"""
Send a department communication email to instructors of active outcome-associated courses
for an input term and target designator.
"""
externalRequirements = r"""
This script requires:
1. Access to Automated Outcome Tool Variables.xlsx and Canvas data.
2. A communication template text file named {targetDesignator}_department_communication(.txt).
3. Access to Microsoft Graph email sending through existing Core_Microsoft_Api tooling.
"""

## Initialize LocalSetup and helpers from ResourceModules
localSetup = LocalSetup(datetime.now(), __file__)
errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)

## Helper function to safely convert pandas values to strings
def _safeStr(value):
    if pd.isna(value):
        return ""
    return str(value).strip()


## Helper function to safely convert string-like values to boolean
def _safeBool(value):
    if isinstance(value, bool):
        return value
    valueString = _safeStr(value).lower()
    return valueString in ["true", "1", "yes", "y"]


## Load a department communication template file, trying multiple encodings to handle cp1252 files
def loadDepartmentCommunicationTemplate(templateFilePath):
    functionName = "Load Department Communication Template"

    try:
        if os.path.exists(templateFilePath):
            ## Try multiple encodings to handle both utf-8 and cp1252 encoded files
            for encoding in ["utf-8-sig", "cp1252", "utf-8"]:
                try:
                    with open(templateFilePath, "r", encoding=encoding) as templateFile:
                        localSetup.logger.info(
                            f"{functionName}: Loaded template from {templateFilePath} using {encoding}"
                        )
                        return templateFile.read()
                except UnicodeDecodeError:
                    continue
    except Exception as Error:
        errorHandler.sendError(functionName, Error)
        raise

    raise FileNotFoundError(
        f"No template file found at path: {templateFilePath}."
    )


## Normalize term code to four-character format (FA26, SU26, etc.)
def _normalizeTermCode(inputTerm):
    inputTerm = _safeStr(inputTerm).upper()
    if len(inputTerm) == 4:
        return inputTerm
    if len(inputTerm) == 6:
        return f"{inputTerm[:2]}{inputTerm[-2:]}"
    raise ValueError(f"Invalid term format: {inputTerm}. Expected FA26 or FA2026 style input.")


## Convert a term code to full year format (FA2026, SU2026, etc.) using the current century
def _termFullYearPrefix(termCode):
    yearSuffix = termCode[2:]
    if len(yearSuffix) != 2:
        raise ValueError(f"Invalid 2-digit term code: {termCode}")
    return f"{termCode[:2]}{localSetup.dateDict['century']}{yearSuffix}"


## Build list of Canvas course term prefixes based on input term and course level (undergrad, grad, or both)
def _buildCourseTermPrefixes(inputTerm, courseLevel):
    normalizedInputTerm = _normalizeTermCode(inputTerm)
    undergradPrefix = _termFullYearPrefix(normalizedInputTerm)
    gradTerm = CanvasReport.determineGradTerm(normalizedInputTerm)
    gradPrefix = _termFullYearPrefix(gradTerm)

    ## Return appropriate prefixes based on course level
    if courseLevel == "Undergraduate":
        return [undergradPrefix]
    if courseLevel == "Graduate":
        return [gradPrefix]
    if undergradPrefix == gradPrefix:
        return [undergradPrefix]
    return [undergradPrefix, gradPrefix]

## Extract instructor names and email addresses from a course row using column name pattern matching
def _collectInstructorData(courseRow):
    instructorNames = []
    instructorEmails = []

    ## Parse instructor columns matching pattern "Instructor_#N_name" and "Instructor_#N_email"
    for columnName in courseRow.index:
        if re.fullmatch(r"Instructor_#\d+_name", str(columnName)):
            nameValue = _safeStr(courseRow[columnName])
            if nameValue:
                instructorNames.append(nameValue)
        elif re.fullmatch(r"Instructor_#\d+_email", str(columnName)):
            emailValue = _safeStr(courseRow[columnName])
            if emailValue:
                instructorEmails.append(emailValue)

    ## Remove duplicate entries while preserving order
    uniqueNames = list(dict.fromkeys(instructorNames))
    uniqueEmails = list(dict.fromkeys(instructorEmails))
    return uniqueNames, uniqueEmails


## Build the email body by substituting template placeholders with course and instructor information
def _buildEmailBody(templateHtml, instructorNames, courseName, emailSignature=""):
    instructorNameText = formatInstructorFirstNames(instructorNames)
    emailBody = templateHtml
    signatureText = _safeStr(emailSignature)
    ## Replace template placeholders with actual values
    emailBody = emailBody.replace("[Instructor Name]", instructorNameText)
    emailBody = emailBody.replace("[Course Name]", courseName)
    emailBody = emailBody.replace(
        "{hyperlinked to https://nnu.co1.qualtrics.com/jfe/form/SV_b43fEMvdoOw6WeG}",
        "",
    )

    ## Apply signature when provided
    if "[Signature]" in emailBody:
        emailBody = emailBody.replace("[Signature]", signatureText)
    elif signatureText and signatureText not in emailBody:
        emailBody = f"{emailBody}<br><br>{signatureText}"

    return emailBody


## Parse email subject from communication template file name
def _getEmailSubjectFromTemplatePath(templatePath, fallbackCourseName):
    fileStem = os.path.splitext(os.path.basename(templatePath))[0]
    fileStemParts = fileStem.split("_")

    ## Naming convention: {targetDesignator}_{Email Subject}_{FileType}
    if len(fileStemParts) >= 3:
        subjectText = "_".join(fileStemParts[1:-1]).strip()
        if subjectText:
            return subjectText

    return f"{fallbackCourseName}: UCTC Course Assessment Survey"


def _normalizeToken(rawValue):
    """Normalize free-text tokens for resilient matching.

    Args:
        rawValue: Value to normalize.

    Returns:
        str: Lowercase alphanumeric/underscore token.
    """
    ## Step 1: Convert to a safe string value.
    textValue = _safeStr(rawValue).lower()

    ## Step 2: Replace non-alphanumeric runs with underscores.
    textValue = re.sub(r"[^a-z0-9]+", "_", textValue)

    ## Step 3: Trim leading/trailing underscores.
    return textValue.strip("_")


def _findDepartmentCommunicationSchedulePath(targetDesignator):
    """Find the CSV schedule path for a target designator.

    Args:
        targetDesignator (str): Target designator code.

    Returns:
        str: Schedule CSV full path, or empty string when none found.
    """
    ## Step 1: Resolve the designator tools directory.
    tlcToolsPath = localSetup.getExternalResourcePath("TLC")
    designatorPath = os.path.join(tlcToolsPath, _safeStr(targetDesignator).upper())

    ## Step 2: Return empty if the designator directory does not exist.
    if not os.path.isdir(designatorPath):
        return ""

    targetPrefix = f"{_safeStr(targetDesignator).lower()}_"

    ## Step 3: Scan direct files and pick the first likely schedule CSV.
    candidatePaths = []
    for fileName in sorted(os.listdir(designatorPath)):
        fullPath = os.path.join(designatorPath, fileName)
        if not os.path.isfile(fullPath):
            continue
        if not fileName.lower().endswith(".csv"):
            continue

        fileStem = os.path.splitext(fileName)[0].lower()
        if not fileStem.startswith(targetPrefix):
            continue
        if "communication" in fileStem and "schedule" in fileStem:
            candidatePaths.append(fullPath)

    return candidatePaths[0] if candidatePaths else ""


def _buildScheduleColumnsMap(scheduleDf):
    """Create a normalized lookup map for schedule dataframe columns.

    Args:
        scheduleDf (pd.DataFrame): Raw schedule dataframe.

    Returns:
        dict: normalized_column_name -> original_column_name.
    """
    ## Step 1: Normalize each source column to a token key.
    columnsMap = {}
    for columnName in scheduleDf.columns:
        normalizedName = _normalizeToken(columnName)
        if normalizedName and normalizedName not in columnsMap:
            columnsMap[normalizedName] = columnName
    return columnsMap


def _resolveScheduleColumn(columnsMap, aliasList):
    """Resolve a schedule column by trying normalized aliases.

    Args:
        columnsMap (dict): normalized name map from _buildScheduleColumnsMap.
        aliasList (list[str]): Candidate aliases in preferred order.

    Returns:
        str: Original dataframe column name, or empty string if unresolved.
    """
    ## Step 1: Attempt aliases in order and return first match.
    for aliasValue in aliasList:
        normalizedAlias = _normalizeToken(aliasValue)
        if normalizedAlias in columnsMap:
            return columnsMap[normalizedAlias]
    return ""


def _loadDepartmentCommunicationScheduleDf(targetDesignator):
    """Load and normalize the department communication schedule CSV.

    Args:
        targetDesignator (str): Target designator code.

    Returns:
        pd.DataFrame: Normalized schedule dataframe with expected columns,
        or empty dataframe when no valid schedule file is found.
    """
    functionName = "Load Department Communication Schedule"

    try:
        ## Step 1: Locate schedule CSV file for this designator.
        schedulePath = _findDepartmentCommunicationSchedulePath(targetDesignator)
        if not schedulePath:
            return pd.DataFrame()

        ## Step 2: Load the CSV and normalize expected column names.
        scheduleDf = pd.read_csv(schedulePath)
        if scheduleDf.empty:
            return pd.DataFrame()

        columnsMap = _buildScheduleColumnsMap(scheduleDf)
        titleColumn = _resolveScheduleColumn(
            columnsMap,
            ["Communication Title", "Title", "Document Title", "Template Title"],
        )
        sendWeekColumn = _resolveScheduleColumn(
            columnsMap,
            ["Send Week", "Course Week", "Week"],
        )
        sendDayColumn = _resolveScheduleColumn(
            columnsMap,
            ["Send Day", "Weekday", "Day"],
        )

        ## Step 3: Require minimum columns for schedule processing.
        if not titleColumn or not sendWeekColumn or not sendDayColumn:
            localSetup.logger.warning(
                f"{functionName}: Missing required columns in schedule file {schedulePath}. "
                f"Expected title/week/day columns."
            )
            return pd.DataFrame()

        ## Step 4: Build normalized output columns.
        normalizedDf = pd.DataFrame()
        normalizedDf["Communication Title"] = scheduleDf[titleColumn].fillna("").astype(str).str.strip()
        normalizedDf["Send Week"] = scheduleDf[sendWeekColumn].fillna("").astype(str).str.strip()
        normalizedDf["Send Day"] = scheduleDf[sendDayColumn].fillna("").astype(str).str.strip()

        ## Optional enabled column support.
        enabledColumn = _resolveScheduleColumn(columnsMap, ["Enabled", "Active", "Send"])
        if enabledColumn:
            normalizedDf["Enabled"] = scheduleDf[enabledColumn]
        else:
            normalizedDf["Enabled"] = True

        ## Step 5: Remove blank title rows and reset index.
        normalizedDf = normalizedDf[normalizedDf["Communication Title"].str.strip() != ""].copy()
        normalizedDf.reset_index(drop=True, inplace=True)

        localSetup.logger.info(
            f"{functionName}: Loaded {len(normalizedDf)} schedule rows from {schedulePath}."
        )
        return normalizedDf

    except Exception as Error:
        errorHandler.sendError(functionName, Error)
        return pd.DataFrame()


def _parseWeekdayValue(rawWeekday):
    """Parse schedule weekday value to Python weekday integer.

    Args:
        rawWeekday: Weekday value from schedule (name or number).

    Returns:
        int or None: Weekday where Monday=0, or None if invalid.
    """
    ## Step 1: Normalize text for comparisons.
    weekdayText = _safeStr(rawWeekday)
    if not weekdayText:
        return None

    ## Step 2: Handle numeric day values directly.
    if weekdayText.isdigit():
        dayValue = int(weekdayText)
        return dayValue if 0 <= dayValue <= 6 else None

    ## Step 3: Handle day-name values.
    weekdayMap = {
        "monday": 0,
        "mon": 0,
        "tuesday": 1,
        "tue": 1,
        "tues": 1,
        "wednesday": 2,
        "wed": 2,
        "thursday": 3,
        "thu": 3,
        "thurs": 3,
        "friday": 4,
        "fri": 4,
        "saturday": 5,
        "sat": 5,
        "sunday": 6,
        "sun": 6,
    }
    return weekdayMap.get(weekdayText.lower())


def _resolveScheduledCourseWeek(sendWeekValue, courseFinalWeek):
    """Resolve schedule week expressions to a concrete course week number.

    Args:
        sendWeekValue: Schedule week token or number.
        courseFinalWeek: Course final week number.

    Returns:
        int or None: Target course week, or None if invalid.
    """
    ## Step 1: Handle direct numeric week values.
    weekText = _safeStr(sendWeekValue)
    if not weekText:
        return None
    if weekText.lstrip("-").isdigit():
        return int(weekText)

    ## Step 2: Handle relative week tokens.
    normalizedWeekToken = _normalizeToken(weekText)
    tokenToWeek = {
        "midterm_minus_1": int(courseFinalWeek / 2) - 1,
        "final_minus_1": int(courseFinalWeek) - 1,
        "final_plus_1": int(courseFinalWeek) + 1,
        "course_start": 0,
    }
    return tokenToWeek.get(normalizedWeekToken)


def _isScheduleRowEnabled(scheduleRow):
    """Determine if a schedule row is enabled.

    Args:
        scheduleRow (pd.Series): One schedule row.

    Returns:
        bool: True when row is enabled.
    """
    ## Step 1: Parse enabled value using resilient boolean helper.
    return _safeBool(scheduleRow.get("Enabled", True))


def _isScheduleRowDueForCourse(scheduleRow, courseRow, runWeekday):
    """Evaluate whether a schedule row should send for a course now.

    Args:
        scheduleRow (pd.Series): Schedule definition row.
        courseRow (pd.Series): Course data row with week metadata.
        runWeekday (int): Current weekday (Monday=0).

    Returns:
        bool: True when this row is due for this course.
    """
    ## Step 1: Skip disabled rows.
    if not _isScheduleRowEnabled(scheduleRow):
        return False

    ## Step 2: Parse required weekday from schedule.
    scheduledWeekday = _parseWeekdayValue(scheduleRow.get("Send Day"))
    if scheduledWeekday is None:
        return False

    ## Step 3: Resolve scheduled target week for the course.
    courseWeek = int(courseRow.get("Course Week", -999))
    courseFinalWeek = int(courseRow.get("Course Final Week", 0))
    scheduledCourseWeek = _resolveScheduledCourseWeek(scheduleRow.get("Send Week"), courseFinalWeek)
    if scheduledCourseWeek is None:
        return False

    ## Step 4: Return due status based on week/day match.
    return courseWeek == scheduledCourseWeek and runWeekday == scheduledWeekday


## Send department communication emails to instructors of active outcome-associated courses
def sendDepartmentCommunication(inputTerm, targetDesignator, enforceOptIn=False):
    functionName = "Send Department Communication"

    try:
        ## Normalize and validate input parameters
        inputTerm = _normalizeTermCode(inputTerm)
        targetDesignator = _safeStr(targetDesignator).upper()
        ## Retrieve active outcome-associated courses and auxiliary data
        completeActiveCanvasCoursesDF, auxillaryDFDict = retrieveDataForRelevantCommunication(
            p1_localSetup=localSetup,
            p1_errorHandler=errorHandler,
            p2_inputTerm=inputTerm,
            p3_targetDesignator=targetDesignator,
        )

        activeOutcomeCoursesDf = auxillaryDFDict.get("Active Outcome Courses DF", pd.DataFrame())
        if activeOutcomeCoursesDf.empty:
            localSetup.logger.info(
                f"{functionName}: No active outcome-associated courses found for {inputTerm} / {targetDesignator}."
            )
            ## Return early if no courses found
            return {
                "status": "no_active_outcome_courses",
                "sent": 0,
                "skipped": 0,
                "targetDesignator": targetDesignator,
                "inputTerm": inputTerm,
            }

        ## Retrieve settings for the target designator and matching communication files
        settings = getDesignatorSettingsDict(localSetup, targetDesignator)
        communicationTemplatePaths = getDesignatorFilesByType(localSetup, targetDesignator, "Communication")
        if not communicationTemplatePaths:
            localSetup.logger.warning(
                f"{functionName}: No communication files found for {targetDesignator}."
            )
            return {
                "status": "no_communication_files",
                "sent": 0,
                "skipped": 0,
                "targetDesignator": targetDesignator,
                "inputTerm": inputTerm,
            }

        courseLevel = _safeStr(settings.get("Course Level")) or "All"
        courseTermPrefixes = _buildCourseTermPrefixes(inputTerm, courseLevel)
        sharedMailbox = _safeStr(settings.get("Client Send/Recieve Email"))
        isOptedIn = _safeBool(settings.get("Outcome Communication Opt In"))

        ## Check if designator is opted in for communications
        if enforceOptIn and not isOptedIn:
            localSetup.logger.info(
                f"{functionName}: {targetDesignator} is not opted in for communication. Skipping send."
            )
            return {
                "status": "skipped_not_opted_in",
                "sent": 0,
                "skipped": 0,
                "targetDesignator": targetDesignator,
                "inputTerm": inputTerm,
            }

        ## Filter courses by term prefixes
        filteredCoursesDf = activeOutcomeCoursesDf[
            activeOutcomeCoursesDf["Course_sis_id"]
            .astype(str)
            .str.startswith(tuple(courseTermPrefixes))
        ].copy()

        if filteredCoursesDf.empty:
            localSetup.logger.info(
                f"{functionName}: No courses matched term prefixes {courseTermPrefixes} "
                f"for {inputTerm} / {targetDesignator}."
            )
            ## Return early if no matching courses found
            return {
                "status": "no_matching_courses",
                "sent": 0,
                "skipped": 0,
                "targetDesignator": targetDesignator,
                "inputTerm": inputTerm,
            }

        ## Remove duplicate courses, keeping the first occurrence
        filteredCoursesDf = filteredCoursesDf.drop_duplicates(subset=["Course_sis_id"], keep="first")

        ## Merge in course week metadata from complete active canvas courses for date-based scheduling.
        if not completeActiveCanvasCoursesDF.empty:
            courseWeekFields = ["course_id", "Course Week", "Course Final Week"]
            availableCourseWeekFields = [
                fieldName
                for fieldName in courseWeekFields
                if fieldName in completeActiveCanvasCoursesDF.columns
            ]
            if len(availableCourseWeekFields) == len(courseWeekFields):
                courseWeekDf = completeActiveCanvasCoursesDF[courseWeekFields].drop_duplicates(
                    subset=["course_id"],
                    keep="first",
                )
                filteredCoursesDf = filteredCoursesDf.merge(
                    courseWeekDf,
                    left_on="Course_sis_id",
                    right_on="course_id",
                    how="left",
                )

        ## Build template lookup keyed by normalized communication title.
        templatePathByTitle = {}
        for templatePath in communicationTemplatePaths:
            templateSubject = _getEmailSubjectFromTemplatePath(templatePath, "")
            normalizedTitle = _normalizeToken(templateSubject)
            if normalizedTitle:
                templatePathByTitle[normalizedTitle] = templatePath

        ## Load optional schedule CSV. If absent, retain legacy behavior by sending all templates.
        scheduleDf = _loadDepartmentCommunicationScheduleDf(targetDesignator)
        scheduleEnabled = not scheduleDf.empty
        runWeekday = localSetup.initialDateTime.weekday()

        sentCount = 0
        skippedCount = 0

        ## Iterate through each course and send communications to instructors
        for _, courseRow in filteredCoursesDf.iterrows():
            courseName = _safeStr(courseRow.get("Course_name")) or _safeStr(courseRow.get("Course_sis_id"))
            instructorNames, instructorEmails = _collectInstructorData(courseRow)

            ## Skip course if no instructor email addresses found
            if not instructorEmails:
                skippedCount += 1
                localSetup.logger.warning(
                    f"{functionName}: No instructor email found for course {courseName}. Skipping."
                )
                continue

            recipients = ", ".join(instructorEmails)

            ## Determine which communication templates are due for this course.
            templatePathsToSend = []
            if scheduleEnabled:
                for _, scheduleRow in scheduleDf.iterrows():
                    scheduleTitle = _safeStr(scheduleRow.get("Communication Title"))
                    normalizedScheduleTitle = _normalizeToken(scheduleTitle)

                    if not normalizedScheduleTitle:
                        continue
                    if normalizedScheduleTitle not in templatePathByTitle:
                        localSetup.logger.warning(
                            f"{functionName}: Schedule title '{scheduleTitle}' did not match a communication template for {targetDesignator}."
                        )
                        continue
                    if _isScheduleRowDueForCourse(scheduleRow, courseRow, runWeekday):
                        templatePathsToSend.append(templatePathByTitle[normalizedScheduleTitle])
            else:
                templatePathsToSend = communicationTemplatePaths.copy()

            ## Remove duplicates while preserving send order.
            templatePathsToSend = list(dict.fromkeys(templatePathsToSend))

            ## Skip course when no communications are due.
            if not templatePathsToSend:
                continue

            ## Send each due communication file to the course instructors.
            for templatePath in templatePathsToSend:
                try:
                    templateHtml = loadDepartmentCommunicationTemplate(templatePath)
                    emailBody = _buildEmailBody(
                        templateHtml,
                        instructorNames,
                        courseName,
                        _safeStr(settings.get("Client Email Signature")),
                    )
                    emailSubject = _getEmailSubjectFromTemplatePath(templatePath, courseName)

                    sendOutlookEmail(
                        p1_subject=emailSubject,
                        p1_body=emailBody,
                        p1_recipientEmailList=recipients,
                        p1_shared_mailbox=sharedMailbox,
                    )
                except Exception as Error:
                    skippedCount += 1
                    localSetup.logger.error(
                        f"{functionName}: Failed to send communication file {templatePath} for {courseName} to {recipients}. Error: {Error}"
                    )
                    continue

                sentCount += 1
                localSetup.logger.info(
                    f"{functionName}: Sent communication file {templatePath} for {courseName} to {recipients}."
                )

        ## Log final results
        localSetup.logger.info(
            f"{functionName}: Complete. Sent={sentCount}, Skipped={skippedCount}, "
            f"TargetDesignator={targetDesignator}, InputTerm={inputTerm}, "
            f"CanvasCoursesLoaded={len(completeActiveCanvasCoursesDF)}"
        )
        ## Return success summary
        return {
            "status": "completed",
            "sent": sentCount,
            "skipped": skippedCount,
            "targetDesignator": targetDesignator,
            "inputTerm": inputTerm,
        }

    except Exception as Error:
        errorHandler.sendError(functionName, Error)
        ## Return error summary
        return {
            "status": "error",
            "sent": 0,
            "skipped": 0,
            "targetDesignator": targetDesignator,
            "inputTerm": inputTerm,
            "error": str(Error),
        }


## Main execution: prompt for input and call the send communication function
if __name__ == "__main__":
    sendDepartmentCommunication(
        inputTerm=input("Enter the desired term in four character format (FA26, SU26, SP26): ").strip().upper(),
        targetDesignator=input("Enter the desired target designator (GE, I-EDUC, U-ENGR): ").strip().upper(),
    )
    input("Press enter to exit")