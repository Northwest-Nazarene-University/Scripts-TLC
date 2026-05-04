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

            ## Send each matching communication file to the course instructors
            for templatePath in communicationTemplatePaths:
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