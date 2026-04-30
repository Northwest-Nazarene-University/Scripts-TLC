## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Copilot

## Import Generic Modules
import os
import sys
from datetime import datetime
import pandas as pd

## Ensure ResourceModules are importable when running from ActionModules
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

try:  ## Irregular try clause, do not comment out in testing
    from Local_Setup import LocalSetup
    from Error_Email import errorEmail
    from TLC_Action import retrieveDataForRelevantCommunication
    from Core_Microsoft_Api import sendOutlookEmail
    from Canvas_Report import CanvasReport
except ImportError:
    from ResourceModules.Local_Setup import LocalSetup
    from ResourceModules.Error_Email import errorEmail
    from ResourceModules.TLC_Action import retrieveDataForRelevantCommunication
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

localSetup = LocalSetup(datetime.now(), __file__)
errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)

SURVEY_URL = "https://nnu.co1.qualtrics.com/jfe/form/SV_b43fEMvdoOw6WeG"


def _safe_str(value):
    if pd.isna(value):
        return ""
    return str(value).strip()


def _candidate_template_paths(targetDesignator):
    baseName = f"{targetDesignator}_department_communication"
    fileNames = [baseName, f"{baseName}.txt"]

    sisPath = localSetup.getExternalResourcePath("SIS")
    tlcPath = localSetup.getExternalResourcePath("TLC")
    repoPath = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    searchDirs = [
        os.path.join(sisPath, "Internal Tool Files") if sisPath else "",
        sisPath or "",
        os.path.join(tlcPath, "Internal Tool Files") if tlcPath else "",
        tlcPath or "",
        os.path.dirname(__file__),
        repoPath,
    ]

    paths = []
    for folder in searchDirs:
        if not folder:
            continue
        for fileName in fileNames:
            paths.append(os.path.join(folder, fileName))
    return paths


def loadDepartmentCommunicationTemplate(targetDesignator):
    functionName = "Load Department Communication Template"

    try:
        for candidatePath in _candidate_template_paths(targetDesignator):
            if os.path.exists(candidatePath):
                with open(candidatePath, "r", encoding="utf-8") as templateFile:
                    localSetup.logger.info(f"{functionName}: Loaded template from {candidatePath}")
                    return templateFile.read()
    except Exception as Error:
        errorHandler.sendError(functionName, Error)
        raise

    raise FileNotFoundError(
        f"No template file found for {targetDesignator}. "
        f"Expected a file named {targetDesignator}_department_communication or "
        f"{targetDesignator}_department_communication.txt in configured resource paths."
    )


def _build_course_term_prefixes(inputTerm, courseLevel):
    undgPrefix = f"{inputTerm[:2]}{localSetup.dateDict['century']}{inputTerm[2:]}"
    gradTerm = CanvasReport.determineGradTerm(inputTerm)
    gradPrefix = f"{gradTerm[:2]}{localSetup.dateDict['century']}{gradTerm[2:]}"

    if courseLevel == "Undergraduate":
        return [undgPrefix]
    if courseLevel == "Graduate":
        return [gradPrefix]
    if undgPrefix == gradPrefix:
        return [undgPrefix]
    return [undgPrefix, gradPrefix]


def _get_designator_settings(targetDesignator):
    sisPath = localSetup.getExternalResourcePath("SIS")
    toolPath = os.path.join(sisPath, "Internal Tool Files", "Automated Outcome Tool Variables.xlsx")
    toolDf = pd.read_excel(toolPath)
    matchDf = toolDf[toolDf["Target Designator"] == targetDesignator]
    if matchDf.empty:
        return {}
    return matchDf.iloc[0].to_dict()


def _collect_instructor_data(courseRow):
    instructorNames = []
    instructorEmails = []

    for columnName in courseRow.index:
        if "Instructor_#" in columnName and "_name" in columnName:
            nameValue = _safe_str(courseRow[columnName])
            if nameValue:
                instructorNames.append(nameValue)
        elif "Instructor_#" in columnName and "_email" in columnName:
            emailValue = _safe_str(courseRow[columnName])
            if emailValue:
                instructorEmails.append(emailValue)

    uniqueNames = list(dict.fromkeys(instructorNames))
    uniqueEmails = list(dict.fromkeys(instructorEmails))
    return uniqueNames, uniqueEmails


def _format_instructor_name(instructorNames):
    if not instructorNames:
        return "Instructor"
    if len(instructorNames) == 1:
        return instructorNames[0]
    return ", ".join(instructorNames[:-1]) + f", and {instructorNames[-1]}"


def _build_email_body(templateHtml, instructorNames, courseName):
    surveyLinkHtml = f"<a href='{SURVEY_URL}' target='_blank'>{SURVEY_URL}</a>"
    instructorNameText = _format_instructor_name(instructorNames)
    emailBody = templateHtml
    emailBody = emailBody.replace("[Instructor Name]", instructorNameText)
    emailBody = emailBody.replace("[Course Name]", courseName)
    emailBody = emailBody.replace("[Survey Link]", surveyLinkHtml)
    emailBody = emailBody.replace(
        "{hyperlinked to https://nnu.co1.qualtrics.com/jfe/form/SV_b43fEMvdoOw6WeG}",
        "",
    )
    return emailBody


def sendDepartmentCommunication(inputTerm, targetDesignator):
    functionName = "Send Department Communication"

    try:
        templateHtml = loadDepartmentCommunicationTemplate(targetDesignator)
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
            return

        settings = _get_designator_settings(targetDesignator)
        courseLevel = _safe_str(settings.get("Course Level")) or "All"
        courseTermPrefixes = _build_course_term_prefixes(inputTerm, courseLevel)
        sharedMailbox = _safe_str(settings.get("Client Send/Recieve Email"))

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
            return

        filteredCoursesDf = filteredCoursesDf.drop_duplicates(subset=["Course_sis_id"], keep="first")

        sentCount = 0
        skippedCount = 0

        for _, courseRow in filteredCoursesDf.iterrows():
            courseName = _safe_str(courseRow.get("Course_name")) or _safe_str(courseRow.get("Course_sis_id"))
            instructorNames, instructorEmails = _collect_instructor_data(courseRow)

            if not instructorEmails:
                skippedCount += 1
                localSetup.logger.warning(
                    f"{functionName}: No instructor email found for course {courseName}. Skipping."
                )
                continue

            emailBody = _build_email_body(templateHtml, instructorNames, courseName)
            emailSubject = f"{courseName}: UCTC Course Assessment Survey"
            recipients = ", ".join(instructorEmails)

            sendOutlookEmail(
                p1_subject=emailSubject,
                p1_body=emailBody,
                p1_recipientEmailList=recipients,
                p1_shared_mailbox=sharedMailbox,
            )

            sentCount += 1
            localSetup.logger.info(
                f"{functionName}: Sent communication for {courseName} to {recipients}."
            )

        localSetup.logger.info(
            f"{functionName}: Complete. Sent={sentCount}, Skipped={skippedCount}, "
            f"TargetDesignator={targetDesignator}, InputTerm={inputTerm}, "
            f"CanvasCoursesLoaded={len(completeActiveCanvasCoursesDF)}"
        )

    except Exception as Error:
        errorHandler.sendError(functionName, Error)


if __name__ == "__main__":
    sendDepartmentCommunication(
        inputTerm=input("Enter the desired term in four character format (FA26, SU26, SP26): ").strip().upper(),
        targetDesignator=input("Enter the desired target designator (GE, I-EDUC, U-ENGR): ").strip().upper(),
    )
    input("Press enter to exit")
