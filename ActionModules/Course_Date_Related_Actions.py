## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller.

## Import Generic Moduels

import os, sys, threading, asyncio
import pandas as pd
from datetime import datetime

# Ensure ResourceModules are importable when running from ActionModules
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

try: ## Irregular try clause, do not comment out in testing
    from Local_Setup import LocalSetup
    from Canvas_Report import CanvasReport
    from Core_Microsoft_Api import sendOutlookEmail, CoreMicrosoftAPI
    from Error_Email import errorEmail
    from TLC_Common import isPresent, isMissing
    from TLC_Action import (
        retrieveDataForRelevantCommunication,
        getUniqueOutcomesAndOutcomeCoursesDict,
        removeMissingOutcomes,
        addOutcomeToCourse,
    )
except ImportError:
    # Fallback to relative imports if package layout differs
    from ResourceModules.Local_Setup import LocalSetup
    from ResourceModules.Canvas_Report import CanvasReport
    from ResourceModules.Core_Microsoft_Api import sendOutlookEmail
    from ResourceModules.Error_Email import errorEmail
    from ResourceModules.TLC_Common import isPresent, isMissing
    from ResourceModules.TLC_Action import (
            retrieveDataForRelevantCommunication,
            getUniqueOutcomesAndOutcomeCoursesDict,
            removeMissingOutcomes,
            addOutcomeToCourse,
        )

## Set working directory
os.chdir(os.path.dirname(__file__))

# Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = __file__

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
errorHandler = errorEmail(scriptName if 'scriptName' in globals() else 'Course_Date_Related_Actions', scriptPurpose, externalRequirements, localSetup)

## This Function creates a formated Mising Outcome Attachment Email Body
def createOutcomeEmailBody (p3_relevantEmail
                            , p4_inputTerm
                            , p1_instructorNameOrNames
                            , p1_course
                            , p1_relevantAuthority
                            , p1_outcome
                            , p1_emailDetails
                            ):
    
    functionName = "createErrorEmailBody"

    ## Define the email body dictionary
    emailBodyDict = {}        
    
    ## Define a variable for the singular or plural word dict according to whether
    singularOrPluralDict = {}
    
    ## If </li> appears more than once in the outcome string
    if p1_outcome.count("</li>") > 1:
        
        ## Assign the plural word dict
        singularOrPluralDict = {"is/are" : "are"
                                , "has/have" : "have"
                                , "this/these" : "these"
                                , "a/" : ""
                                , "outcome/outcomes" : "outcomes"
                                , "designatorSpecificTerm" : "habits" if "GE" in p1_outcome else "outcomes"
                                , "rubric/rubrics" : "rubrics"
                                , "assignment/assignments" : "assignments"
                                }
        
    ## Otherwise
    else:
        
        ## Assign the singular word dict
        singularOrPluralDict = {"is/are" : "is"
                                , "has/have" : "has"
                                , "this/these" : "this"
                                , "a/" : " a"
                                , "outcome/outcomes" : "outcome"
                                , "designatorSpecificTerm" : "habit" if "GE" in p1_outcome else "outcome"
                                , "rubric/rubrics" : "rubric"
                                , "assignment/assignments" : "assignment"
                                }
        
    ## If there is more than one instructor, designated by whether there is is a comma in the instructor name string
    if ',' in p1_instructorNameOrNames:
        
        ## Assign the neccessary plural Professor/Professors string
        singularOrPluralDict["Professor/Professors"] = "Professors"
        
        ## Assign the neccessary plural instructor/instructors string
        singularOrPluralDict["an instructor/instructors"] = "instructors"
        
    ## Otherwise
    else:
        
        ## Assign the neccessary singular Professor/Professors string
        singularOrPluralDict["Professor/Professors"] = "Professor"
        
        ## Assign the neccessary singular an instructor/instructors string
        singularOrPluralDict["an instructor/instructors"] = "an instructor"

    ## Set the emailbodysignature to Client Email Signature
    emailBodyDict["signature"] = p1_emailDetails["Client Email Signature"]
        
    ## Define the action
    emailBodyDict["bulletted resource list"]  = """<li><a href='https://community.canvaslms.com/t5/Instructor-Guide/How-do-I-align-an-outcome-with-a-rubric-in-a-course-using/ta-p/609340' target='_blank'>Attaching an outcome to a rubric</a></li>
    <li><a href='https://community.canvaslms.com/t5/Instructor-Guide/How-do-I-add-a-rubric-to-an-assignment/ta-p/1058#open_assignment' target='_blank'>Attaching a rubric to an assignment</a></li>
    <li><a href='https://community.canvaslms.com/t5/Instructor-Guide/How-do-I-add-a-rubric-to-a-graded-discussion/ta-p/1062#open_discussion' target='_blank'>Attaching a rubric to a graded discussion</a></li>
    <li><a href='https://community.canvaslms.com/t5/Instructor-Guide/How-do-I-add-a-rubric-to-a-quiz/ta-p/1009#open_quiz' target='_blank'>Attaching a rubric to a classic quiz</a></li>
    <li><a href='https://community.canvaslms.com/t5/Instructor-Guide/How-do-I-align-an-outcome-to-a-quiz-in-New-Quizzes/ta-p/776#open-assessment' target='_blank'>Attaching an outcome to a new quiz</a></li>
    <li><a href='https://community.canvaslms.com/t5/Instructor-Guide/How-do-I-align-an-outcome-to-a-quiz-question-in-New-Quizzes/ta-p/778#edit-quiz' target='_blank'>Attaching an outcome to a new quiz question</a></li>
    <li><a href='https://community.canvaslms.com/t5/Instructor-Guide/How-do-I-use-a-rubric-to-grade-submissions-in-SpeedGrader/ta-p/1015#open_student_submission' target='_blank'>Using a rubric to grade submissions in SpeedGrader</a></li>
    <li><a href='https://community.canvaslms.com/t5/Instructor-Guide/How-do-I-use-a-non-scoring-rubric-to-assess-submissions-in/ta-p/989' target='_blank'>Using a non-scoring rubric to assess submissions in SpeedGrader</a></li>"""

    ## If the relevant email is a course start email
    if "Course Start" in p3_relevantEmail:
            
        ## Assign the future or current instructor dynamic string
        emailBodyDict["future/current instructor"] = "are scheduled to be"
            
        ## Assign the course start dynamic cause string
        emailBodyDict["dynamic cause"] = f"which has the following {singularOrPluralDict['outcome/outcomes']} associated with it:"
            
        ## Assign the course start reminder to attach outcomes to published assignments string
        emailBodyDict["timeOfYearReminder"] = f"""As we begin the term, please ensure that the {singularOrPluralDict['designatorSpecificTerm']} langauge is included in your courses's syllabus.{p1_emailDetails['Dept Specific Wording']}</p>
        <p> Additionally, please consider how you will perform your outcome assessment, particularly which course assignment or assignments you will attach the {singularOrPluralDict['outcome/outcomes']} to."""
        
    ## If the relevant email is a reminder
    elif  "Reminder" in p3_relevantEmail:
            
        ## Assign the future or current instructor dynamic string
        emailBodyDict["future/current instructor"] = "are"
            
        ## Assign the reminder dynamic cause string
        emailBodyDict["dynamic cause"] = f"where it appears that the following {singularOrPluralDict['is/are']} not attached to a published assignment:"
    
        ## If it is a midterm reminder
        if "Midterm" in p3_relevantEmail:
        
            ## Assign the midterm reminder to attach outcomes to published assignments string
            emailBodyDict["timeOfYearReminder"] = f"""As we proceed through midterm week for your course, please consider how you will perform your outcome assessment, and make sure that you have the most recent version of your {singularOrPluralDict['outcome/outcomes']} attached to an assignment rubric."""

        ## If it is a finals reminder
        elif "Finals" in p3_relevantEmail:
        
            ## Assign the finals reminder to attach outcomes to published assignments string
            emailBodyDict["timeOfYearReminder"] = f"""As finals week has arrived, please make sure that you have the most recent version of the {singularOrPluralDict['outcome/outcomes']} attached to at least one rubric and that the associated {singularOrPluralDict['rubric/rubrics']} are attached to {singularOrPluralDict['a/']}published {singularOrPluralDict['assignment/assignments']}."""

    elif "Missing" in p3_relevantEmail:
            
        ## Assign the future or current instructor dynamic string
        emailBodyDict["future/current instructor"] = "were"

        ## Assign the alert that there outcome data missing dynamic cause string
        emailBodyDict["dynamic cause"] = f"where it appears that less than 75% of the students have been scored for the following {singularOrPluralDict['outcome/outcomes']}:"
        
        ## Assign the missing data alert string
        emailBodyDict["timeOfYearReminder"] = """For outcome data to be recorded, an additional grading step is required for each student that submitted to an assignment with an outcome rubric attached."""
        
    emailBodyDict["formatedEmaiBody"] = f"""<!DOCTYPE html>
<html>
<body>
<p>Hello {singularOrPluralDict["Professor/Professors"]} {p1_instructorNameOrNames},<br></p>
    
<p>You are receiving this email because you {emailBodyDict["future/current instructor"]} {singularOrPluralDict["an instructor/instructors"]} of the NNU outcome course {p1_course}, {emailBodyDict["dynamic cause"]}</p>
    
<ul>{p1_outcome}</ul>
    
<p>{emailBodyDict["timeOfYearReminder"]}<br></p>
    
<p>If you would like a refresher on how to do this, please identify your interest below:</p>
    
<ul>{emailBodyDict["bulletted resource list"]}</ul>
    
<p>{p1_emailDetails['User Contact Name'] or p1_emailDetails['Client Contact Name']} can be reached at <a href='mailto:{p1_emailDetails["User Contact Email"] or p1_emailDetails['Client Send/Recieve Email']}'>{p1_emailDetails["User Contact Email"] or p1_emailDetails['Client Send/Recieve Email']}</a> and is a good resource for how to assess your associated outcomes in your field of study. Additionally, NNU's Teaching and Learning Center at <a href='mailto:tlc@nnu.edu'>tlc@nnu.edu</a> is always ready to provide ideas, best practice tips, and assistance with creating and assessing outcomes.<br></p>

    {emailBodyDict["signature"]}
"""
        
    return emailBodyDict["formatedEmaiBody"]

## This function crafts and sends the relevant outcome email
def craftAndSendRelevantEmail(
        p3_inputTerm,
        p2_relevantEmail,
        p2_row,
        p1_auxiliaryDfDict,
    ):
    
    functionName = "craftAndSendRelevantEmail"
    
    try:
        ## Define baseExternalInputPath in function scope
        baseExternalInputPath = localSetup.getExternalResourcePath("SIS") or localSetup.configPath

        ## Retrieve the Automated Outcome Tool Variables excel file as a df    
        automatedOutcomeToolVariablesDf = pd.read_excel(os.path.join(baseExternalInputPath, "Internal Tool Files", "Automated Outcome Tool Variables.xlsx"))
        
        ## Filter the automated outcome tool variables df to only the row with the relevant outcome area
        automatedOutcomeToolVariablesDict = automatedOutcomeToolVariablesDf[
            automatedOutcomeToolVariablesDf["Target Designator"] == p2_row["Outcome Area"]
            ].iloc[0].to_dict()

        ## If the Outcome Communication Opt In is false for the relevant outcome area
        if not automatedOutcomeToolVariablesDict["Outcome Communication Opt In"]:

            ## Return from the function without sending an email
            return
        
        ## Make a filtered Unassessed Outcome Courses DF that only includes the course that the email is being sent about
        filteredUnassessedOutcomeCoursesDF = p1_auxiliaryDfDict["Unassessed Outcome Courses DF"][
            p1_auxiliaryDfDict["Unassessed Outcome Courses DF"]["Course_name"] == p2_row["Course_name"]
            ]

        ## Create an email details dictionary
        emailDetails = {"Client Name" : automatedOutcomeToolVariablesDict["Client Name"]
                        , "Client Contact Name" : automatedOutcomeToolVariablesDict["Client Contact Name"]
                        , "Client Send/Recieve Email" : automatedOutcomeToolVariablesDict["Client Send/Recieve Email"]
                        , "Client Email Signature" : f"""{automatedOutcomeToolVariablesDict["Client Email Signature"]}"""
                        , "User Contact Name" : (automatedOutcomeToolVariablesDict["User Contact Name"])
                        , "User Contact Email" :automatedOutcomeToolVariablesDict["User Contact Email"]
                        , "Input Term": p3_inputTerm
                        , "Course Name": p2_row["Course_name"]
                        , "Relevant Email": p2_relevantEmail
                        , "Outcome Email Subject": f"{p2_row['Course_name']} {p2_relevantEmail}"
                        , "Dept Specific Wording": (
                            " " + str(automatedOutcomeToolVariablesDict["Dept Specific Wording"])
                            if isPresent(automatedOutcomeToolVariablesDict["Dept Specific Wording"]) 
                            else ""
                            )
                        }


        ## If the relevant email is an outcome email
        if "Outcome" in p2_relevantEmail:

            ## Add the outcome area to the email details
            emailDetails["Outcome Area"] = p2_row["Outcome Area"]
    
            ## Iterate through the p2_rows datapoints to find the instructor and outcome information
            for key, datapoint in p2_row.items():
        
                ## If the datapoint is teacher related
                if "Instructor" in key:
        
                    ## If the datapoint is a teacher name
                    if "name" in key and isPresent(datapoint):
                
                        ## If there is already a name in instructorNameOrNamesString
                        if "Instructor Name Or Names String" in emailDetails.keys():

                            ## Seperate the last name from the datapoint
                            lastName = datapoint.split(" ")[-1]
                
                            ## Add a comma and space and then the additional name
                            emailDetails["Instructor Name Or Names String"] += f", {lastName}"
                        
                        ## Otherwise
                        else:

                            ## Seperate the last name from the datapoint
                            lastName = datapoint.split(" ")[-1]
                        
                            ## Change the instructor name string to the datapoint name
                            emailDetails["Instructor Name Or Names String"] = lastName

                    ## If the datapoint is a teacher email
                    elif "email" in key and isPresent(datapoint):

                        ## If the key does not already exist in the email details
                        if "Instructor Email Or Emails String" not in emailDetails.keys():
                            
                            ## Add the teacher email to the list of instructor emails
                            emailDetails["Instructor Email Or Emails String"] = f"{datapoint}"
                        
                        ## Otherwise
                        else:

                            ## Add the teacher email to the list of instructor emails
                            emailDetails["Instructor Email Or Emails String"] += f", {datapoint}"


                ## If the datapoint is an outcome
                elif "Outcome" in key and isPresent(datapoint) and key != "Outcome Area":
                    
                    ## If the email is a missing required data email
                    if "Missing" in p2_relevantEmail:
                        
                        ## If the data point is not amoung a list of the unique outcome titles in the filteredUnassessedOutcomeCoursesDF
                        if datapoint not in filteredUnassessedOutcomeCoursesDF["Outcome_Title"].values:
                            
                            ## Skip the datapoint
                            continue
                        
                    ## If the email is a Reminder email (Midterm or Finals)
                    elif "Reminder" in p2_relevantEmail:

                        ## Build a filtered df of outcomes missing attachments for this course
                        filteredWithoutAttachmentsDF = p1_auxiliaryDfDict["Outcome Courses Without Attachments DF"][
                            p1_auxiliaryDfDict["Outcome Courses Without Attachments DF"]["Course_name"] == p2_row["Course_name"]
                        ]

                        ## If the outcome is not in the missing-attachment report's Required Outcome column, skip it
                        if isMissing(filteredWithoutAttachmentsDF) or datapoint not in filteredWithoutAttachmentsDF["Required Outcome"].values:
                            continue

                    ## If the key does not already exist in the email details
                    if "Outcome Or Outcomes String" not in emailDetails.keys():
                        
                        ## Add the outcome to the list of outcomes as an li element
                        emailDetails["Outcome Or Outcomes String"] = f"<li>{datapoint}</li>"

                    ## Otherwise
                    else:
            
                        ## Add the outcome to the list of outcomes as an li element
                        emailDetails["Outcome Or Outcomes String"] += f"<li>{datapoint}</li>"

            ## Create the formated email contents
            emailDetails['Outcome Email Body'] = createOutcomeEmailBody(p3_relevantEmail = p2_relevantEmail
                                                          , p4_inputTerm = p3_inputTerm
                                                          , p1_instructorNameOrNames = emailDetails["Instructor Name Or Names String"]
                                                          , p1_course = p2_row["Course_name"]
                                                          , p1_outcome = emailDetails["Outcome Or Outcomes String"]
                                                          , p1_relevantAuthority = emailDetails["Client Name"]
                                                          , p1_emailDetails = emailDetails
                                                          )

            ## Send the Outcome Email
            sendOutlookEmail(p1_subject = emailDetails['Relevant Email']
                             , p1_body = emailDetails['Outcome Email Body']
                             , p1_recipientEmailList = emailDetails['Instructor Email Or Emails String']
                             , p1_shared_mailbox = emailDetails['Client Send/Recieve Email']
                             )
            ## info log the test
            localSetup.logger.info(f"Crafted and sent email with subject: {emailDetails['Relevant Email']} to {emailDetails['Instructor Email Or Emails String']} with body: {emailDetails['Outcome Email Body']}")

    except Exception as Error:
        errorHandler.sendError(functionName, Error)

## This function determines what course date related actions need to be taken for a specific term and performs them
def termDetermineAndPerformRelevantActions (p1_inputTerm
                                            , p1_targetDesignator
                                            ):
    functionName = "Term Determine And Send Relevant Communication"

    try:

        ## Retrieve the data for determining and sending out relevant communication
        completeActiveCanvasCoursesDF, auxiliaryDfDict = retrieveDataForRelevantCommunication(
            p1_localSetup = localSetup
            , p1_errorHandler = errorHandler
            , p2_inputTerm = p1_inputTerm
            , p3_targetDesignator = p1_targetDesignator
            )
                
        ## Define a list to hold the communication threads
        actionThreads = []
        
        ## For each row in the complete active canvas courses df
        for index, row in completeActiveCanvasCoursesDF.iterrows():
            
            ## If ENGR4250 in row long_name
            #if "WELL1000" in row["long_name"]:

                ## Define a variable to track whether the course is an outcome course
                isOutcomeCourse = True if row["long_name"] in auxiliaryDfDict["Active Outcome Courses DF"]["Course_name"].values else False

                ## Define a relevant auxillary DF dict with empty dataframes
                relevantAuxillaryDfDict = {}
            
                ## If the course is in the list of courses who do not have their outcome attached to a published assignment
                if "Outcome Courses Without Attachments DF" in auxiliaryDfDict and isPresent(auxiliaryDfDict["Outcome Courses Without Attachments DF"]):
                    
                    ## Isolate the course's data in p1_outcomeCoursesWithoutAttachmentDF
                    relevantAuxillaryDfDict["Relevant Course Outcome Without Attachment Df"] = (
                        auxiliaryDfDict["Outcome Courses Without Attachments DF"][
                            auxiliaryDfDict["Outcome Courses Without Attachments DF"]["Course_name"] == row["long_name"]
                            ]
                        )
                    
                ## Otherwise
                else:

                    ## Create an empty dataframe for the courses without attached outcomes
                    relevantAuxillaryDfDict["Relevant Course Outcome Without Attachment Df"] = pd.DataFrame()

                ## If the course is in the list of courses who have no outcome results
                if "Unassessed Outcome Courses DF" in auxiliaryDfDict and isPresent(auxiliaryDfDict["Unassessed Outcome Courses DF"]):
                    
                    ## Isolate the course's data in p1_outcomeCoursesWithoutOutcomeData
                    relevantAuxillaryDfDict["Relevant Course Outcome Without Data Df"] = (
                        auxiliaryDfDict["Unassessed Outcome Courses DF"][
                            auxiliaryDfDict["Unassessed Outcome Courses DF"]["Course_name"] == row["long_name"]
                            ]
                        )
                    
                ## Otherwise
                else:
                    
                    ## Create an empty dataframe for the courses without outcome data
                    relevantAuxillaryDfDict["Relevant Course Outcome Without Data Df"] = pd.DataFrame()
            
                ## Define a variable to track what email, if any, needs to be sent to the instructors of the course
                relevantEmailList = []
            
                ## If it is the monday before the courses's week 0 and it is an outcome course
                if (row['Course Week'] == 0
                    and isOutcomeCourse
                    ):                
                    
                        ## Use the pre-computed unique outcomes and outcome course dict
                        ## from retrieveDataForRelevantCommunication (already called above)
                        uniqueOutcomes = auxiliaryDfDict["Unique Outcomes"]
                        outcomeCourseDict = auxiliaryDfDict["Outcome Canvas Data Dict"]
                    
                        ## Start a thread to make sure the outcome has been added to the course
                        addOutcomeThread = threading.Thread(
                            target=addOutcomeToCourse
                            , args=(localSetup
                                    , errorHandler
                                    , row
                                    , auxiliaryDfDict
                                    )
                            )

                        ## Start the thread
                        addOutcomeThread.start()

                        ## Add the thread to the list of communication threads
                        actionThreads.append(addOutcomeThread)
        
                ## If it is the Monday of week 0
                if (row['Course Week'] == 0
                    and localSetup.initialDateTime.weekday() == 0
                      ):

                    ## If the course is an Outcome course
                    if isOutcomeCourse:

                        ## Send the courses's instructors the Course Start email
                        relevantEmailList.append("Associated Course Outcomes: Course Start Information")
                        
                ## Otherwise, if it is the Monday of the week before the course's midpoint (e.g. week 7 in a 16 week course)
                elif (row['Course Week'] == (int(row["Course Final Week"] / 2) - 1)
                      and localSetup.initialDateTime.weekday() == 0
                      ): ## Casting the result of courseLength / 2 to int rounds the number down        
            
                    ## If the course is an Outcome course
                    if isOutcomeCourse:

                        ## If the course is an outcome course that does not have all of its outcomes attached to published assignments
                        if isPresent(relevantAuxillaryDfDict.get("Relevant Course Outcome Without Attachment Df")):    

                            ## Send the courses's instructors the Midterm Reminder email
                            relevantEmailList.append("Associated Course Outcomes: Midterm Reminder")

                ## Otherwise, if it is the Monday of the week before its final week (e.g. week 15 in a 16 week course)
                elif (row['Course Week'] == (row["Course Final Week"] -1)
                      and localSetup.initialDateTime.weekday() == 0
                      ):

                    ## If the course is an Outcome course
                    if isOutcomeCourse:

                        ## If the course is an outcome course that does not have all of its outcomes attached to published assignments
                        if isPresent(relevantAuxillaryDfDict.get("Relevant Course Outcome Without Attachment Df")): 

                            ## Send the courses's instructors the Finals Reminder email
                            relevantEmailList.append("Associated Course Outcomes: Finals Reminder")

                ## Otherwise, if it is the wednesday of the week after finals (e.g. week 17 from the start of a 16 week course)
                elif (row['Course Week'] == (row["Course Final Week"] + 1)
                      and localSetup.initialDateTime.weekday() == 2
                      ):
            
                    ## If the course is an Outcome course
                    if isOutcomeCourse:

                        ## If the course is in the list of courses who do not have all of their outcome data
                        if isPresent(relevantAuxillaryDfDict.get("Relevant Course Outcome Without Data Df")):    

                            ## Send the courses's instructors the Missing Data email as the course's outcome data is past due
                            relevantEmailList.append("Associated Course Outcomes: Missing Required Data")

                ## For each determined relevant email
                for relevantEmail in relevantEmailList:
                
                    ## Define a target row variable
                    targetRow = None
                
                    ## If the relevant email contains the word "Outcome"
                    if "Outcome" in relevantEmail:
                    
                        ## Find the index of course in the active outcome courses df
                        courseIndex = auxiliaryDfDict["Active Outcome Courses DF"][
                            auxiliaryDfDict["Active Outcome Courses DF"]["Course_name"] == row["long_name"]
                            ].index[0]

                        ## Define the target row as the row in the active outcome courses df
                        targetRow = auxiliaryDfDict["Active Outcome Courses DF"].loc[courseIndex]
                    
                        
                    # testCraft = craftAndSendRelevantEmail(p1_inputTerm
                    #         , relevantEmail
                    #         , targetRow
                    #         , auxiliaryDfDict
                    #         )
                    
                    ## Create a thread to send the relevant outcome email
                    communicationThread = threading.Thread(
                        target=craftAndSendRelevantEmail
                        , args=(p1_inputTerm
                                , relevantEmail
                                , targetRow
                                , auxiliaryDfDict
                                )
                        )
                
                    ## Start the thread
                    communicationThread.start()
                
                    ## Add the thread to the list of communication threads
                    actionThreads.append(communicationThread)

        ## For each thread in the list of communication threads
        for thread in actionThreads:
            
            ## Wait for the thread to finish
            thread.join()

    except Exception as Error:
        errorHandler.sendError(functionName, Error)


## For testing
if __name__ == "__main__":
     
    # craftAndSendRelevantEmail (p2_relevantEmail= "Associated Course Outcomes: Missing Required Data"
    #                            ,p2_row = {'Term': 'GF25'
    #                                       , 'Outcome Area': 'G-EDUC'
    #                                       , 'Course_sis_id': 'GF2025_EDUC7575_1L'
    #                                       , 'Course_name': 'LEGAL/FIN ISSUES IN EDUCATION GF2025_EDUC7575_1L'
    #                                       , 'Account_id': 'Graduate Education'
    #                                       , 'Number_of_students': 7
    #                                       , 'Outcome 1': 'G-EDUC_CAEP: 1.3_1.0'
    #                                       , 'Outcome 2': 'G-EDUC_CAEP: 1.4_1.0'
    #                                       , 'Instructor_#1_ID': 63232
    #                                       , 'Instructor_#1_name': 'John Doe'
    #                                       , 'Instructor_#1_email': 'brycezmiller@nnu.edu'
    #                                       , 'Instructor_#2_name': 'John Doe'
    #                                       , 'Instructor_#2_email': 'brycezmiller@nnu.edu'
    #                                       },
    #                             p3_inputTerm="GF25")
    
    ## Get an input term and start the term outcome email function
    termDetermineAndPerformRelevantActions (
        p1_inputTerm = input("Enter the desired term in four character format (FA20, SU20, SP20): ")
        , p1_targetDesignator = input("Enter the desired target designator (GE, I-EDUC, U-ENGR): ")
        )

    input("Press enter to exit")