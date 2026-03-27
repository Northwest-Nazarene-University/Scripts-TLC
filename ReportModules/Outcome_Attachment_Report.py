## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

## External libraries
import os, sys, csv, json, os.path, shutil, threading
from datetime import datetime
import pandas as pd

## Add Script repository to syspath
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

## New resource modules
from Local_Setup import LocalSetup
from TLC_Common import makeApiCall, isFileRecent, isPresent
from Canvas_Report import CanvasReport
from Common_Configs import coreCanvasApiUrl, termSchoolYearLogic
from Error_Email import errorEmail

## Create the localsetup variable
localSetup = LocalSetup(datetime.now(), __file__)  ## sets cwd, paths, logs, date parts

from Common_Configs import undgTermsCodesToWordsDict, gradTermsCodesToWordsDict

## Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = os.path.basename(__file__).replace(".py", "")

scriptPurpose = r"""
The Course Addendum Checker Script was written by NNU's IDT department to check whether NNU's canavs courses have the static Syllabus Addendum link, make .csv lista of the courses that do not have the link, and store the .csv files under \Employees-Read Only\University Syllabi by college and department.
"""
externalRequirements = r"""
To function properly, this script requires that the static Syllabus Addendum link "https://my.nnu.edu/ics/syllabus_addendum.aspx" (which redirects to the current addendum) be placed in the Canvas Syllabus tab.
"""

## Setup the error handler
errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)


""" 
 This fuction saves the course ID and other identifiers of the course in question.
 The intended purpose of this function is to make a csv of missing syllabi made up of courses without
 a syllabus or with short syllabi (which generally indicate that the link wasn't named properly)
 with the ulimate goal that all syllabi are gathered because departments are able to find and 
 add/fix the syllabi in the log.
"""
def saveOutcomeAttachmentCourseInfo(saveLocation, fileName, p1_course_name, p1_requiredOutcome, issue, p1_instructor_name, p1_instructor_email, p2_newFileCreated):
    functionName = "saveOutcomeAttachmentCourseInfo"
    try:

        ## This function creates a csv file to record when a Outcome outcpome is missing
        ## Create a new csv for that context the first time that a department is missing the requirement
        if not p2_newFileCreated[0]:

            if not (os.path.exists(saveLocation)):
                os.makedirs(saveLocation, mode=0o777, exist_ok=False)
            with open (f"{saveLocation}{fileName}", "w", newline="") as csvFile_2:
                fieldnames = ["Course_name", "Required Outcome", "Issue", "Instructor Name", "Instructor Email"]
                csvWriter = csv.DictWriter(csvFile_2, fieldnames=fieldnames, delimiter = ',')
                csvWriter.writeheader()
                csvWriter.writerow({"Course_name": p1_course_name
                                    , "Required Outcome": p1_requiredOutcome
                                    , "Issue": issue
                                    , "Instructor Name": p1_instructor_name
                                    , "Instructor Email": p1_instructor_email})
            p2_newFileCreated[0] = True

        ## If it is the second time (or more) in the current run of the script the missing syllabi file is added onto
        else:
            with open (f"{saveLocation}{fileName}", "a", newline="") as csvFile_2:
                fieldnames = ["Course_name", "Required Outcome", "Issue", "Instructor Name", "Instructor Email"]
                csvWriter = csv.DictWriter(csvFile_2, fieldnames=fieldnames, delimiter = ',')
                csvWriter.writerow({"Course_name": p1_course_name
                                    , "Required Outcome": p1_requiredOutcome
                                    , "Issue": issue
                                    , "Instructor Name": p1_instructor_name
                                    , "Instructor Email": p1_instructor_email})

    except Exception as Error:
        errorHandler.sendError (functionName, Error)

## This function processes an assignment that an outcome is attached to to ensure that it is published
## and assigned to the primary course section
def assignmentIsPublishedCheck (p1_rubric_api_url, assignment_id):
    functionName = "Assignment Is Published Check"

    ## Isolate the api url through the sis ID (leaving out the rubric specific piece)
    assignmentApiUlr = p1_rubric_api_url[:57] + p1_rubric_api_url.split(':')[2].split('/')[0] + "/assignments/" + str(assignment_id)
    
    ## Define the api payload to include associations
    assignmentApiPayload = {"include": ["submission", "assignment_visibility"]}

    ## Make a variable to hold the course's rubric api object
    assignmentApiObject, _ = makeApiCall(localSetup, p1_apiUrl = assignmentApiUlr, p1_payload = assignmentApiPayload)

    ## Save the primary body of information retrieved by the API call
    assignmentApiText = assignmentApiObject.text
        
    ## Convert the json body of information into a Python Dictionary
    assignmentApiDict = json.loads(assignmentApiText)
    
    ## If the assignment is published and is visible to at least one student
    if assignmentApiDict["published"] and assignmentApiDict["assignment_visibility"]:

        ## Return True
        return True

    ## If the assignment is not published or is not visible to at least one student
    else:
        
        ## Return False
        return False

## This function processes a rubric with the desired outcome and returns true if the rubric is attached to a published assignment within the course
def rubricIsAttachedToAPublishedAssignmentCheck(p1_courseRubricApiUrl, p1_rubricId):
    functionName = "Rubric Is Attached To A Published Assignment Check"

    ## Define the rubric specific api url by replacing the per page piece with a / and the rubric's id
    ## on the all rubrics api url
    rubricApiUlr = p1_courseRubricApiUrl.replace("?per_page=100", "/" + str(p1_rubricId))

    ## Define the api payload to include associations
    rubricApiPayload = {"include": ["assessments", "graded_assessments", "assignment_associations"]}

    ## Make a variable to hold the course's rubric api object
    rubricApiObject = None
    ## Try to get the api object, but count 404 errors as not attached to published assignment
    try:
        rubricApiObject, _ = makeApiCall(localSetup, p1_apiUrl = rubricApiUlr, p1_payload = rubricApiPayload)
    except Exception as Error:
        msg = str(Error)
        ## If the error is a 404 error, the rubric may be in an unsaved state or deleted, so return false
        if "HTTP 404" in msg or "Rubric not found" in msg:
            return False
        else:
            raise

                    
    ## Save the primary body of information retrieved by the API call
    rubricApiText = rubricApiObject.text
        
    ## Convert the json body of information into a Python Dictionary
    rubricApiDict = json.loads(rubricApiText)
                    
    ## Define a boolean variable to track whether the rubric is attached to any published assignments
    ## in the relavent course
    attachedToPublishedAssignment = False

    ## If the rubric has associations in its keys
    if "associations" in rubricApiDict.keys():
        
        ## If the associations key has a value
        if rubricApiDict["associations"]:

            ## For each association
            for association in rubricApiDict["associations"]:

                ## If attachedToPublishedAssignment is still false
                if not attachedToPublishedAssignment:
                
                    ## If the association is an assignment
                    if (association["association_type"] == "Assignment"):
                        attachedToPublishedAssignment = assignmentIsPublishedCheck(rubricApiUlr, association["association_id"])

    return attachedToPublishedAssignment

## This function 

## This function Checks a course's rubrics for outcome alignments
## and checks to see if the rubric is attached to a published assignment
## Updating the p1_uniqueAttachedOutcomes dict to be true for the outcome
## if the rubric is attached to a published assignment
def checkRubricOutcomeAlignment(p1_row, p1_targetCourseSisId, p1_uniqueAttachedOutcomes, p1_uniqueAttachedOutcomesVendorGuidDict):
    functionName = "Check Rubric Outcome Alignment"

    try:
        ## Define a dict to hold the rubric ids of rubrics with the desired outcomes with values of the outcomes that are attached
        rubricsWithOutcomes = {}
            
        ## Define the course's API rubric call url
        courseRubricApiUlr = coreCanvasApiUrl + "courses/sis_course_id:" + p1_targetCourseSisId + "/rubrics" + "?per_page=100"
            
        ## Make a variable to hold the course's rubric api object
        courseRubricApiObject, _ = makeApiCall(localSetup, p1_apiUrl = courseRubricApiUlr)
            
        ## Save the primary body of information retrieved by the API call
        course_rubrics_api_call_text_jsonString = courseRubricApiObject.text
        
        ## Convert the json body of information into a Python Dictionary
        course_rubrics_api_call_text_dict = json.loads(course_rubrics_api_call_text_jsonString)
            
        ## Go through each rubric in the text dict
        for rubric in course_rubrics_api_call_text_dict:
                
            ## Go through each of the rubrics criterion
            for criterion in rubric["data"]:

                ## Define a refined criterion title and variable by replacing the unicode character
                criterionTitle = criterion["title"].replace('\u200b', '') if "title" in criterion.keys() \
                    else ""
                criterionDescription = criterion["description"].replace('\u200b', '') if "description" in criterion.keys() \
                    else ""

                ## Define the target identifier for the outcome as the title if it exists and contains the outcome area, otherwise the description
                targetOutcomeIdentifier = (criterionTitle 
                                           if (criterionTitle 
                                               and p1_row['Outcome Area'] in criterionTitle
                                               ) 
                                           else criterionDescription
                                           )
                
                ## If the criterion is an outcome
                if 'learning_outcome_id' in criterion.keys():

                    ## If the title of the outcome is in uniqueAttachedOutcomes
                    if targetOutcomeIdentifier in p1_uniqueAttachedOutcomes.keys():

                        ## Add the rubric id as a key and the outcome as a value in a list to the rubrics with outcomes dict, appending the outcome if the key already exists
                        rubricsWithOutcomes.setdefault(rubric["id"], []).append(targetOutcomeIdentifier)

                        
                    ## Otherwise check to see if the vendor id matches
                    else:    

                        ## Define a Get outcome api url
                        outcomeApiUrl = f"{coreCanvasApiUrl}outcomes/{criterion['learning_outcome_id']}"

                        ## Make a variable to hold the outcome api object
                        outcomeApiObject, _ = makeApiCall(localSetup, p1_apiUrl = outcomeApiUrl)
                        
                        ## Save the primary body of information retrieved by the API call
                        outcomeApiText = outcomeApiObject.text
                        
                        ## Convert the json body of information into a Python Dictionary  
                        outcomeApiDict = json.loads(outcomeApiText)

                        ## Define a refined outcome title variable by replacing the unicode character
                        outcomeTitle = outcomeApiDict["title"].replace('\u200b', '')
                
                        ## If the vendor_guid of the outcome is in the keys of the uniqueAttachedOutcomesVendorGuidDict or if the title of the outcome is in uniqueAttachedOutcomes
                        if outcomeApiDict["vendor_guid"] in p1_uniqueAttachedOutcomesVendorGuidDict.values():

                            ## Add the rubric id as a key and the outcome as a value in a list to the rubrics with outcomes dict, appending the outcome if the key already exists
                            rubricsWithOutcomes.setdefault(rubric["id"], []).append(outcomeTitle)

                    
        ## For each rubric in the rubrics with outcomes list 
        for rubric_id in rubricsWithOutcomes:
            
            ## If the rubric is attached to a published assignment
            if rubricIsAttachedToAPublishedAssignmentCheck(courseRubricApiUlr, rubric_id):

                ## For each outcome in the list of outcomes attached to the rubric
                for outcome in rubricsWithOutcomes[rubric_id]:

                    ## If the outcome's value in the unique outcomes attached dict is still false
                    if p1_uniqueAttachedOutcomes[outcome] == False:

                        ## Set the value to true
                        p1_uniqueAttachedOutcomes[outcome] = True

    ## If there is an error
    except Exception as Error:
        errorHandler.sendError (functionName, Error)

## This function checks the rubrics in each course on the list to see which, if any, have the required outcome/s 
## and if those rubrics are attached to a published assignment. It adds the course to the naughty list if any of 
## these checks come back false
def outcomeAttachmentReport(row, p1_rawOutcomesDF, p1_outcomeCoursesMissingAttachmentsDataDict):
    functionName = "Check Outcome Attachments"

    try:

        ## All courses sis ids should be strings, otherwise there is an issue with the row so ignore it
        if not isinstance(row["Course_sis_id"], str):
            return
    
        ## Make variables for the relavent course information
        courseSisId = row["Course_sis_id"]
        courseName = row["Course_name"]
        targetCourseSisId = None

        ## If there is a parent course id
        if (not pd.isna(row["Parent_Course_sis_id"]) 
        and row["Parent_Course_sis_id"] not in ["", None]
        ):
            
            ## Set the target course id to the parent course id
            targetCourseSisId = row["Parent_Course_sis_id"]

        ## If there is no parent course id
        else:
            
            ## Set the target course id to the course id
            targetCourseSisId = courseSisId

        ## Make a dict of the unique outcomes associated with the course
        uniqueAttachedOutcomes = {row[key]: False for key in row.index 
            if "Outcome" in key 
            and "Area" not in key 
            and isPresent(row[key])
            }
            
        ## Make a filtered df by keeping only the rows where the outcome is in the uniqueAttachedOutcomes and the row['Outcome Area'] is in the title
        outcomesDF = p1_rawOutcomesDF[
            (p1_rawOutcomesDF['title'].isin(uniqueAttachedOutcomes.keys()))
             & (p1_rawOutcomesDF['title'].str.contains(row['Outcome Area']))
             ]
        
            
        ## Make a dict with the outcome titles as keys and the vendor_guids as values
        uniqueAttachedOutcomesVendorGuidDict = {row2['title']: row2['vendor_guid'] for index, row2 in outcomesDF.iterrows()}
        
        ## Check the rubrics in the course for the desired outcomes
        checkRubricOutcomeAlignment(row, targetCourseSisId, uniqueAttachedOutcomes, uniqueAttachedOutcomesVendorGuidDict)

        ## If any of the unique outcomes attached to the course are still false
        ##if False in uniqueAttachedOutcomes.values():

            ## Check the new quizzes in the course for the desired outcomes


        ## If any of the unique outcomes attached to the course are continue to be false after checking the rubrics and quizz
        if False in uniqueAttachedOutcomes.values():
            
            ## Make a list of the outcomes that are still false
            missingOutcomes = [key for key, value in uniqueAttachedOutcomes.items() if value == False]

            ## Make a list of instructor name values that are non nan
            instructorNames = [row[instructorColumn] for instructorColumn in row.index if (
                "name" in instructorColumn
                and "Instructor" in instructorColumn
                and not pd.isna(row[instructorColumn])
                )
            ]
            
            ## Make a list of the instructor email values that are non nan
            instructorEmails = [row[instructorColumn] for instructorColumn in row.index if (
                "email" in instructorColumn
                and "Instructor" in instructorColumn
                and not pd.isna(row[instructorColumn])
                )
            ]
            
            ## Make a string of the missing outcomes
            missingOutcomesString = ", ".join(missingOutcomes)

            ## If there is more than one outcome in the missing outcomes list
            if len(missingOutcomes) > 1:
            
                ## Get the last outcome in the missing outcomes list
                lastMissingOutcome = missingOutcomes[-1]
            
                ## Replace the last outcome in the missing outcomes list with "and" + the last outcome
                missingOutcomesString = missingOutcomesString.replace(lastMissingOutcome, f"and {lastMissingOutcome}")

            ## Make a string of the teacher names
            instructorNamesString = ", ".join(instructorNames)
            
            ## Make a string of the teacher emails
            instructorEmailsString = ", ".join(instructorEmails)
            
            ## Add the course's information to the dictionary of courses missing outcomes
            p1_outcomeCoursesMissingAttachmentsDataDict["Course_name"].append(courseName)
            p1_outcomeCoursesMissingAttachmentsDataDict["Required Outcome"].append(missingOutcomesString)
            p1_outcomeCoursesMissingAttachmentsDataDict["Issue"].append("The Associated Outcome/s is/are not attached to a published assignment")
            p1_outcomeCoursesMissingAttachmentsDataDict["Instructor Name"].append(instructorNamesString)
            p1_outcomeCoursesMissingAttachmentsDataDict["Instructor Email"].append(instructorEmailsString)

    except Exception as Error:
        errorHandler.sendError (functionName, Error)
                    
  
## This function processes the rows of the CSV file and sends on the relavent data to process_course
def termOutcomeAttachmentReport (p1_inputTerm
                                 , p1_targetDesignator
                                 ):
    functionName = "Term OutcomeAttachment Report"

    try:
       
        ## Extract term prefix and decade+
        
        ## Extract term prefix and decade
        termCodePrefix = p1_inputTerm[:2]  ## e.g., "FA", "SP", "SU"
        termWord = undgTermsCodesToWordsDict.get(termCodePrefix, gradTermsCodesToWordsDict.get(termCodePrefix))
        termYear = int(str(localSetup.dateDict["century"]) + p1_inputTerm[2:])
        schoolYear = localSetup.getSchoolYear(termWord, termYear)

        ## Build local paths  
        designatorLocalOutputPath = localSetup.getTargetDesignatedOutputPath(termWord, termYear, p1_targetDesignator)

        ## Ensure directories exist
        os.makedirs(designatorLocalOutputPath, exist_ok=True)

        ## Define the output file name
        termOutputFileName = f"{p1_inputTerm}_{p1_targetDesignator}_Outcome_Attachment_Report.csv"

        ## Build the designated internal output path
        targetDestinationFilePath = os.path.join(designatorLocalOutputPath, termOutputFileName)
        targetExternalDestinationFilePath = os.path.join(localSetup.getExternalResourcePath("IE"), schoolYear, p1_inputTerm, termOutputFileName)

        ## If the file is recent return
        if isFileRecent(localSetup, targetDestinationFilePath):
            return targetDestinationFilePath
            
        ## Retrieve Automated Outcome Tool Variables
        automatedOutcomeToolVariablesDf = pd.read_excel(
            os.path.join(localSetup.getExternalResourcePath("TLC"), "Automated Outcome Tool Variables.xlsx")
        )
        targetAccountName = automatedOutcomeToolVariablesDf.loc[
            automatedOutcomeToolVariablesDf["Target Designator"] == p1_targetDesignator,
            "Outcome Location Account Name"
        ].values[0]

        ## Retrieve the current outcomes csv file path
        rawOutcomesDF = CanvasReport.getOutcomesDf(localSetup, p1_inputTerm, targetAccountName, p1_targetDesignator)

        ## Remove the unicode character from the title column
        rawOutcomesDF['title'] = rawOutcomesDF['title'].str.replace('\u200b', '')

        ## Get the relavent term's course report as a df
        termActiveOutcomeCoursesDF = CanvasReport.getActiveOutcomeCoursesDf(localSetup, p1_inputTerm, p1_targetDesignator)
        
        ## For each column in the term active Outcome courses df
        for column in termActiveOutcomeCoursesDF.columns:

            ## If the column has outcome in the name and doesn't have area in the name
            if "Outcome" in column and "Area" not in column:
                
                ## Replace any nan values with ""
                termActiveOutcomeCoursesDF[column] = termActiveOutcomeCoursesDF[column].fillna("")

        ## Define a dict to hold the assignment data for published assignments with outcomes
        outcomeCoursesMissingAttachments = {
            "Course_name": []
            , "Required Outcome": []
            , "Issue": []
            , "Instructor Name": []
            , "Instructor Email": []
            }
        
        ## Create a list to hold the ongoing outcome attachment report threads
        outcomeAttachmentReportThreads = []
        
        ## For each row in the termActiveOutcomeCoursesDF
        for index, row in termActiveOutcomeCoursesDF.iterrows():

                ## Target a specific course for testing if needed
                # if row['Course_sis_id'] == "SP2026_EDUC3090_1L":
                    
                #     outcomeAttachmentReport (row, rawOutcomesDF, outcomeCoursesMissingAttachments)
            
                # If the row is not a nan
                if not pd.isna(row["Course_sis_id"]):
                
                    ## Create a thread to process the row
                    outcomeAttachmentReportThread = threading.Thread(target=outcomeAttachmentReport
                                                                     , args=(row
                                                                             , rawOutcomesDF
                                                                             , outcomeCoursesMissingAttachments
                                                                             )
                                                                     )
                
                    ## Start the thread
                    outcomeAttachmentReportThread.start()
                
                    ## Add the thread to the ongoing threads list
                    outcomeAttachmentReportThreads.append(outcomeAttachmentReportThread)
                
        ## For each thread in the ongoing threads list
        for thread in outcomeAttachmentReportThreads:
            
            ## Wait for the thread to finish
            thread.join()
            
        # If any of the lists in the outcomeCoursesMissingAttachments dict are not empty
        if any([len(outcomeCoursesMissingAttachments[key]) > 0 for key in outcomeCoursesMissingAttachments.keys()]):
            
            ## Create a dataframe from the outcomeCoursesMissingAttachments dict
            outcomeCoursesMissingAttachmentsDF = pd.DataFrame(outcomeCoursesMissingAttachments)

            ## Save the dataframe to a csv to both the local and external output paths
            outcomeCoursesMissingAttachmentsDF.to_csv(f"{targetDestinationFilePath}", index = False)

            ## Copy the file to the external destination
            shutil.copy2(targetDestinationFilePath, targetExternalDestinationFilePath)

        return targetDestinationFilePath

    except Exception as Error:
        errorHandler.sendError (functionName, Error)

if __name__ == "__main__":

    ## Define the API Call header using the retreived Canvas Token
    ##header = {'Authorization' : f"Bearer {canvasAccessToken}"}

    ## Start and download the Canvas report
    termOutcomeAttachmentReport (
        p1_inputTerm = input("Enter the desired term in four character format (FA20, SU20, SP20): ")
        , p1_targetDesignator = input("Enter the desired target designator (GE, I-EDUC, U-ENGR): ")
        )

    input("Press enter to exit")