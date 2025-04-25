# Author: Bryce Miller - brycezmiller@nnu.edu
# Last Updated by: Bryce Miller

## Import Generic Moduels

import traceback, os, sys, logging, threading, csv, requests, json, pdfkit, re, os, os.path, time
from datetime import date, datetime
import pandas as pd

## Set working directory
os.chdir(os.path.dirname(__file__))

## Add Script repository to syspath
sys.path.append(f"{os.getcwd()}\ResourceModules")

## Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = "Outcome Attachment Report"

scriptPurpose = r"""
The Course Addendum Checker Script was written by NNU's IDT department to check whether NNU's canavs courses have the static Syllabus Addendum link, make .csv lista of the courses that do not have the link, and store the .csv files under \Employees-Read Only\University Syllabi by college and department.
"""
externalRequirements = r"""
To function properly, this script requires that the static Syllabus Addendum link "https://my.nnu.edu/ics/syllabus_addendum.aspx" (which redirects to the current addendum) be placed in the Canvas Syllabus tab.
"""

## Date Variables
currentDateTime = datetime.now()
currentYear = currentDateTime.year
currentMonth = currentDateTime.month
lastYear = currentYear - 1
nextYear = currentYear + 1
century = str(currentYear)[:2]
decade = str(currentYear)[2:]

## Set working directory
fileDir = os.path.dirname(__file__)
os.chdir(fileDir)

## The relative path is used to provide a generic way of finding where the Scripts TLC folder has been placed
## This provides a non-script specific manner of finding the vaiours related modules
PFRelativePath = r".\\"

## If the Scripts TLC folder is not in the folder the PFRelative path points to
## look for it in the next parent folder
while "Scripts TLC" not in os.listdir(PFRelativePath):

    PFRelativePath = f"..\\{PFRelativePath}"

## Change the relative path to an absolute path
PFAbsolutePath = f"{os.path.abspath(PFRelativePath)}\\"

## Add Input Modules to the sys path
sys.path.append(f"{PFAbsolutePath}Scripts TLC\\ResourceModules")
sys.path.append(f"{PFAbsolutePath}Scripts TLC\\ActionModules")

## Import local modules
from Error_Email_API import errorEmailApi
from Create_Sub_Account_Save_Path import determineDepartmentSavePath
from Get_Outcomes import termGetOutcomes
from Make_Api_Call import makeApiCall

## Local Path Variables
baseLogPath = f"{PFAbsolutePath}Logs\\{scriptName}\\"
configPath = f"{PFAbsolutePath}\\Configs TLC\\"
baseLocalInputPath = f"{PFAbsolutePath}Canvas Resources\\"  ## This is only the base path as the real path requires the requested term
baseLocalOutputPath = f"{PFAbsolutePath}Canvas Resources\\" ## This is only the base path as the real path requires the requested term

## External Path Variables

## Define a variable to hold the base external input path and output path 
baseExternalInputPath = None ## Where the sis input files are stored
baseExternalOutputPath = None ## Where the syllabus repository will be created and relavent reports stored

## Open Base_External_Paths.json from the config path and get the baseExternalInputPath and baseExternalOutputPath values
with open (f"{configPath}Base_External_Paths.json", "r") as file:
    fileJson = json.load(file)
    baseExternalInputPath = fileJson["baseExternalInputPath"]
    baseExternalOutputPath = fileJson["baseTlcUniversitySyllabiDataExternalOutputPath"]

## Canvas Instance Url
CoreCanvasAPIUrl = None
## Open the Core_Canvas_Url.txt from the config path
with open (f"{configPath}Core_Canvas_Url.txt", "r") as file:
    CoreCanvasAPIUrl = file.readlines()[0]

## If the script is run as main the folder with the access token is in the parent directory
canvasAccessToken = ""

## Open and retrieve the Canvas Access Token
with open (fr"{configPath}Canvas_Access_Token.txt", "r") as file:
    canvasAccessToken = file.readlines()[0]

##Primary API call header and payload
header = {'Authorization' : 'Bearer ' + canvasAccessToken}

## Begin logger set up

## If the base log path doesn't already exist, create it
if not (os.path.exists(baseLogPath)):
    os.makedirs(baseLogPath, mode=0o777, exist_ok=False)

## Log configurations
logger = logging.getLogger(__name__)
rootFormat = ("%(asctime)s %(levelname)s %(message)s")
FORMAT = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
logging.basicConfig(format=rootFormat, filemode = "a", level=logging.INFO)

## Info Log Handler
infoLogFile = f"{baseLogPath}\\Info Log.txt"
logInfo = logging.FileHandler(infoLogFile, mode = 'a')
logInfo.setLevel(logging.INFO)
logInfo.setFormatter(FORMAT)
logger.addHandler(logInfo)

## Warning Log handler
warningLogFile = f"{baseLogPath}\\Warning Log.txt"
logWarning = logging.FileHandler(warningLogFile, mode = 'a')
logWarning.setLevel(logging.WARNING)
logWarning.setFormatter(FORMAT)
logger.addHandler(logWarning)

## Error Log handler
errorLogFile = f"{baseLogPath}\\Error Log.txt"
logError = logging.FileHandler(errorLogFile, mode = 'a')
logError.setLevel(logging.ERROR)
logError.setFormatter(FORMAT)
logger.addHandler(logError)

## This variable enables the error_handler function to only send
## an error email the first time the function triggeres an error
## by tracking what functions have already been recorded as having errors
setOfFunctionsWithErrors = set()

## This function handles function errors
def error_handler (p1_ErrorLocation, p1_ErrorInfo, sendOnce = True):
    functionName = "error_handler"

    ## Log the error
    logger.error (f"     \nA script error occured while running {p1_ErrorLocation}. " +
                     f"Error: {str(p1_ErrorInfo)}")

    ## If the function with the error has not already been processed send an email alert
    if (p1_ErrorLocation not in setOfFunctionsWithErrors):
        errorEmailApi.sendEmailError(p2_ScriptName = scriptName, p2_ScriptPurpose = scriptPurpose, 
                                     p2_ExternalRequirements = externalRequirements, 
                                     p2_ErrorLocation = p1_ErrorLocation, p2_ErrorInfo = f"{p1_ErrorInfo}: \n\n {traceback.format_exc()}")
        
        ## Add the function name to the set of functions with errors
        setOfFunctionsWithErrors.add(p1_ErrorLocation)
        
        ## Note that an error email was sent
        logger.error (f"     \nError Email Sent")
    
    ## Otherwise log the fact that an error email as already been sent
    else:
        logger.error (f"     \nError email already sent")

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
                csvFile_2.close()
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
            csvFile_2.close()

    except Exception as error:
        error_handler (functionName, error)

## This function processes an assignment that an outcome is attached to to ensure that it is published
## and assigned to the primary course section
def assignmentIsPublishedCheck (p1_rubric_api_url, assignment_id):
    functionName = "Assignment Is Published Check"

    ## Isolate the api url through the sis ID (leaving out the rubric specific piece)
    assignmentApiUlr = p1_rubric_api_url[:57] + p1_rubric_api_url.split(':')[2].split('/')[0] + "/assignments/" + str(assignment_id)
    
    ## Define the api payload to include associations
    assignmentApiPayload = {"include": ["submission", "assignment_visibility"]}

    ## Make a variable to hold the course's rubric api object
    assignmentApiObject = makeApiCall(p1_header = header, p1_apiUrl = assignmentApiUlr, p1_payload = assignmentApiPayload)
        
    # ## Define a variable to track the number of attempts to get the api call
    # assignmentViewApiAttempts = 0
        
    # ## If the api call was not sucessful and the number of assignmentViewApiAttempts is less than 5
    # while (not assignmentApiObject
    #         or (assignmentApiObject.status_code != 200 
    #             and assignmentViewApiAttempts < 5)
    #         ):

    #     try: ## Irregular try clause, do not comment out in testing
                            
    #         ## If the number of attempts is greater than 0
    #         if assignmentApiObject:
                
    #             ## Close the previous api object
    #             assignmentApiObject.close()
    
    #             ## Wait for 2 seconds
    #             time.sleep(2)
                    
    #         ##  the api call again
    #         assignmentApiObject = requests.get(url=assignmentApiUlr, headers = header, params = assignmentApiPayload)
                            
    #     except Exception as error: ## Irregular except clause, do not comment out in testing
    #         logger.warning(f"Error: {error} \n Occured when calling {assignmentApiUlr} for assignment id: {assignment_id}")
            
    #     ## Increment the number of attempts
    #     assignmentViewApiAttempts += 1

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
    rubricApiObject = makeApiCall(p1_header = header, p1_apiUrl = rubricApiUlr, p1_payload = rubricApiPayload)
        
    # ## Define a variable to track the number of attempts to get the api call
    # rubricViewApiAttempts = 0
        
    # ## If the api call was not sucessful and the number of rubricViewApiAttempts is less than 5
    # while (not rubricApiObject
    #         or (rubricApiObject.status_code != 200 
    #             and rubricViewApiAttempts < 5)
    #         ):

    #     try: ## Irregular try clause, do not comment out in testing
                            
    #         ## If the number of attempts is greater than 0
    #         if rubricApiObject:
                
    #             ## Close the previous api object
    #             rubricApiObject.close()
    
    #             ## Wait for 2 seconds
    #             time.sleep(2)
                    
    #         ##  the api call again
    #         rubricApiObject = requests.get(url=rubricApiUlr, headers = header, params = rubricApiPayload)
                            
    #     except Exception as error: ## Irregular except clause, do not comment out in testing
    #         logger.warning(f"Error: {error} \n Occured when calling {rubricApiUlr} for rubric id: {p1_rubricId}")
            
    #     ## Increment the number of attempts
    #     rubricViewApiAttempts += 1
                    
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

## This function checks the rubrics in each course on the list to see which, if any, have the required outcome/s 
## and if those rubrics are attached to a published assignment. It adds the course to the naughty list if any of 
## these checks come back false
def outcomeAttachmentReport(row, p1_termLocalOutputPath, p1_outcomesLocationPath, p1_outcomeCoursesMissingAttachmentsDataDict):
    functionName = "Check Outcome Attachments"

    try:

        ## All courses sis ids should be strings, otherwise there is an issue with the row so ignore it
        if not isinstance(row["Course_sis_id"], str):
            return
    
        ## Make variables for the relavent course information
        courseSisId = row["Course_sis_id"]
        courseName = row["Course_name"]
        parentCourseSisId = row["Parent_Course_sis_id"]
        targetCourseSisId = None

        ## If there is a parent course id
        if pd.notna(row["Parent_Course_sis_id"]):
            
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
                      and str(row[key]) != "nan"
                      }
            
        ## Read the outcomes csv into a pandas dataframe
        rawOutcomesDF = pd.DataFrame()

        readRawOutcomesCsvAttempt = 0

        ## While the rawOutcomesDF is empty
        while rawOutcomesDF.empty and readRawOutcomesCsvAttempt < 5:

            try: ## Irregular try clause, do not comment out in testing
            
                ## Read the outcomes csv into a pandas dataframe
                rawOutcomesDF = pd.read_csv(p1_outcomesLocationPath, encoding='utf-8')
                
            except Exception as error: ## Irregular except clause, do not comment out in testing

                ## Log a warning that the csv file could not be read with the exception
                logger.warning (f"     \n {p1_outcomesLocationPath} could not be read. Error: {error}")

                ## Wait for 5 seconds
                time.sleep(5)

            ## Increment the readRawOutcomesCsvAttempt
            readRawOutcomesCsvAttempt += 1    

        ## if the rawOutcomesDF is still empty
        if rawOutcomesDF.empty:

            ## log a warning and return
            logger.warning (f"     \n {p1_outcomesLocationPath} is empty")
            return
        
        ## Remove the unicode character from the title column
        rawOutcomesDF['title'] = rawOutcomesDF['title'].str.replace('\u200b', '')
        
        ## Make a filtered df by keeping only the rows where the outcome is in the uniqueAttachedOutcomes and the row['Outcome Area'] is in the title
        outcomesDF = rawOutcomesDF[
            (rawOutcomesDF['title'].isin(uniqueAttachedOutcomes.keys()))
             & (rawOutcomesDF['title'].str.contains(row['Outcome Area']))
             ]
        
            
        ## Make a dict with the outcome titles as keys and the vendor_guids as values
        uniqueAttachedOutcomesVendorGuidDict = {row['title']: row['vendor_guid'] for index, row in outcomesDF.iterrows()}
        
        ## Define a dict to hold the rubric ids of rubrics with the desired outcomes with values of the outcomes that are attached
        rubricsWithOutcomes = {}
            
        ## Define the course's API rubric call url
        courseRubricApiUlr = CoreCanvasAPIUrl + "courses/sis_course_id:" + targetCourseSisId + "/rubrics" + "?per_page=100"
            
        ## Make a variable to hold the course's rubric api object
        courseRubricApiObject = makeApiCall(p1_header = header, p1_apiUrl = courseRubricApiUlr)
            
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
                                               and row['Outcome Area'] in criterionTitle
                                               ) 
                                           else criterionDescription
                                           )
                
                ## If the criterion is an outcome
                if 'learning_outcome_id' in criterion.keys():

                    ## If the title of the outcome is in uniqueAttachedOutcomes
                    if targetOutcomeIdentifier in uniqueAttachedOutcomes.keys():

                        ## Add the rubric id as a key and the outcome as a value in a list to the rubrics with outcomes dict, appending the outcome if the key already exists
                        rubricsWithOutcomes.setdefault(rubric["id"], []).append(targetOutcomeIdentifier)

                        
                    ## Otherwise check to see if the vendor id matches
                    else:    

                        ## Define a Get outcome api url
                        outcomeApiUrl = f"{CoreCanvasAPIUrl}outcomes/{criterion['learning_outcome_id']}"

                        ## Make a variable to hold the outcome api object
                        outcomeApiObject = makeApiCall(p1_header = header, p1_apiUrl = outcomeApiUrl)
                        
                        ## Save the primary body of information retrieved by the API call
                        outcomeApiText = outcomeApiObject.text
                        
                        ## Convert the json body of information into a Python Dictionary  
                        outcomeApiDict = json.loads(outcomeApiText)

                        ## Define a refined outcome title variable by replacing the unicode character
                        outcomeTitle = outcomeApiDict["title"].replace('\u200b', '')
                
                        ## If the vendor_guid of the outcome is in the keys of the uniqueAttachedOutcomesVendorGuidDict or if the title of the outcome is in uniqueAttachedOutcomes
                        if outcomeApiDict["vendor_guid"] in uniqueAttachedOutcomesVendorGuidDict.values():

                            ## Add the rubric id as a key and the outcome as a value in a list to the rubrics with outcomes dict, appending the outcome if the key already exists
                            rubricsWithOutcomes.setdefault(rubric["id"], []).append(outcomeTitle)

                    
        ## For each rubric in the rubrics with outcomes list 
        for rubric_id in rubricsWithOutcomes:
            
            ## If the rubric is attached to a published assignment
            if rubricIsAttachedToAPublishedAssignmentCheck(courseRubricApiUlr, rubric_id):

                ## For each outcome in the list of outcomes attached to the rubric
                for outcome in rubricsWithOutcomes[rubric_id]:

                    ## If the outcome's value in the unique outcomes attached dict is still false
                    if uniqueAttachedOutcomes[outcome] == False:

                        ## Set the value to true
                        uniqueAttachedOutcomes[outcome] = True

        ## If any of the unique outcomes attached to the course are still false
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

    except Exception as error:
        error_handler (functionName, error)
                    
  
## This function processes the rows of the CSV file and sends on the relavent data to process_course
def termOutcomeAttachmentReport (p1_inputTerm
                                 , p1_targetDesignator
                                 ):
    functionName = "Term OutcomeAttachment Report"

    try:
        
        ## Determine and save the term's school year
        schoolYear = None
        if re.search("AF|FA|GF", p1_inputTerm):
            ## Fall terms are the first terms of a new school year so FA20 is part of the 2020-21 school year.
            schoolYear = (century + p1_inputTerm[2:] + "-" + str(int(p1_inputTerm[2:]) + 1))
        elif re.search("SP|GS|AS|SG|SA|SU", p1_inputTerm):
            ## Spring and Summer terms belong in the same school year as the fall terms before them, so SP21 is part of the same 2020-21 school year as FA20.
            schoolYear = (century + str(int(p1_inputTerm[2:]) - 1) + "-" + p1_inputTerm[2:])

        ## Define the school year input paths
        localSchoolYearInputPath = f"{baseLocalOutputPath}{schoolYear}\\"

        ## Define the term specific input path
        termLocalInputPath = f"{localSchoolYearInputPath}{p1_inputTerm}\\"

        ## Determine term related output paths
        termLocalOutputPath = f"{baseLocalOutputPath}{schoolYear}\\{p1_inputTerm}\\"
        termExternalOutputPath = f"{baseExternalOutputPath}{schoolYear}\\{p1_inputTerm}\\"
        
        ## Retrieve the Automated Outcome Tool Variables excel file as a df    
        automatedOutcomeToolVariablesDf = pd.read_excel(f"{baseExternalInputPath}Internal Tool Files\\Automated Outcome Tool Variables.xlsx")

        ## Get the account name associated with the target designator
        targetAccountName = automatedOutcomeToolVariablesDf.loc[automatedOutcomeToolVariablesDf["Target Designator"] == p1_targetDesignator, "Outcome Location Account Name"].values[0]
        
        ## Open the accounts csv as a df
        accountInfoDF = pd.read_csv(f"{baseLocalInputPath}Canvas_Accounts.csv")

        # Get the canvas account id associated with the targetAccountName
        targetCanvasAccountId = 1 if p1_targetDesignator == "GE" else ( ## GE outcomes are located at the root account level which is not in the accounts csv
            accountInfoDF.loc[
                accountInfoDF["name"] == targetAccountName
                , "canvas_account_id"
                ].values[0]
            )
        
        ## If the targetCanvasAccountId is 1
        if targetCanvasAccountId != 1:
            
            ## Use the targetCanvasAccountId to determine the department specific path element
            departmentSpecifcPathElement = determineDepartmentSavePath(targetCanvasAccountId)
            
            ## Use the targetCanvasAccountId to get the external save paths
            termExternalOutputPath = (
                f"{baseExternalOutputPath}{departmentSpecifcPathElement}\\{schoolYear}\\{p1_inputTerm}\\"
                )
            
        ## If the termExternalOutputPath doesn't exist
        if not os.path.exists(termExternalOutputPath):

            ## Create the path
            os.makedirs(termExternalOutputPath, mode=0o777, exist_ok=False)

        ## Determine term related output file name
        termOutputFileName = f"{p1_inputTerm}_{p1_targetDesignator}_Courses_Without_Required_Outcome_Attached.csv"

        ## Define the target destination file path
        targetDestinationFilePath = f"{termExternalOutputPath}{termOutputFileName}"
        
        ## If the target output file exists
        if os.path.exists(targetDestinationFilePath):
            
            ## Get its last moddifed timestamp
            targetFileTimestamp = os.path.getmtime(targetDestinationFilePath)

            ## Convert the timestamp to datetime
            targetFileDateTime = datetime.fromtimestamp(targetFileTimestamp)

            ## Subtract the file's datetime from the current datetime
            targetFileHoursOld = int((currentDateTime - targetFileDateTime).total_seconds() // 3600)

            ## If it has been less than hour or more since the target was updated
            if targetFileHoursOld < 3.5:

                ## logger.info that the file is up to date and return
                logger.info (f"     \n {targetDestinationFilePath} is up to date")
                return
            
        ## Retrieve the current outcomes csv file path
        outcomesLocationPath = termGetOutcomes(p1_inputTerm, targetAccountName, p1_targetDesignator)

        ## Define the active outcome courses file path
        activeOutcomeCoursesFilePath = f"{termLocalInputPath}{p1_inputTerm}_{p1_targetDesignator}_Active_Outcome_Courses.xlsx"

        ## If the file doesn't exist
        if not os.path.exists(activeOutcomeCoursesFilePath):

            ## Log a warning that the file doesn't exist and return
            logger.warning (f"     \n {activeOutcomeCoursesFilePath} does not exist")
            return

        ## Read the relavent term's courses file into a pandas dataframe
        termActiveOutcomeCoursesDF = pd.read_excel(f"{termLocalInputPath}{p1_inputTerm}_{p1_targetDesignator}_Active_Outcome_Courses.xlsx")
        
        ## For each column in the term active Outcome courses df
        for column in termActiveOutcomeCoursesDF.columns:

            ## If the column has outcome in the name and doesn't have area in the name
            if "Outcome" in column and "Area" not in column:
                
                ## Replace any nan values with ""
                termActiveOutcomeCoursesDF[column].fillna("")

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
            #if row['Course_sis_id'] == "GF2024_EDUC7160_7A":
            
                ## If the row is not a nan
                if not pd.isna(row["Course_sis_id"]):
                
                    ## Create a thread to process the row
                    outcomeAttachmentReportThread = threading.Thread(target=outcomeAttachmentReport
                                                                     , args=(row
                                                                             , termLocalOutputPath
                                                                             , outcomesLocationPath
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
            
        ## If any of the lists in the outcomeCoursesMissingAttachments dict are not empty
        if any([len(outcomeCoursesMissingAttachments[key]) > 0 for key in outcomeCoursesMissingAttachments.keys()]):
            
            ## Create a dataframe from the outcomeCoursesMissingAttachments dict
            outcomeCoursesMissingAttachmentsDF = pd.DataFrame(outcomeCoursesMissingAttachments)

            ## If either the term local output path or the term external output path don't exist
            if not (os.path.exists(termLocalOutputPath) or os.path.exists(termExternalOutputPath)):
                
                ## Create them
                os.makedirs(termLocalOutputPath, mode=0o777, exist_ok=False)
                os.makedirs(termExternalOutputPath, mode=0o777, exist_ok=False)

            ## Save the dataframe to a csv to both the local and external output paths
            outcomeCoursesMissingAttachmentsDF.to_csv(f"{termLocalOutputPath}{termOutputFileName}", index = False)
            outcomeCoursesMissingAttachmentsDF.to_csv(f"{targetDestinationFilePath}", index = False)
            
            #outcomeAttachmentReport(row, termLocalOutputPath, termExternalOutputPath, termOutputFileName, newFileCreated, outcomeCoursesMissingAttachments)

    except Exception as error:
        error_handler (functionName, error)

if __name__ == "__main__":

    ## Define the API Call header using the retreived Canvas Token
    ##header = {'Authorization' : f"Bearer {canvasAccessToken}"}

    ## Start and download the Canvas report
    termOutcomeAttachmentReport (
        p1_inputTerm = input("Enter the desired term in four character format (FA20, SU20, SP20): ")
        , p1_targetDesignator = input("Enter the desired target designator (GE, I-EDUC, U-ENGR): ")
        )

    input("Press enter to exit")