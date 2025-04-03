## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller.

## Import Generic Moduels

import os, sys, logging, csv, os, os.path, threading, json
from datetime import date, datetime
import pandas as pd

## Define the script name, purpose, and external requirements for 
## logging and error reporting purposes
script_name = "Send_Outcome_Emails"

purpose_of_script = r"""
This script retrieves the contents of the active GE Outcomes information files, determines what emails needs to be sent, and sends them
"""
external_requirements_to_work_properly = r"""
See the microsoft API requirements
"""

## Set working directory
os.chdir(os.path.dirname(__file__))

# Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = "Course_Date_Related_Actions"

scriptPurpose = r"""

"""
externalRequirements = r"""

"""

# Time variables
currentDate = date.today()
todaysDateDateTime = datetime.combine(currentDate, datetime.min.time())
currentMonth = currentDate.month
currentYear = currentDate.year
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

## Add the script folders to the path
sys.path.append(f"{PFAbsolutePath}\\Scripts TLC\\ResourceModules")
sys.path.append(f"{PFAbsolutePath}\\Scripts TLC\\ActionModules")

## Import local modules
from Error_Email_API import errorEmailApi
from Core_Microsoft_Api import sendOutlookEmail
from Add_Outcomes_to_Active_Courses import addOutcomeToCourse, getUniqueOutcomesAndOutcomeCoursesDict, removeMissingOutcomes, retrieveDataForRelevantCommunication


## Local Path Variables
baseLogPath = f"{PFAbsolutePath}Logs\\{scriptName}\\"
configPath = f"{PFAbsolutePath}\\Configs TLC\\"
baseLocalInputPath = f"{PFAbsolutePath}Canvas Resources\\"

## External Path Variables

## Define a variable to hold the base external input path which is where the sis input files are stored
baseExternalInputPath = None 
## Open Base_External_Paths.json from the config path and get the baseExternalInputPath value
with open (f"{configPath}Base_External_Paths.json", "r") as file:
    fileJson = json.load(file)
    baseExternalInputPath = fileJson["baseExternalInputPath"]

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

## List of courses that don't need a syllabus. Syllabi for such courses are still gathered but they are not listed in the missing_syllabi.csv
list_of_courses_that_dont_need_syllabi = []
with open(f"{configPath}List_of_uneeded_syllabi.csv", 'r') as tempCsvFile:
    tempcsvReader = csv.DictReader(tempCsvFile, delimiter = ',')
    for row in tempcsvReader:
        list_of_courses_that_dont_need_syllabi.append(row['course_id'])
    tempCsvFile.close()

#Primary API call header and payload
header = {'Authorization' : 'Bearer ' + canvasAccessToken}
payload = {'include[]': ['syllabus_body', 'term', 'account', 'teachers', 'sections', 'total_students']}

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
                                     p2_ErrorLocation = p1_ErrorLocation, p2_ErrorInfo = p1_ErrorInfo)
        
        ## Add the function name to the set of functions with errors
        setOfFunctionsWithErrors.add(p1_ErrorLocation)
        
        ## Note that an error email was sent
        logger.error (f"     \nError Email Sent")
    
    ## Otherwise log the fact that an error email as already been sent
    else:
        logger.error (f"     \nError email already sent")

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
    
    ## If the relevant email is a course start email and GE is in the outcome
    if "GE" in p1_outcome:
        emailBodyDict["signature"] = """<p>Sincerely, 
        <br> The General Education Council</p>
        <span style="font-weight: bold;">Catherine Becker, Ph.D.</span>
        <br>General Education Council Chair
        <br>Associate Professor of English
        <br>Northwest Nazarene University
        """
    
    ## If the relevant email is a course start email and EDUC is in the outcome
    elif "I-EDUC" in p1_outcome:
        
        emailBodyDict["signature"] = """<p>Sincerely, 
        <br>Holly Ripley  Ed.S., M.Ed.
        <br>Chair, Education Department
        <br>Associate Professor of Education
        <br>Director, Accelerated Certification in Education (ACE)
        <br>Northwest Nazarene University
        <br>208-467-8621
        <br><br><a href='https://outlook.office.com/bookwithme/user/ab329a6a861642e8929104093bb4d929@nnu.edu?anonymous&ep=signature' target='_blank'>Book time to meet with me</a>
        </p>
        """
        
    ## Else if the outcome is an ENGR
    elif "ENGR" in p1_outcome:    
        
        ## Define the signature
        emailBodyDict["signature"] = """<p>Duke M Bulanon, PhD, PE
        <br>Professor
        <br>Department of Engineering and Physics
        <br>Northwest Nazarene University
        <br>Nampa, ID 83686
        <br>Tel no 208 467 8047
        </p>
        """

    ## If the relevant email is a course start email and GE is in the outcome
    if ("Course Start" in p3_relevantEmail
        and "GE" in p1_outcome
        ):


        ## Assign the course start greeting string
        emailBodyDict["greeting"] = "Greetings!"
        
        ## Assign the course start html formated email body
        emailBodyDict["formatedEmaiBody"] = f"""
<!DOCTYPE html>
<html>
<head>
    <style>
        td {{
            padding: 10px;
        }}

        .bold-text {{
            font-weight: bold;
        }}
    </style>
</head>
<body>
    <p>Hello {singularOrPluralDict["Professor/Professors"]} {p1_instructorNameOrNames},</p>

    <p>You are receiving this email because you are scheduled to be {singularOrPluralDict["an instructor/instructors"]} of the NNU outcome course {p1_course}, which is associated with the following {singularOrPluralDict['outcome/outcomes']}:</p>
    
    <ul>{p1_outcome}</ul>
    
    <p>You will find the language for {singularOrPluralDict['this/these']} {singularOrPluralDict['outcome/outcomes']} in the list linked below.</p>

    <p>Please ensure the following steps are completed for your course:</p>

    <ol>
        <li>Identify which {singularOrPluralDict['outcome/outcomes']} should be assessed in your course. Outcomes are designated by two letters and one number (i.e. HU1). I have highlighted one of my courses below as an example.</li>
        <li><a href="https://library.nnu.edu/general-education/outcomes-and-rubrics">Follow this link to our General Education Guide to identify your outcome language.</a>
        <br>Here is the outcome language for my highlighted course:
            <div style="border-collapse: collapse; border-spacing: 0; max-width: 70%; margin-bottom: 20px; border: 1px solid rgb(221, 221, 221); color: rgb(51, 51, 51); font-family: Arial, Helvetica, Verdana; font-size: 12px; overflow: hidden; display: flex;">
                <div style="box-sizing: border-box; padding: 8px; height: 100%; line-height: 1.42857; vertical-align: middle; position: relative; border: none; min-width: 50px;">
                    <span dir="ltr" style="box-sizing: border-box; margin: 0; font-weight: bold;">HU1:</span>
                </div>
                <div style="box-sizing: border-box; padding: 8px; line-height: 1.42857; vertical-align: top; position: relative; border: none; flex-grow: 1;">
                    <span dir="ltr" style="box-sizing: border-box; margin: 0; padding-left: 5px;">Students will understand & appreciate visual, musical, and literary art based on the historical, political, and socio-cultural contexts in which they emerged.&nbsp;</span>
                </div>
            </div>
        </li>
        <li>Copy the outcome language.</li>
        <li>Ensure the outcome language is in your course syllabus. Here is a syllabus statement from a previous outcome associated course as an example:
            <br><br>
            <div style="margin-left: 20px; font-style: italic;">
                General Education Outcomes
                <br>Humanities
                <br><span style="font-weight: bold;">HU1, Transformation</span> - Students will understand & appreciate literary artworks based on the historical,
                <br>political, and socio-cultural contexts in which they emerged.
            </div>
        </li>
    </ol>

    <p>That's all for now. Please let us know if you have any questions.</p>

    {emailBodyDict["signature"]}
</body>
</html>
"""

    ## If the relevant email is a reminder or an alert
    else:
        
        ## Define the action
        emailBodyDict["bulletted resource list"]  = """<li><a href='https://community.canvaslms.com/t5/Instructor-Guide/How-do-I-align-an-outcome-with-a-rubric-in-a-course/ta-p/1130' target='_blank'>Attaching an outcome to a rubric</a></li>
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
            emailBodyDict["timeOfYearReminder"] = f"""As we begin the term, please consider how you will perform your outcome assessment, particularly which course assignment or assignments you will attach the {singularOrPluralDict['outcome/outcomes']} to."""
        
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
            emailBodyDict["timeOfYearReminder"] = f"""For outcome data to be recorded, an additional grading step is required for each student that submitted to an assignment with an outcome rubric attached."""
        
        emailBodyDict["formatedEmaiBody"] = f"""<!DOCTYPE html>
<html>
<body>
    <p>Hello {singularOrPluralDict["Professor/Professors"]} {p1_instructorNameOrNames},<br></p>
    
    <p>You are receiving this email because you {emailBodyDict["future/current instructor"]} {singularOrPluralDict["an instructor/instructors"]} of the NNU outcome course {p1_course}, {emailBodyDict["dynamic cause"]}</p>
    
    <ul>{p1_outcome}</ul>
    
    <p>{emailBodyDict["timeOfYearReminder"]}<br></p>
    
    <p>If you would like a refresher on how to do this, please identify your interest below:</p>
    
    <ul>{emailBodyDict["bulletted resource list"]}</ul>
    
    <p>{p1_emailDetails['Relevant Authority Contact Name']} at <a href='mailto:{p1_emailDetails['Client Send/Recieve Email']}'>{p1_emailDetails['Client Send/Recieve Email']}</a> is a good resource for how to assess your associated outcomes in this field of study. Additionally, NNU's Teaching and Learning Center at <a href='mailto:tlc@nnu.edu'>tlc@nnu.edu</a> is always ready to provide ideas, best practice tips, and assistance with creating and assessing outcomes.<br></p>

    {emailBodyDict["signature"]}
"""
        
    return emailBodyDict["formatedEmaiBody"]

## This function crafts and sends the relevant outcome email
def craftAndSendRelevantEmail (
        p3_inputTerm
        , p2_relevantEmail
        , p2_row#):
        , p1_auxillaryDFDict
        ):
    
    functionName = "craftAndSendRelevantEmail"
    
    try:
    
        ## Retrieve the Automated Outcome Tool Variables excel file as a df    
        automatedOutcomeToolVariablesDf = pd.read_excel(f"{baseExternalInputPath}Internal Tool Files\\Automated Outcome Tool Variables.xlsx")
        
        ## Filter the automated outcome tool variables df to only the row with the relevant outcome area
        automatedOutcomeToolVariablesDict = automatedOutcomeToolVariablesDf[
            automatedOutcomeToolVariablesDf["Target Designator"] == p2_row["Outcome Area"]
            ].iloc[0].to_dict()
        
        ## Make a filtered Unassessed Outcome Courses DF that only includes the course that the email is being sent about
        filteredUnassessedOutcomeCoursesDF = p1_auxillaryDFDict["Unassessed Outcome Courses DF"][
            p1_auxillaryDFDict["Unassessed Outcome Courses DF"]["Course_name"] == p2_row["Course_name"]
            ]

        ## Create an email details dictionary
        emailDetails = {"Client Name" : automatedOutcomeToolVariablesDict["Client Name"]
                        , "Client Send/Recieve Email" : automatedOutcomeToolVariablesDict["Client Send/Recieve Email"]
                        , "Relevant Authority Contact Name" : automatedOutcomeToolVariablesDict["Client Contact Name"]
                        , "Input Term": p3_inputTerm
                        , "Course Name": p2_row["Course_name"]
                        , "Relevant Email": p2_relevantEmail
                        , "Outcome Email Subject": f"{p2_row['Course_name']} {p2_relevantEmail}"
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
                    if "name" in key and pd.notna(datapoint):
                
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
                    elif "email" in key and pd.notna(datapoint):

                        ## If the key does not already exist in the email details
                        if "Instructor Email Or Emails String" not in emailDetails.keys():
                            
                            ## Add the teacher name to the list of instructor names
                            emailDetails["Instructor Email Or Emails String"] = f"{datapoint}, "
                        
                        ## Otherwise
                        else:

                            ## Add the teacher name to the list of instructor names
                            emailDetails["Instructor Email Or Emails String"] += f"{datapoint}, "


                ## If the datapoint is an outcome
                elif "Outcome" in key and pd.notna(datapoint) and key != "Outcome Area":
                    
                    ## If the email is a missing required data email
                    if "Missing" in p2_relevantEmail:
                        
                        ## If the data point is not amoung a list of the unique outcome titles in the filteredUnassessedOutcomeCoursesDF
                        if datapoint not in filteredUnassessedOutcomeCoursesDF["Outcome_Title"].values:
                            
                            ## Skip the datapoint
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
            
            # Send the Outcome Email
            sendOutlookEmail(p1_microsoftUserName = "lmsservice@nnu.edu"
                             , p1_subject = emailDetails['Relevant Email']
                             , p1_body = emailDetails['Outcome Email Body']
                             , p1_recipientEmailList = emailDetails['Instructor Email Or Emails String']
                             , p1_shared_mailbox = emailDetails['Client Send/Recieve Email']
                             )

    except Exception as error:
        error_handler (functionName, error)

## This function determines what course date related actions need to be taken for a specific term and performs them
def termDetermineAndPerformRelevantActions (p1_inputTerm
                                            , p1_targetDesignator
                                            ):
    functionName = "Term Determine And Send Relevant Communication"

    try:

        ## Retrieve the data for determining and sending out relevant communication
        completeActiveCanvasCoursesDF, auxillaryDFDict = retrieveDataForRelevantCommunication(p2_inputTerm = p1_inputTerm
                                                                                              , p3_targetDesignator = p1_targetDesignator
                                                                                              )
                
        ## Define a list to hold the communication threads
        actionThreads = []
        
        ## For each row in the complete active canvas courses df
        for index, row in completeActiveCanvasCoursesDF.iterrows():
            
            ## If ENGR4250 in row long_name
            #if "ENGR4250" in row["long_name"]:

                ## Define a variable to track whether the course is an outcome course
                isOutcomeCourse = True if row["long_name"] in auxillaryDFDict["Active Outcome Courses DF"]["Course_name"].values else False

                ## Define a relevant auxillary DF dict with empty dataframes
                relevantAuxillaryDfDict = {}
            
                ## If the course is in the list of courses who do not have their outcome attached to a published assignment
                if not auxillaryDFDict["Outcome Courses Without Attachments DF"].empty:
                    
                    ## Isolate the course's data in p1_outcomeCoursesWithoutAttachmentDF
                    relevantAuxillaryDfDict["Relevant Course Outcome Without Attachment Df"] = (
                        auxillaryDFDict["Outcome Courses Without Attachments DF"][
                            auxillaryDFDict["Outcome Courses Without Attachments DF"]["Course_name"] == row["long_name"]
                            ]
                        )
                    
                ## Otherwise
                else:

                    ## Create an empty dataframe for the courses without attached outcomes
                    relevantAuxillaryDfDict["Relevant Course Outcome Without Attachment Df"] = pd.DataFrame()

                ## If the course is in the list of courses who have no outcome results
                if not auxillaryDFDict["Unassessed Outcome Courses DF"].empty:
                    
                    ## Isolate the course's data in p1_outcomeCoursesWithoutOutcomeData
                    relevantAuxillaryDfDict["Relevant Course Outcome Without Data Df"] = (
                        auxillaryDFDict["Unassessed Outcome Courses DF"][
                            auxillaryDFDict["Unassessed Outcome Courses DF"]["Course_name"] == row["long_name"]
                            ]
                        )
                    
                ## Otherwise
                else:
                    
                    ## Create an empty dataframe for the courses without outcome data
                    relevantAuxillaryDfDict["Relevant Course Outcome Without Data Df"] = pd.DataFrame()
            
                ## Define a variable to track what email, if any, needs to be sent to the instructors of the course
                relevantEmailList = []
            
                ## If it is the monday before the courses's week 0 and it is an outcome course
                if (row['Course Week'] <= 0
                    and currentDate.weekday() == 3
                    and isOutcomeCourse
                    ):                
                    
                        ## Make a list of the unique outcomes that are not blank 
                        ## and a dict to hold the course id of the course named after each outcome
                        uniqueOutcomes, outcomeCourseDict = getUniqueOutcomesAndOutcomeCoursesDict(completeActiveCanvasCoursesDF)

                        ## Remove any outcomes that don't have corresponding courses
                        auxillaryDFDict["Active Outcome Courses DF"] = removeMissingOutcomes (auxillaryDFDict["Active Outcome Courses DF"], uniqueOutcomes, outcomeCourseDict)
                    
                        ## Start a thread to make sure the outcome has been added to the course
                        addOutcomeThread = threading.Thread(
                            target=addOutcomeToCourse
                            , args=(row
                                    , auxillaryDFDict
                                    )
                            )

                        ## Start the thread
                        addOutcomeThread.start()

                        ## Add the thread to the list of communication threads
                        actionThreads.append(addOutcomeThread)
        
                ## If it is the Monday of week 0
                if (row['Course Week'] == 0
                    and currentDate.weekday() == 3
                      ):

                    ## If the course is an Outcome course
                    if isOutcomeCourse:

                        ## Send the courses's instructors the Course Start email
                        relevantEmailList.append("Associated Course Outcomes: Course Start Information")
            
                ## Otherwise, if it is the Monday of the week before the course's midpoint (e.g. week 7 in a 16 week course)
                elif (row['Course Week'] == (int(row["Course Final Week"] / 2) - 1)
                      and currentDate.weekday() == 0
                      ): ## Casting the result of courseLength / 2 to int rounds the number down        
            
                    ## If the course is an Outcome course
                    if isOutcomeCourse:

                        ## If the course is an outcome course that does not have all of its outcomes attached to published assignments
                        if not relevantAuxillaryDfDict["Relevant Course Outcome Without Attachment Df"].empty:    
                
                            ## Send the courses's instructors the Midterm Reminder email
                            relevantEmailList.append("Associated Course Outcomes: Midterm Reminder")

                ## Otherwise, if it is the Monday of the week before its final week (e.g. week 15 in a 16 week course)
                elif (row['Course Week'] == (row["Course Final Week"] - 1)
                      and currentDate.weekday() == 1
                      ):

                    ## If the course is an Outcome course
                    if isOutcomeCourse:

                        ## If the course is an outcome course that does not have all of its outcomes attached to published assignments
                        if not relevantAuxillaryDfDict["Relevant Course Outcome Without Attachment Df"].empty: 
                
                            ## Send the courses's instructors the Finals Reminder email
                            relevantEmailList.append("Associated Course Outcomes: Finals Reminder")

                ## Otherwise, if it is the wednesday of the week after finals (e.g. week 17 from the start of a 16 week course)
                elif (row['Course Week'] == (row["Course Final Week"] + 1)
                      and currentDate.weekday() == 2
                      ):
            
                    ## If the course is an Outcome course
                    if isOutcomeCourse:

                        ## If the course is in the list of courses who do not have all of their outcome data
                        if not relevantAuxillaryDfDict["Relevant Course Outcome Without Data Df"].empty:    
            
                            ## Send the courses's instructors the Missing Data email as the course's outcome data is past due
                            relevantEmailList.append("Associated Course Outcomes: Missing Required Data")

                ## For each determined relevant email
                for relevantEmail in relevantEmailList:
                
                    ## Define a target row variable
                    targetRow = None
                
                    ## If the relevant email contains the word "Outcome"
                    if "Outcome" in relevantEmail:
                    
                        ## Find the index of course in the active outcome courses df
                        courseIndex = auxillaryDFDict["Active Outcome Courses DF"][
                            auxillaryDFDict["Active Outcome Courses DF"]["Course_name"] == row["long_name"]
                            ].index[0]

                        ## Define the target row as the row in the active outcome courses df
                        targetRow = auxillaryDFDict["Active Outcome Courses DF"].loc[courseIndex]
                    
                        
                    ## Create a thread to send the relevant outcome email
                    communicationThread = threading.Thread(
                        target=craftAndSendRelevantEmail
                        , args=(p1_inputTerm
                                , relevantEmail
                                , targetRow
                                , auxillaryDFDict
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

    except Exception as error:
        error_handler (functionName, p1_ErrorInfo = error)


## For testing
if __name__ == "__main__":
     
    # craftAndSendRelevantEmail (p2_relevantEmail="Associated Course Outcomes: Course Start Information"
    #                            ,p2_row = {'Term': 'FA24'
    #                                       , 'Outcome Area': 'I-EDUC'
    #                                       , 'Course_sis_id': 'SP2024_ENGL4980_01'
    #                                       , 'Course_name': 'SENIOR SEMINAR SP2024_ENGL4980_01'
    #                                       , 'Account_id': 'U_LLIT'
    #                                       , 'Number_of_students': 7
    #                                       , 'Outcome 1': 'I-EDUC_HU2_U2024'
    #                                       , 'Outcome 2': None
    #                                       , 'Instructor_#1_ID': 70009
    #                                       , 'Instructor_#1_name': 'Catherine Becker'
    #                                       , 'Instructor_#1_email': 'brycezmiller@nnu.edu'
    #                                       #, 'Instructor_#2_name': 'Dale Hamilton'
    #                                       },
    #                             p3_inputTerm="FA24")
    
    ## Get an input term and start the term outcome email function
    termDetermineAndPerformRelevantActions (
        p1_inputTerm = input("Enter the desired term in four character format (FA20, SU20, SP20): ")
        , p1_targetDesignator = input("Enter the desired target designator (GE, I-EDUC, U-ENGR): ")
        )

    input("Press enter to exit")