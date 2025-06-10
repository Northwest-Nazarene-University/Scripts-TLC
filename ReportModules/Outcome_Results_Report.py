# Author: Bryce Miller - brycezmiller@nnu.edu
# Last Updated by: Bryce Miller


from datetime import datetime
import traceback, os, logging, sys, re, threading, shutil, ast, json
import pandas as pd, numpy as np, time


## Set working directory
os.chdir(os.path.dirname(__file__))

# Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = "Outcome_Results Report"

scriptPurpose = r"""
This script (Outcome_Results_Report) views the active GE course lists and Outcome Results reports for a given term,
and creates a report that shows which courses have outcome data for the required GE outcomes. The script also creates
a csv report to that shows the highest scoring outcome for each student in each course. The script saves the reports
in the Canvas Resources folder and the Institutional Effectiveness shared drive.
"""
externalRequirements = r"""
To function properly this script requires access to the institutions Canvas instance via an Active Canvas Bearer Token
"""

## Date Variables
currentDate = datetime.now()
currentMonth = currentDate.month
currentYear = currentDate.year
century = str(currentYear)[:2]
decade = str(currentYear)[2:]

## Relative Path (this changes depending on the working directory of the main script)
PFRelativePath = r".\\"

## If the Canvas directory is not in the folder the relative path points to
## find the Canvas directory and set the relative path to its parent folder
targetFolder = os.listdir(PFRelativePath)
while "Scripts TLC" not in os.listdir(PFRelativePath):
    
    PFRelativePath = f"..\\{PFRelativePath}"
    targetFolder = os.listdir(PFRelativePath)

## Change the relative path to an absolute path
PFAbsolutePath = f"{os.path.abspath(PFRelativePath)}\\"

## Add Input Modules to the sys path
sys.path.append(f"{PFAbsolutePath}Scripts TLC\\ResourceModules")
sys.path.append(f"{PFAbsolutePath}Scripts TLC\\ActionModules")

## Import local modules
from Error_Email_API import errorEmailApi
from Create_Sub_Account_Save_Path import determineDepartmentSavePath
from Get_Outcomes import termGetOutcomes
from Get_Outcome_Results import termGetOutcomeResults
from Make_Api_Call import makeApiCall
from Get_Active_Outcome_Courses import termGetActiveOutcomeCourses
from Get_Enrollments import termGetEnrollments


## Local Path Variables
baseLogPath = f"{PFAbsolutePath}Logs\\{scriptName}\\"
baseLocalInputPath = f"{PFAbsolutePath}Canvas Resources\\"  ## This is only the base path as the real path requires the requested term
baseLocalOutputPath = f"{PFAbsolutePath}Canvas Resources\\"
configPath = f"{PFAbsolutePath}Configs TLC\\"

## External Path Variables

## Define a variable to hold the base external input path and output path 
baseExternalInputPath = None ## Where the sis input files are stored
baseExternalOutputPath = None ## Where the output files are stored

## Open Base_External_Paths.json from the config path and get the baseExternalInputPath and baseExternalOutputPath values
with open (f"{configPath}Base_External_Paths.json", "r") as file:
    fileJson = json.load(file)
    baseExternalInputPath = fileJson["baseExternalInputPath"]
    baseExternalOutputPath = fileJson["baseIeDepartmentDataExternalOutputPath"]

## If the base log path doesn't already exist, create it
if not (os.path.exists(baseLogPath)):
    os.makedirs(baseLogPath, mode=0o777, exist_ok=False)

## If the output path doesn't already exist, create it
if not (os.path.exists(baseLocalOutputPath)):
    os.makedirs(baseLocalOutputPath, mode=0o777, exist_ok=False)

## Final length of relative Path
relPathLen = len(PFRelativePath)

## Canvas Instance Url
CoreCanvasAPIUrl = None
## Open the Core_Canvas_Url.txt from the config path
with open (f"{configPath}Core_Canvas_Url.txt", "r") as file:
    CoreCanvasAPIUrl = file.readlines()[0]

## If the script is run as main the folder with the access token is in the parent directory
canvasAccessToken = ""

## Open and retrieve the Canvas Access Token
with open (f"{configPath}\Canvas_Access_Token.txt", "r") as file:
    canvasAccessToken = file.readlines()[0]

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

## The variable below holds a set of the functions that have had errors. This enables the except function to only send
## an error email the first time the function triggeres an error
setOfFunctionsWithErrors = set()

## This function handles function errors
def error_handler (p1_ErrorLocation, p1_ErrorInfo, sendOnce = True):
    functionName = "except"
    logger.error (f"     \nA script error occured while running {p1_ErrorLocation}. " +
                     f"Error: {str(p1_ErrorInfo)}")
    ## If the function with the error has not already been processed send an email alert
    if (p1_ErrorLocation not in setOfFunctionsWithErrors):
        errorEmailApi.sendEmailError(p2_ScriptName = scriptName, p2_ScriptPurpose = scriptPurpose, 
                                     p2_ExternalRequirements = externalRequirements, 
                                     p2_ErrorLocation = p1_ErrorLocation, p2_ErrorInfo = f"{p1_ErrorInfo}: \n\n {traceback.format_exc()}")
        setOfFunctionsWithErrors.add(p1_ErrorLocation)
        ## Note that an error email was sent
        logger.error (f"     \nError Email Sent")
    ## Otherwise log the fact that an error email as already been sent
    else:
        logger.error (f"     \nError email already sent")

## This function checks whether each outcome course has outcome data of the required type
def termCreateOutcomeComplianceReport(
        p3_inputTerm
        , p2_schoolYear
        , p2_uniqueOutcomeInfoDictOfDicts
        , p2_outcomeResultDF
        , p2_activeCanvasOutcomeCoursesDf
        , p2_accountInfoDF
        , p1_targetAccountDataDict
        , p2_termEnrollmentDf
        ):
    functionName = "Term Outcome Results Info"

    try:

        ## If the p1_combinedTermoutcomeResultDF is not empty
        if not p2_outcomeResultDF.empty:
            
            ## Define a dict to hold the outcome result report
            outcomeResultReportDict = {
                "Term_ID" : []
                , "School_Year" : []
                , "Term_Year" : []
                , "Course_name" : []
                , "Course_code" : []
                , "Course_section" : []
                , "Course_Section_ID" : []
                , "Instructor_Name_List" : []
                , "Number_of_students" : []
                , "Canvas_Account_id" : []
                , "Account_Canvas_ID" : []
                , "College" : []
                , "Discpline" : []
                , "Department" : []
                , "Outcome_Area": []
                , "Outcome_Title" : []
                , "Outcome_Name" : []
                , "Outcome_Parent" : []
                , "Outcome_Root" : []
                , "Outcome_Id" : []
                , "Outcome_Version" : []
                , "Vendor_Guid": []
                , "Number_of_Students_With_Submission_Records_for_Outcome_Assignments" : []
                , "Students_Missing_Submission_Records_for_Outcome_Assignments" : []
                , "Number of Students assessed" : []
                , "Assessment_Status" : []
                }

            ## For each course in the p1_targetDesignatorActiveCanvasOutcomeCoursesDf
            for index, course in p2_activeCanvasOutcomeCoursesDf.iterrows():

                ## If the courseDict's course_sis_id has 3400 in it
                #if "EDUC3750" in course["Course_sis_id"]:

                    ## Define a target course sis id
                    targetCourseSisId = course["Course_sis_id"]

                    ## If there is a non nan Parent_Course_sis_id
                    if not pd.isna(course["Parent_Course_sis_id"]):

                        ## Set the target course sis id to the Parent_Course_sis_id
                        targetCourseSisId = course["Parent_Course_sis_id"]

                    ## Make a course info dict
                    courseInfoDict = {"Term_ID" : p3_inputTerm
                                      , "School_Year" : p2_schoolYear
                                      , "Term_Year" : (p3_inputTerm[2:5] 
                                                       if "F" in p3_inputTerm 
                                                       else int(p3_inputTerm[2:5]) + 1
                                                       )
                                      , "Course_name" : course["Course_name"]
                                      , "Course_code" : course["Course_sis_id"].split("_")[1]
                                      , "Course_section" : course["Course_sis_id"].split("_")[2]
                                      , "Course Section ID" : course["Section_id"]
                                      , "Instructor_Name_List" : ", ".join([course[instructorColumn] 
                                                                  for instructorColumn in course.index.tolist() 
                                                                  if ("Instructor" in instructorColumn
                                                                      and "name" in instructorColumn
                                                                      and (course[instructorColumn] != ""
                                                                           and str(course[instructorColumn]) != "nan"
                                                                           )
                                                                      )
                                                                  ])
                                      , "Number_of_students" : course["Number_of_students"]
                                      , "Canvas_Account_id" : course["Canvas_Account_id"]
                                      , "Account_Canvas_ID": p2_accountInfoDF.loc[
                                          p2_accountInfoDF["canvas_account_id"] == course["Canvas_Account_id"]
                                          , "canvas_account_id"
                                          ].values[0] if course["Canvas_Account_id"] != 1 else 1
                                      , "College" : ""
                                      , "Discpline" : ""
                                      , "Department" : ""
                                      }
                
                    ###############################################################################
                    ## The following for course code uses the Account_ID column to determine each 
                    ## course's department and college and add them to the college and department 
                    ## list to be added to the outcomeResultReportDF in place of the Account_ID column
                    ###############################################################################

                    ## If the account id is not already in the dict
                    if courseInfoDict["Canvas_Account_id"] not in p1_targetAccountDataDict.keys():

                        ## Determine what the save path for the department would be (which is determined by the parent 
                        ## accounts for the particular sub account)
                        courseDepartmentPath = determineDepartmentSavePath (courseInfoDict["Canvas_Account_id"])

                        ## Split the path by \\ to seperate the college, department, and sub department where applicable
                        courseDepartmentPathSeperated = courseDepartmentPath.split("\\")

                        ## The course college (e.g. College of Business) is always the 0th element of the section 
                        courseInfoDict["College"] = courseDepartmentPathSeperated[0].replace("College of ", "")

                        ## Append the college name to the p1_targetAccountDataDict[courseInfoDict["Canvas_Account_id"]] list as the 0th element
                        p1_targetAccountDataDict[courseInfoDict["Canvas_Account_id"]] = [courseInfoDict["College"]]

                        ## The length of the seperated sections list tells whether the college is made of multiple disciplines
                        ## or if it is all one. This changes where the department name is placed in the section list
                        courseDepartmentPathNumberOfSections = len(courseDepartmentPathSeperated)

                        ## If the length of the section list == 4, the college contains multiple disciplines
                        if courseDepartmentPathNumberOfSections == 4:

                            ## The discpline names (e.g. Music) for college's with multiple disciplines is the 2nd element
                            # of the section list. Append the discpline name to the p1_targetAccountDataDict[courseInfoDict["Canvas_Account_id"]] list as the 1st element
                            courseInfoDict['Discpline'] = courseDepartmentPathSeperated[1]
                            p1_targetAccountDataDict[courseInfoDict["Canvas_Account_id"]].append(courseInfoDict['Discpline'])

                            ## The department (e.g. Undergraduate Music, Undergraduate_NNUO Music) is made by combining the
                            ## 2nd element in the list and the course discpline
                            courseInfoDict["Department"] = f"{courseDepartmentPathSeperated[2]} {courseInfoDict['Discpline']}"

                            ## Append the department name to the p1_targetAccountDataDict[courseInfoDict["Canvas_Account_id"]] list as the 2nd element
                            p1_targetAccountDataDict[courseInfoDict["Canvas_Account_id"]].append(courseInfoDict["Department"])

                        ## If the length of the section list isn't 4
                        else:
                            ## The college name and discipline name is the same
                            courseInfoDict['Discpline'] = courseInfoDict["College"]

                            ## Append the discpline name to the p1_targetAccountDataDict[courseInfoDict["Canvas_Account_id"]] list as the 1st element
                            p1_targetAccountDataDict[courseInfoDict["Canvas_Account_id"]].append(courseInfoDict['Discpline'])

                            ## The department (e.g. Undergraduate Nursing, Undergraduate RN-BSN Nursing) for single
                            ## discipline colleges is made of up the secondary department component (e.g. Undergraduate, Undergraduate RN-BSN)
                            # and the College discipline (e.g. Nursing). 
                            courseInfoDict["Department"] = f"{courseDepartmentPathSeperated[1]} {courseInfoDict['Discpline']}"

                            # Append the department name to the p1_targetAccountDataDict[courseInfoDict["Canvas_Account_id"]] list
                            p1_targetAccountDataDict[courseInfoDict["Canvas_Account_id"]].append(courseInfoDict["Department"])
                        
                    ## If the account id is already in the dict
                    else:
                    
                        ## Set the courseInfoDict["College"] to the 0th element of the p1_targetAccountDataDict[courseInfoDict["Canvas_Account_id"]] list
                        courseInfoDict["College"] = p1_targetAccountDataDict[courseInfoDict["Canvas_Account_id"]][0]
                    
                        ## Set the courseInfoDict['Discpline'] to the 1st element of the p1_targetAccountDataDict[courseInfoDict["Canvas_Account_id"]] list
                        courseInfoDict['Discpline'] = p1_targetAccountDataDict[courseInfoDict["Canvas_Account_id"]][1]
                    
                        ## Set the courseInfoDict["Department"] to the 2nd element of the p1_targetAccountDataDict[courseInfoDict["Canvas_Account_id"]] list
                        courseInfoDict["Department"] = p1_targetAccountDataDict[courseInfoDict["Canvas_Account_id"]][2]

                    ## For each column in the aux term report df that doesn't have area in the title
                    for column in course.index.tolist():

                        ## If the column doesn't have area in the title
                        if "Outcome" in column and "Area" not in column:
                        
                            ## If there is no outcome value in the column
                            if (course[column] == "" 
                            or str(course[column]) == "nan"
                            ):
                            
                                ## Skip the column
                                continue

                            ## Get the outcome info dict
                            outcomeInfoDict = p2_uniqueOutcomeInfoDictOfDicts[course[column]]

                            ## Filter the outcome results to only include the current course, section, and the current outcome
                            targetCourseOutcomeResults = p2_outcomeResultDF[
                                (p2_outcomeResultDF["course name"] == courseInfoDict["Course_name"])
                                & (p2_outcomeResultDF["section id"] == courseInfoDict["Course Section ID"])
                                & (
                                    (p2_outcomeResultDF["learning outcome name"].str.contains(outcomeInfoDict["Outcome_Name"]))
                                    | (p2_outcomeResultDF["learning outcome id"] == outcomeInfoDict["Outcome_Id"])
                                    )
                                ]
                            #targetCourseOutcomeResults = p1_combinedTermoutcomeResultDF[p1_combinedTermoutcomeResultDF["course name"] == course["Course_name"]]

                            ## Find the number of students with an outcome result after filtering to only contain rows that have a "learning outcome rating points" value
                            numOfStuWithOutcomeResults = targetCourseOutcomeResults.dropna(
                                subset=["learning outcome rating points"]
                                )["student id"].nunique()
                        
                            #numOfStuWithOutcomeResults = targetCourseOutcomeResults["student id"].nunique()

                            ## Record the number of students with outcome results
                            outcomeInfoDict["Number of Students assessed"] = numOfStuWithOutcomeResults
                        
                            ## Define lists to track what students submitted to outcome assignments
                            listOfStudentsWithSubmissionRecordsForOutcomeAssignments = []
                            outcomeInfoDict["Students_Missing_Submission_Records_for_Outcome_Assignments"] = []
                        
                            ## If there are any outcome results for the course
                            if numOfStuWithOutcomeResults > 0:

                                ## If the course sis id == FA2024_PHYS1010_1U
                                # if course["Course_sis_id"] == "FA2024_PHYS1010_1U":

                                #     print (1)
                        
                                ## Get the unique assignment ids
                                uniqueAssignmentIds = targetCourseOutcomeResults["assessment id"].unique()
                            
                                ## For each unique assignment id
                                for assignmentId in uniqueAssignmentIds:
                                
                                    ## Define a assignment results api url
                                    assignmentResultsApiUrl = f"{CoreCanvasAPIUrl}courses/sis_course_id:{targetCourseSisId}/assignments/{assignmentId}/submissions"
                                
                                    ## Make a call to the assignment results api
                                    assignmentResultsObject = makeApiCall(
                                        p1_header = {"Authorization": f"Bearer {canvasAccessToken}"}
                                        , p1_apiUrl = assignmentResultsApiUrl
                                        , apiCallType = "get"
                                        , p1_payload = {"include[]": ['user']}
                                        )
                                
                                    ## Define a variable to hold the raw object link list/s
                                    rawAccountOutcomeLinksList = []

                                    ## If the object is actually a list of objects
                                    if isinstance(assignmentResultsObject, list):

                                        ## For each response object in the list
                                        for accountOutcomeLinkObject in assignmentResultsObject:
                    
                                            ## If the response was a 200
                                            if accountOutcomeLinkObject.status_code == 200:

                                                ## Extend the assignmentResultsList with the object
                                                rawAccountOutcomeLinksList.extend(accountOutcomeLinkObject.json())

                                    ## Otherwise there was just one response
                                    else:
                
                                        ## If the status code was a 200
                                        if assignmentResultsObject.status_code == 200:

                                            ## Extend the assignmentResultsList with the object
                                            rawAccountOutcomeLinksList.extend(assignmentResultsObject.json())

                                    ## For each object in the rawAccountOutcomeLinksList
                                    for responseOjbect in rawAccountOutcomeLinksList:

                                        ## Filter the p2_termEnrollmentDf to only contain the current student
                                        targetStudentDf = p2_termEnrollmentDf[p2_termEnrollmentDf["canvas_user_id"] == responseOjbect["user_id"]]

                                        ## Further filter it to contain only the student's enrollment in the current course if it exists
                                        targetStudentCourseDf = targetStudentDf[targetStudentDf["course_id"] == targetCourseSisId]

                                        ## If the targetStudentCourseDf is not empty and the status is active
                                        if not targetStudentCourseDf.empty and targetStudentCourseDf["status"].values[0] != "deleted":
                                    
                                            ## If "user_id" hasn't already been added to the list
                                            ## and they are not the test student (indicated by a null excused field)
                                            ## and their assignment is not missing
                                            if (responseOjbect["user_id"] not in listOfStudentsWithSubmissionRecordsForOutcomeAssignments):
                                        
                                                ## If the excused field niether null nor true and the assignment is not missing
                                                if (str(responseOjbect["excused"]).lower() not in ["null", "true"]
                                                and str(responseOjbect["missing"]).lower() != "true"
                                                ):
                                                
                                                    ## Add it to the list of students with submission records for outcome assignments
                                                    listOfStudentsWithSubmissionRecordsForOutcomeAssignments.append(responseOjbect["user_id"])

                                                    ## If the id was in outcomeInfoDict["Students_Missing_Submission_Records_for_Outcome_Assignments"]
                                                    if (responseOjbect["user_id"] in outcomeInfoDict[
                                                        "Students_Missing_Submission_Records_for_Outcome_Assignments"
                                                        ]):

                                                        ## Remove it from the list of students missing submission records for outcome assignments
                                                        outcomeInfoDict[
                                                            "Students_Missing_Submission_Records_for_Outcome_Assignments"
                                                            ].remove(responseOjbect["user"]["sis_user_id"])
                                                    
                                                ## Else if the missing status is true
                                                ## and the id is not already in outcomeInfoDict["Students_Missing_Submission_Records_for_Outcome_Assignments"]        
                                                elif (
                                                    str(responseOjbect["missing"]).lower() == "true"
                                                    and responseOjbect["user_id"] not in outcomeInfoDict[
                                                        "Students_Missing_Submission_Records_for_Outcome_Assignments"
                                                        ]
                                                    ):
                                                    
                                                    ## Add it to the list of students missing submission records for outcome assignments
                                                    outcomeInfoDict[
                                                        "Students_Missing_Submission_Records_for_Outcome_Assignments"
                                                        ].append(responseOjbect["user"]["sis_user_id"])

                                ## Record the "Number_of_Students_With_Submission_Records_for_Outcome_Assignments"
                                outcomeInfoDict[
                                    "Number_of_Students_With_Submission_Records_for_Outcome_Assignments"
                                    ] = len(listOfStudentsWithSubmissionRecordsForOutcomeAssignments)
                                        
                                ## If numOfStuWithOutcomeResults is equal to or greater than .75 of the the length of the listOfStudentsWithSubmissionRecordsForOutcomeAssignments
                                if numOfStuWithOutcomeResults >= (len(listOfStudentsWithSubmissionRecordsForOutcomeAssignments) * .75):
                            
                                    ## Set the "Assessment_Statusa" column to "Yes"
                                    outcomeInfoDict["Assessment_Status"] = "Assessed"
                            
                                ## Otherwise
                                else:
                            
                                    ## Set the "Assessment_Status" column to "Partial"
                                    outcomeInfoDict["Assessment_Status"] = "Partially Assessed"
                                
                            ## Otherwise
                            else:
                            
                                ## Set the "Assessment_Status" column to "Not Assessed"
                                outcomeInfoDict["Assessment_Status"] = "Not Assessed"
                            
                                ## Set the "Number_of_Students_With_Submission_Records_for_Outcome_Assignments" to 0
                                outcomeInfoDict["Number_of_Students_With_Submission_Records_for_Outcome_Assignments"] = 0

                            ## For each key in the course info dict
                            for key in courseInfoDict.keys():

                                ## Replace the " " in the course info dict key to make the outcome result report dict key
                                outcomeResultReportDictKey = key.replace(" ", "_")
                            
                                ## Append the value to the outcomeResultReportDict
                                outcomeResultReportDict[outcomeResultReportDictKey].append(courseInfoDict[key])
                            
                            ## For each key in the outcome info dict
                            for key in outcomeInfoDict.keys():

                                ## Append the value to the outcomeResultReportDict
                                outcomeResultReportDict[key].append(outcomeInfoDict[key])
                    
            ## Convert the outcomeResultReportDict to a df
            outcomeResultReportDF = pd.DataFrame(outcomeResultReportDict)        

            ## Return the outcomeResultReportDF
            return outcomeResultReportDF

        ## Otherwise
        else:
            
            ## Return an empty dataframe
            return pd.DataFrame()

    except Exception as error:
        error_handler (functionName, p1_ErrorInfo = error)

## This function compiles the outcome scores from the outcome results report into longitudinal data
def termCompileCourseOutcomesScores (p1_CourseDict
                                    , p1_targetTermEnrollmentDf
                                    , p1_targetOutcomeResultsDf
                                    , p1_targetOutcomeResultReportDf
                                    , p1_outcomeResultsDashboardDataDictList
                                    , p2_uniqueOutcomeInfoDictOfDicts
                                    ):
        
    functionName = "Term Compile Outcome Scores"

    try:
    
        # ## If the course's Outcome Area is GE
        # if p1_CourseDict["Outcome Area"] == "GE":

        #     ## Some outcome ratings don't have the intial descriptor word (i.e. exemplary, target, etc.)
        #     ## The following dictionary has snippets of these descriptions and the words that need to be added to them
        #     ## The contents of the following if was generated by Chat GPT as a clean up of my original code
        #     # Define a dictionary to map the first four words to the descriptor word
        #     descriptor_mapping = {
        #         "Design and carry out a research study": "Exemplary",
        #         "Students will evaluate conclusions relative to": "Exemplary",
        #         "Engage in redemptive service to the world": "Exemplary",
        #         "Engage substantially in body stewardship practices": "Exemplary",
        #         "Create personal visual": "Exemplary",
        #         "Generate solutions, using epistemologies": "Exemplary",
        #         "Initiate interactions with individuals from diverse cultures": "Exemplary",
        #         "Engage in the practice and application of the humanities": "Exemplary",
        #         "Construct a persuasive historical interpretation": "Exemplary",
        #         "Deliver a persuasive presentation, written or oral": "Exemplary",
        #         "Analyze their own and others' assumptions": "Exemplary",
        #         "Effectively problem-solve in contexts demanding quantitative literacy": "Exemplary",
        #         "Research multiple sources of information on a topic": "Exemplary",
        #         "In any popular communication (e.g., article, interview, blog, movie, documentary) students will assess": "Target",
        #         "Students will properly analyze data": "Target",
        #         "Investigate the influence of social, cultural, economic, and political institutions": "Target",
        #         "Assess their own health status and develop a plan": "Target",
        #         "Delineate the characteristics that make the historical composers/artists relevant": "Target",
        #         "Apply the diverse ways of knowing to analyze real-world problems": "Target",
        #         "Demonstrate cultural competence by observing, contrasting, comparing, and understanding": "Target",
        #         "Demonstrate knowledge of humanities and the skills": "Target",
        #         "Articulate an adequate historical interpretation, supported by ample historical evidence": "Target",
        #         "Effectively use the English language in writing and speaking": "Target",
        #         "Make an informed, logical judgment of the arguments of others": "Target",
        #         "Critically evaluate data and draw reasonable and appropriately qualified conclusions": "Target",
        #         "Research information in response to critical inquiry and synthesize": "Target",
        #         "In any popular communication (e.g., article, interview, blog, movie, documentary) students will identify": "Minimum",
        #         "Students will gather and analyze accurate data": "Minimum",
        #         "State the guiding theories of two area of the social sciences": "Minimum",
        #         "Describe a healthy lifestyle": "Minimum",
        #         "Identify and define historical characteristics of visual, musical, and literary art": "Minimum",
        #         "Define ways of knowing and identify them within a major discipline": "Minimum",
        #         "Compare and contrast their own culture with another culture": "Minimum",
        #         "Identify the contribution of the humanities in culture": "Minimum",
        #         "Identify a possible historical interpretation, which references some historical evidence": "Minimum",
        #         "Create a clear and coherent written or oral presentation for an audience": "Minimum",
        #         "Develop reasoned and meaningful arguments and positions": "Minimum",
        #         "Use quantitative concepts and methods to interpret data and form plausible arguments": "Minimum",
        #         "Locate, access, and utilize information in a research study": "Minimum"
        #     }
            
        #     ## Iterate through the outcome descriptions and add the descriptor word if applicable
        #     for index, row in p1_targetOutcomeResultsDf.iterrows():

        #         ## Skip rows where the learning outcome rating is NaN
        #         if pd.isna(row["learning outcome rating"]):
        #             continue
                
        #         for descriptor, rating in descriptor_mapping.items():
        #             if row["learning outcome rating"].startswith(descriptor):
        #                 row["learning outcome rating"] = f"{rating}: {row['learning outcome rating']}"
        #                 break  ## Stop searching for descriptor if found

        #     ## Iterate through each row in the filtered term report df
        #     ## The contents of the following for was generated by Chat GPT as a clean up of my original code
        #     for index, row in p1_targetOutcomeResultsDf.iterrows():
                
        #         ## If there is no learning outcome rating points
        #         ## Determine the rating points based on the rating
        #         if pd.isna(row["learning outcome rating points"]):
        #             ## Skip rows with NaN rating
        #             if pd.isna(row["learning outcome rating"]):
        #                 continue
        
        #             ## Determine rating points based on the starting of the rating
        #             rating = row["learning outcome rating"]
        #             rating_points = 0
        #             outcome_mastered = 0
        #             if rating.startswith("Exemplary"):
        #                 rating_points = 3
        #                 outcome_mastered = 1
        #             elif rating.startswith("Target"):
        #                 rating_points = 2
        #                 outcome_mastered = 1
        #             elif rating.startswith("Minimum"):
        #                 rating_points = 1
        #                 outcome_mastered = 0
        #             elif rating.startswith("Outcome_Not Met"):
        #                 rating_points = 0
        #                 outcome_mastered = 0
        
        #             ## Set the learning outcome rating points and mastered
        #             p1_targetOutcomeResultsDf.at[index, "learning outcome rating points"] = rating_points
        #             p1_targetOutcomeResultsDf.at[index, "learning outcome mastered"] = outcome_mastered
                    
        #             ## Fix any outcome name spelling errors
        #             p1_targetOutcomeResultsDf.at[index, "learning outcome name"] = p1_targetOutcomeResultsDf.at[index, "learning outcome name"].replace("S1", "SC1").replace("S2", "SC2").replace("S3", "SC3").replace("H1", "HU1").replace("H3", "HU3").replace("H4", "HU4").replace("H5", "HU5")

        # ## If the course's Outcome Area is I-EDUC
        # elif p1_CourseDict["Outcome Area"] == "I-EDUC":
            
        #     ## Iterate through each row in the filtered term report df
        #     for index, row in p1_targetOutcomeResultsDf.iterrows():
                
        #         ## If there is a rating points value
        #         if pd.isna(row["learning outcome rating points"]):
        #             ## Skip rows with NaN rating as the points for those can't be determined
        #             if pd.isna(row["learning outcome rating"]):
        #                 continue
        
        #             ## Determine rating points based on the starting of the rating
        #             rating = row["learning outcome rating"]
        #             rating_points = 0
        #             outcome_mastered = 0
        #             if rating.startswith("Distinguished"):
        #                 rating_points = 4
        #                 outcome_mastered = 1
        #             elif rating.startswith("Proficient"):
        #                 rating_points = 3
        #                 outcome_mastered = 1
        #             elif rating.startswith("Basic"):
        #                 rating_points = 2
        #                 outcome_mastered = 0
        #             elif rating.startswith("Unsatisfactory"):
        #                 rating_points = 1
        #                 outcome_mastered = 0
        #             elif rating.startswith("No Evidence"):
        #                 rating_points = 0
        #                 outcome_mastered = 0
        
        #             ## Set the learning outcome rating points and mastered
        #             p1_targetOutcomeResultsDf.at[index, "learning outcome rating points"] = rating_points
        #             p1_targetOutcomeResultsDf.at[index, "learning outcome mastered"] = outcome_mastered
                    
        #             ## Create a variable to hold the target unique outcome info dict
        #             targetUniqueOutcomeInfoDict = None

        #             ## For each unique outcome info dict
        #             for uniqueOutcomeInfoDict in p2_uniqueOutcomeInfoDictOfDicts.values():
                        
        #                 ## If the unique outcome info dict's outcome id is equal to the learning outcome id
        #                 if uniqueOutcomeInfoDict["Outcome_Id"] == row["learning outcome id"]:
                            
        #                     ## Set the target unique outcome info dict to the unique outcome info dict
        #                     targetUniqueOutcomeInfoDict = uniqueOutcomeInfoDict
        #                     break

        #             ## Set the name of the outcome to the title paired with the id in the unique outcome info dict
        #             p1_targetOutcomeResultsDf.at[
        #                 index
        #                 , "learning outcome name"
        #                 ] = targetUniqueOutcomeInfoDict[
        #                     "Outcome_Title"
        #                     ]

        ## For each unique student of the course
        for studentID in p1_targetTermEnrollmentDf["user_id"].astype(int).unique():
            
            ## For each column with a value that has outcome and not area in the title of p1_CourseDict
            for column in (column 
                           for column 
                           in p1_CourseDict.index.tolist() 
                           if ("Outcome" in column 
                               and "Area" not in column
                               and (p1_CourseDict[column] != ""
                                        and str(p1_CourseDict[column]) != "nan"
                                        )
                               )
                           ):
                                
                ## Create a Outcome Name Variable
                outcomeFullTitle = p1_CourseDict[column]

                ## Get the outcome id and outcome name for the column from p2_uniqueOutcomeInfoDictOfDicts
                outcomeId = p2_uniqueOutcomeInfoDictOfDicts[
                    outcomeFullTitle
                    ]["Outcome_Id"]
                outcomeName = p2_uniqueOutcomeInfoDictOfDicts[outcomeFullTitle]["Outcome_Name"]

                ## Get the target unique outcome info dict
                targetUniqueOutcomeInfoDict = p2_uniqueOutcomeInfoDictOfDicts[outcomeFullTitle]
                    
                ## Filter the target outcome results df to only contain the current student and outcome 
                ## results that either match the outcome id or contains the name of the outcome
                targetStudentOutcomeResults = p1_targetOutcomeResultsDf[
                    (p1_targetOutcomeResultsDf["student sis id"] == studentID)
                    & (
                        (p1_targetOutcomeResultsDf["learning outcome id"] == outcomeId)
                        | (p1_targetOutcomeResultsDf["learning outcome name"].str.contains(outcomeName))
                        )
                    ]

                ## If any of the targetStudentOutcomeResults's learning outcome name values are not 
                ## equal to the outcomeName and match one of the keys in p2_uniqueOutcomeInfoDictOfDicts
                if (not targetStudentOutcomeResults.empty
                    and targetStudentOutcomeResults[
                        "learning outcome name"
                        ].str.contains(
                            outcomeFullTitle
                            ).any()
                    and not targetStudentOutcomeResults[
                        "learning outcome name"
                        ].str.contains(
                            outcomeFullTitle
                            ).all()
                    and any(
                        targetStudentOutcomeResults[
                            "learning outcome name"
                            ].isin(
                                p2_uniqueOutcomeInfoDictOfDicts.keys()
                                )
                        )
                    ):

                    ## Filter out those that are in p2_uniqueOutcomeInfoDictOfDicts and are not equal to the outcomeFullTitle
                    targetStudentOutcomeResults = targetStudentOutcomeResults[
                        targetStudentOutcomeResults[
                            "learning outcome name"
                            ].isin(
                                p2_uniqueOutcomeInfoDictOfDicts.keys()
                                )
                        & ~targetStudentOutcomeResults[
                            "learning outcome name"
                            ].str.contains(
                                outcomeFullTitle
                                )
                        ]

                ## Define the relevent instructor name
                instructorName = p1_targetOutcomeResultReportDf["Instructor_Name_List"].values[0]
                        
                ## Define the outcome information using the targetUniqueOutcomeInfoDict
                outcomeRoot = targetUniqueOutcomeInfoDict["Outcome_Root"]
                outcomeParent = targetUniqueOutcomeInfoDict["Outcome_Parent"]
                outcomeVersion = targetUniqueOutcomeInfoDict["Outcome_Version"]
                
                ## Define the term and course information using the course sis id
                termYear = p1_CourseDict["Course_sis_id"].split("_")[0]
                courseID = p1_CourseDict["Course_sis_id"]
                courseCode = p1_CourseDict["Course_sis_id"].split("_")[1]
                courseSection = p1_CourseDict["Course_sis_id"].split("_")[2]
                
                ## Initialize outcomeDashboardDataDict with the relavent information from the row and places for future data
                outcomeDashboardDataDict = {"Term_Year" : termYear
                                            , "Course_ID" : courseID
                                            , "Course_code" : courseCode
                                            , "Course_Section" : courseSection
                                            , "Instructor" : instructorName
                                            , "Outcome_Title" : outcomeFullTitle
                                            , "Outcome_Name" : outcomeName
                                            , "Outcome_Id" : outcomeId
                                            , "Outcome_Root" : outcomeRoot
                                            , "Outcome_Parent" : outcomeParent
                                            , "Outcome_Version" : outcomeVersion
                                            , "College" : p1_targetOutcomeResultReportDf["College"].values[0]
                                            , "Discpline" : p1_targetOutcomeResultReportDf["Discpline"].values[0]
                                            , "Department" : p1_targetOutcomeResultReportDf["Department"].values[0]
                                            , "Student Canvas Id" : studentID
                        }
                    
                ## If the targetStudentOutcomeResults is not empty and the student name dict is not empty
                if (not targetStudentOutcomeResults.empty 
                    and not targetStudentOutcomeResults['student name'].isnull().all()
                    and not targetStudentOutcomeResults['learning outcome rating'].isnull().all()
                    ):
                        
                    ## Find the row of p1_targetOutcomeResultReportDf that has the highest rating points
                    highestRatingPointsEntry = targetStudentOutcomeResults[
                        targetStudentOutcomeResults[
                            "learning outcome rating points"
                            ] == targetStudentOutcomeResults[
                                "learning outcome rating points"
                                ].max()
                        ]
                    
                    ## Add the highest rating date points to the outcomeDashboardDataDict
                    outcomeDashboardDataDict.update({
                        "Assignment_Type" : highestRatingPointsEntry["assessment type"].values[0]
                        , "Outcome_Mastered" : highestRatingPointsEntry["learning outcome mastered"].values[0]
                        , "Outcome_rating" : highestRatingPointsEntry["learning outcome rating"].values[0]
                        , "Outcome_rating points" : highestRatingPointsEntry["learning outcome rating points"].values[0]
                        , "Outcome_group title" : highestRatingPointsEntry["learning outcome group title"].values[0]
                    })
                        
                    
                    
                ## Otherwise
                else:

                    ## For each student missing submission records for outcome assignments value
                    for idList in p1_targetOutcomeResultReportDf[
                        "Students_Missing_Submission_Records_for_Outcome_Assignments"
                        ].values:
                        
                        ## If the ID list is not == '[]' and is not a list
                        if idList != '[]' and not isinstance(idList, list):

                            ## If the student id is in the ID list
                            if studentID in ast.literal_eval(idList):

                                ## Record the outcomeDashboardDataDict with the outcome rating points set to 0
                                outcomeDashboardDataDict.update({
                                    "Assignment_Type" : "No Assessment"
                                    , "Outcome_Mastered" : 0
                                    , "Outcome_rating" : "Outcome_Not Met"
                                    , "Outcome_rating points" : 0
                                    , "Outcome_group title" : "No Group"
                                })

                                ## Break the loop
                                break
                            
                        ## Else if the ID list is a list and is not empty
                        elif isinstance(idList, list) and idList != []:

                            ## If the student id is in the ID list
                            if studentID in idList:

                                ## Record the outcomeDashboardDataDict with the outcome rating points set to 0
                                outcomeDashboardDataDict.update({
                                    "Assignment_Type" : "No Assessment"
                                    , "Outcome_Mastered" : 0
                                    , "Outcome_rating" : "Outcome_Not Met"
                                    , "Outcome_rating points" : 0
                                    , "Outcome_group title" : "No Group"
                                })

                                ## Break the loop
                                break
                            
                    ## If there is no Outcome Raiting Points key in the outcomeDashboardDataDict
                    if "Outcome_rating points" not in outcomeDashboardDataDict.keys():
                        
                        ## Record the outcomeDashboardDataDict with the outcome rating points set to -1
                        outcomeDashboardDataDict.update({
                            "Assignment_Type" : "No Assignment"
                            , "Outcome_Mastered" : -1
                            , "Outcome_rating" : "Outcome_Not Met"
                            , "Outcome_rating points" : -1
                            , "Outcome_group title" : "No Group"
                        })



                    # ## If the student id is in the "Students_Missing_Submission_Records_for_Outcome_Assignments" list
                    # if (str(studentID) in p1_targetOutcomeResultReportDf[
                    #     "Students_Missing_Submission_Records_for_Outcome_Assignments"
                    #     ].values.tolist()
                    #     or (str(studentID) in ast.literal_eval(
                    #         p1_targetOutcomeResultReportDf[
                    #             "Students_Missing_Submission_Records_for_Outcome_Assignments"
                    #             ].values.tolist()[0])
                    #         )
                    #     ):    

                    #     ## Record the outcomeDashboardDataDict with the outcome rating points set to 0
                    #     outcomeDashboardDataDict.update({
                    #         "Assignment_Type" : "No Assessment"
                    #         , "Outcome_Mastered" : 0
                    #         , "Outcome_rating" : "Outcome_Not Met"
                    #         , "Outcome_rating points" : 0
                    #         , "Outcome_group title" : "No Group"
                    #     })

                    # ## Otherwise
                    # else:    
                    
                    #     ## Record the outcomeDashboardDataDict with the outcome rating points set to -1
                    #     outcomeDashboardDataDict.update({
                    #         "Assignment_Type" : "No Assignment"
                    #         , "Outcome_Mastered" : -1
                    #         , "Outcome_rating" : "Outcome_Not Met"
                    #         , "Outcome_rating points" : -1
                    #         , "Outcome_group title" : "No Group"
                    #     })

                ## Make the dict df conversion compatible
                modifiedoutcomeDashboardDataDict = {key: [value] if isinstance(value, (str, int, float, np.int32, np.int64)) else value for key, value in outcomeDashboardDataDict.items()}

                ## Convert the created dict to a df
                outcomeDashboardDataDf = pd.DataFrame(modifiedoutcomeDashboardDataDict)
        
                ## Append the df to the outcomeResultsDashboardDataDictList
                p1_outcomeResultsDashboardDataDictList.append(outcomeDashboardDataDf)

                ## logger.info the current length of the outcomeResultsDashboardDataDictList
                logger.info(len(p1_outcomeResultsDashboardDataDictList))
            
        ## End the function
        return

    except Exception as error:
        error_handler (functionName, p1_ErrorInfo = error)

## This function processes the outcome results for a given target designation
def targetDesignatorProcessOutcomeResults(
        p2_inputTerm
        , p1_schoolYear
        , p1_destinationFilePathDict
        , p1_uniqueOutcomeInfoDictOfDicts
        , p1_outcomeResultDF
        , p1_activeCanvasOutcomeCoursesDf
        , p1_accountInfoDF
        , p1_termEnrollmentDf
        ):
    
    functionName = "Term Get Outcome Results"
    
    try:

        ## Define the account data dict
        targetAccountDataDict = {}
        
        ## Define a variable to hold the outcomeResultReportDF
        outcomeResultReportDF = None
        
        ## If the first internal output report file exists
        if os.path.exists(p1_destinationFilePathDict["Internal Output Report File Path and Name"]):
            
            ## Get the last moddifed timestamps
            targetFileTimestamp = os.path.getmtime(p1_destinationFilePathDict["Internal Output Report File Path and Name"])

            ## Convert the timestamps to datetime
            targetFileDateTime = datetime.fromtimestamp(targetFileTimestamp)

            ## Subtract the files' datetime from the current datetime
            targetFileHoursOld = int((currentDate - targetFileDateTime).total_seconds() // 3600)

            ## If both have been less than hour or more since the target was updated
            if (targetFileHoursOld < 3.5):

                ## logger.info that the file is up to date and return
                logger.info (f"     \n {p1_destinationFilePathDict['Internal Output Report File Path and Name']} is up to date")

                ## Read the file into a df
                outcomeResultReportDF = pd.read_excel(p1_destinationFilePathDict["Internal Output Report File Path and Name"], sheet_name = "General")

        ## If the first internal output report wasn't found or is older than an hour
        if outcomeResultReportDF is None:
            
            ## Create the Outcome Compliance Report
            outcomeResultReportDF = termCreateOutcomeComplianceReport(
                p3_inputTerm = p2_inputTerm
                , p2_schoolYear = p1_schoolYear
                , p2_uniqueOutcomeInfoDictOfDicts = p1_uniqueOutcomeInfoDictOfDicts
                , p2_outcomeResultDF = p1_outcomeResultDF
                , p2_activeCanvasOutcomeCoursesDf = p1_activeCanvasOutcomeCoursesDf
                , p2_accountInfoDF = p1_accountInfoDF
                , p1_targetAccountDataDict = targetAccountDataDict
                , p2_termEnrollmentDf = p1_termEnrollmentDf
                )

            ## If a df returned from termCreateOutcomeComplianceReport
            if not outcomeResultReportDF.empty:
            
                ## Save the DF into an excel file
                outcomeResultReportDF.to_excel(p1_destinationFilePathDict["Internal Output Report File Path and Name"], sheet_name = "General", index=False)

                ## If the file already exists in the external output path
                if os.path.exists(p1_destinationFilePathDict["External Output Report File Path and Name"]):
                
                    ## Remove it
                    os.remove(p1_destinationFilePathDict["External Output Report File Path and Name"])

                ## Copy it to the year associated departmental data folder in Institutional Effectiveness's shared drive
                shutil.copy(p1_destinationFilePathDict["Internal Output Report File Path and Name"], p1_destinationFilePathDict["External Output Report File Path and Name"])
                
            ## Otherwise
            else:
                
                ## Log that there are no outcome results to report on
                logger.info("No Outcome Results to Report on. Exiting function")
                
                ## Exit the function
                return

        ## Create a list to hold the outcome results dashboard data
        outcomeResultsDashboardDataDictList = []
            
        ## Create a new column that is a combination of the student id, course id, and outcome id
        p1_outcomeResultDF["student-course-outcome id"] = p1_outcomeResultDF["student id"].astype(str) + p1_outcomeResultDF["course sis id"].astype(str) + p1_outcomeResultDF["learning outcome id"].astype(str)

        ## Create a list of the unique student-course-outcome ids
        #uniqueStudentCourseIds = p1_outcomeResultDF["student-course-outcome id"].unique()

        ## Create a list to hold active threads
        activeThreadsList = []

        ## For each active Canvas Outcome Course
        for index, courseDict in p1_activeCanvasOutcomeCoursesDf.iterrows():

            ## If the courseDict's course_sis_id has 3400 in it
            #if "COMM1210" in courseDict["Course_sis_id"]:
            
                ## Define a target course variables
                targetCourseSisId = courseDict["Course_sis_id"]
                targetCourseName = courseDict["Course_name"]
                targetSectionId = courseDict["Section_id"]

                ## If there is a non nan Parent_Course_sis_id
                if not pd.isna(courseDict["Parent_Course_sis_id"]):

                    ## Set the target course sis id to the Parent_Course_sis_id
                    targetCourseSisId = courseDict["Parent_Course_sis_id"]

                    ## Find the index of the Parent_Course_sis_id in the activeCanvasOutcomeCoursesDf
                    parentCourseIndex = p1_activeCanvasOutcomeCoursesDf[p1_activeCanvasOutcomeCoursesDf["Course_sis_id"] == targetCourseSisId].index[0]

                    ## Set the target course name to the Parent_Course_sis_id's course name
                    targetCourseName = p1_activeCanvasOutcomeCoursesDf.at[parentCourseIndex, "Course_name"]
            
                ## Make a filtered p1_termEnrollmentDf for the target course using the target course sis id and the canvas_section_id
                targetTermEnrollmentDf = p1_termEnrollmentDf[
                    (p1_termEnrollmentDf["course_id"] == targetCourseSisId)
                    & (p1_termEnrollmentDf["canvas_section_id"] == targetSectionId)
                    ]

                ## Make a filtered p1_outcomeResultDF for the current course using the target course sis id amd section id
                targetOutcomeResultsDf = p1_outcomeResultDF[
                    (p1_outcomeResultDF["course sis id"] == targetCourseSisId)
                    & (p1_outcomeResultDF["section id"] == targetSectionId)
                    ]
                
                ## Make a filtered outcomeResultReportDF for the current course
                targetOutcomeResultReportDf = outcomeResultReportDF[outcomeResultReportDF["Course_name"] == targetCourseName]

                ## Create a thread for the current course
                currentThread = threading.Thread(target=termCompileCourseOutcomesScores
                                                    , args=(courseDict
                                                            , targetTermEnrollmentDf
                                                            , targetOutcomeResultsDf
                                                            , targetOutcomeResultReportDf
                                                            , outcomeResultsDashboardDataDictList
                                                            , p1_uniqueOutcomeInfoDictOfDicts
                                                            )
                                                    )
                
                ## Start the thread
                currentThread.start()
                
                ## Append the thread to the active threads list
                activeThreadsList.append(currentThread)

        # ## For each student-course-outcome id in the unique student course ids
        # for studentCourseOutcomeId in uniqueStudentCourseIds:
                
        #     # if studentPlusCourseId == "26511FA2022_BIBL1100_01":

        #         ## Create a filtered term report for the current student-course-outcome id
        #         targetEnrollmentReportDf = p1_outcomeResultDF[p1_outcomeResultDF["student-course-outcome id"] == studentCourseOutcomeId]

        #         ## Create a thread for the current student-course-outcome id
        #         currentThread = threading.Thread(target=termCompileOutcomeScores
        #                                          , args=(
        #                                              targetEnrollmentReportDf
        #                                              , outcomeResultReportDF
        #                                              , outcomeResultsDashboardDataDictList
        #                                              , p1_termEnrollmentDf
        #                                              )
        #                                          )

        #         ## Start the thread
        #         currentThread.start()

        #         ## Append the thread to the active threads list
        #         activeThreadsList.append(currentThread)
            
        ## For each active thread
        for thread in activeThreadsList:

            ## Join the thread
            thread.join()
            

        ## If there are any dashboard dicts in the outcomeResultsDashboardDataDictList
        if outcomeResultsDashboardDataDictList:
                
            ## Combine the dashboard data dicts in the dasboard data list to become one dataframe
            outcomeResultsDashboardDataDF = pd.concat(outcomeResultsDashboardDataDictList)

            ## Save the dataframe in the relavent Canvas Resources folder
            outcomeResultsDashboardDataDF.to_excel(p1_destinationFilePathDict["Second Internal Output Report File Path and Name"], sheet_name = "General", index=False)

            ## If the file already exists in the external output path
            if os.path.exists(p1_destinationFilePathDict["Second External Output Report File Path and Name"]):

                ## Try to remove it
                try: ## Irregular try clause, do not comment out in testing
                    
                    ## Remove it
                    os.remove(p1_destinationFilePathDict["Second External Output Report File Path and Name"])

                ## If there is an error
                except Exception as error: ## Irregular except clause, do not comment out in testing

                    ## Log the error as a warning
                    logger.warning(f"Error: {error} while trying to remove the existing version of {p1_destinationFilePathDict['Second External Output Report File Path and Name']}")
        
            ## Copy it to the year associated departmental data folder in Institutional Effectiveness's shared drive
            shutil.copy(p1_destinationFilePathDict["Second Internal Output Report File Path and Name"], p1_destinationFilePathDict["Second External Output Report File Path and Name"])    
    
    except Exception as error:
        error_handler (functionName, p1_ErrorInfo = error)

## This function processes the outcome results for a given term
def termProcessOutcomeResults(p1_inputTerm
                              , p1_targetDesignator
                              ):
    functionName = "Term Get Outcome Results"

    try:

        ## Define the relevant grad term by replacing FA with GF, SP with GS, and SU with SG
        relevantGradTerm = p1_inputTerm.replace("FA", "GF").replace("SP", "GS").replace("SU", "SG")

        ## Determine and save the term's school year
        schoolYear = None
        if re.search("AF|FA|GF", p1_inputTerm):
            ## Fall terms are the first terms of a new school year so FA20 is part of the 2020-21 school year.
            schoolYear = (century + p1_inputTerm[2:] + "-" + str(int(p1_inputTerm[2:]) + 1))
        elif re.search("SP|GS|AS|SG|SA|SU", p1_inputTerm):
            ## Spring and Summer terms belong in the same school year as the fall terms before them, so SP21 is part of the same 2020-21 school year as FA20.
            schoolYear = (century + str(int(p1_inputTerm[2:]) - 1) + "-" + p1_inputTerm[2:])
            
        ## Define the input paths
        localSchoolYearInputPath = f"{baseLocalOutputPath}{schoolYear}\\"
        localTermInputPath = f"{localSchoolYearInputPath}{p1_inputTerm}\\"
        localGradTermInputPath = f"{localSchoolYearInputPath}{relevantGradTerm}\\"

        ## Determine output paths
        localTermOutputPath = f"{baseLocalOutputPath}{schoolYear}\\{p1_inputTerm}\\"
        termExternalOutputPath = f"{baseExternalOutputPath}{schoolYear}\\{p1_inputTerm}\\"
            
        ## Create a dict of the first and second, internal and external output report file paths
        destinationFilePathDict = {
            "Internal Output Report File Path and Name" : f"{localTermOutputPath}{p1_inputTerm}_{p1_targetDesignator}_Outcome_Results_Course_Data.xlsx"
            , "External Output Report File Path and Name" : f"{termExternalOutputPath}\\{p1_inputTerm}_{p1_targetDesignator}_Outcome_Results_Course_Data.xlsx"
            , "Second Internal Output Report File Path and Name" : f"{localTermOutputPath}{p1_inputTerm}_{p1_targetDesignator}_Outcome_Results_Dashboard_Data.xlsx"
            , "Second External Output Report File Path and Name" : f"{termExternalOutputPath}\\{p1_inputTerm}_{p1_targetDesignator}_Outcome_Results_Dashboard_Data.xlsx"
            }
        
        ## If the internal output report file and the external output report file already exist
        if (os.path.exists(destinationFilePathDict["Internal Output Report File Path and Name"])
        and os.path.exists(destinationFilePathDict["Second Internal Output Report File Path and Name"])
        ):
            
            ## Get both last moddifed timestamps
            targetFileTimestamp = os.path.getmtime(destinationFilePathDict["Internal Output Report File Path and Name"])
            targetFileTimestamp2 = os.path.getmtime(destinationFilePathDict["Second Internal Output Report File Path and Name"])

            ## Convert the timestamps to datetime
            targetFileDateTime = datetime.fromtimestamp(targetFileTimestamp)
            targetFileDateTime2 = datetime.fromtimestamp(targetFileTimestamp2)

            ## Subtract the files' datetime from the current datetime
            targetFileHoursOld = int((currentDate - targetFileDateTime).total_seconds() // 3600)
            targetFileHoursOld2 = int((currentDate - targetFileDateTime2).total_seconds() // 3600)

            ## If both have been less than hour or more since the target was updated
            if (targetFileHoursOld < 3.5 and targetFileHoursOld2 < 1):

                ## logger.info that the file is up to date and return
                logger.info (f"     \n {destinationFilePathDict['Internal Output Report File Path and Name']} and {destinationFilePathDict['Second Internal Output Report File Path and Name']} is up to date")
                return

        ## If the file doesn't exist
        else:
            
            ## If the local output path doesn't already exist, create it
            if not (os.path.exists(localTermOutputPath)):
                os.makedirs(localTermOutputPath, mode=0o777, exist_ok=False)

        ## If the external output path doesn't already exist, create it
        if not (os.path.exists(termExternalOutputPath)):
            os.makedirs(termExternalOutputPath, mode=0o777, exist_ok=False)
                
        ## Retrieve the Automated Outcome Tool Variables excel file as a df    
        automatedOutcomeToolVariablesDf = pd.read_excel(f"{baseExternalInputPath}Internal Tool Files\\Automated Outcome Tool Variables.xlsx")

        ## Get the account name associated with the target designator
        targetAccountName = automatedOutcomeToolVariablesDf.loc[automatedOutcomeToolVariablesDf["Target Designator"] == p1_targetDesignator, "Outcome Location Account Name"].values[0]

        ## Get the path to the current target outcome location
        outcomesCsvPath = termGetOutcomes(p1_inputTerm, targetAccountName, p1_targetDesignator)

        ## Read the outcomes csv into a pandas dataframe
        outcomesCsvDf = pd.DataFrame()

        readOutcomesCsvAttempt = 0

        ## While the outcomesCsvDf is empty
        while outcomesCsvDf.empty and readOutcomesCsvAttempt < 5:

            try: ## Irregular try clause, do not comment out in testing
            
                ## Read the outcomes csv into a pandas dataframe
                outcomesCsvDf = pd.read_csv(outcomesCsvPath, encoding='utf-8')

            except Exception as error: ## Irregular except clause, do not comment out in testing

                ## Log a warning that the outcomes csv could not be read and the error
                logger.warning(f"Outcomes csv could not be read. Attempt {readOutcomesCsvAttempt + 1} of 5")
                
                ## Wait for 5 seconds
                time.sleep(5)

            ## Increment the readRawOutcomesCsvAttempt
            readOutcomesCsvAttempt += 1  
        
        ## Remove the unicode character from the title column
        outcomesCsvDf['title'] = outcomesCsvDf['title'].str.replace('\u200b', '')

        ## Retrieve the active Canvas Outcome Courses excel file as a df, updating it if necessary
        activeCanvasOutcomeCoursesDf = pd.read_excel(termGetActiveOutcomeCourses(p1_inputTerm, p1_targetDesignator))

        # ## Define the path to the the active target designator outcome courses file
        # activeCanvasOutcomeCoursesPath = f"{localTermInputPath}{p1_inputTerm}_{p1_targetDesignator}_Active_Outcome_Courses.xlsx"

        # ## Open the target active target designator outcome courses file
        # activeCanvasOutcomeCoursesDf = pd.read_excel(activeCanvasOutcomeCoursesPath)
        
        ## Open the accounts csv as a df
        accountInfoDF = pd.read_csv(f"{baseLocalInputPath}Canvas_Accounts.csv")

        ## Get the canvas account id associated with the targetAccountName
        targetCanvasAccountId = 1 if p1_targetDesignator == "GE" else ( ## GE outcomes are located at the root account level which is not in the accounts csv
            accountInfoDF.loc[
                accountInfoDF["name"] == targetAccountName
                , "canvas_account_id"
                ].values[0]
            )

        ## Create a Unique Outcome Title : Vendor guids dict
        uniqueOutcomeInfoDictOfDicts = {}

        ## For each column in the activeCanvasOutcomeCoursesDf
        for column in activeCanvasOutcomeCoursesDf.columns:

            ## If the column  has outcome in the title and doesn't have area in the title
            if "Outcome" in column and "Area" not in column:    

                ## Get all the unique outcomes in the column
                uniqueOutcomes = activeCanvasOutcomeCoursesDf[column].unique()
                
                ## For each unique outcome in the column
                for uniqueOutcome in uniqueOutcomes:
                    
                    ## If the unique outcome is not nan
                    if not pd.isna(uniqueOutcome):
                        
                        ## If the unique outcome is not in the uniqueOutcomeVendorGuidDict
                        if uniqueOutcome not in uniqueOutcomeInfoDictOfDicts.keys():
                            
                            ## Add the unique outcome to the dict with a value of an empty list
                            uniqueOutcomeInfoDictOfDicts[uniqueOutcome] = {
                                "Outcome_Area" : p1_targetDesignator
                                , "Outcome_Title" : uniqueOutcome
                                , "Outcome_Name" : (
                                    uniqueOutcome.split("_")[1].split(":")[1] if (
                                        ":" in uniqueOutcome
                                        ) 
                                    else uniqueOutcome.split("_")[1]
                                    )
                                , "Outcome_Parent" : (
                                    uniqueOutcome.split("_")[1][:2] if (
                                        p1_targetDesignator == "GE"
                                        )
                                    else uniqueOutcome.split("_")[1].split("Standard")[0] if (
                                        p1_targetDesignator == "I-EDUC" or p1_targetDesignator == "G-EDUC"
                                        ) 
                                    else uniqueOutcome.split("_")[1].split(":")[0]
                                    )
                                , "Outcome_Root" : (p1_targetDesignator if (
                                        p1_targetDesignator == "GE"
                                        )
                                        else uniqueOutcome.split("_")[1].split(" ")[0].replace(":", "")
                                        )
                                , "Outcome_Version" : uniqueOutcome.split("_")[2]
                                }
                            
        ## Make a list to retain unique outcome keys that don't have a vendor guid
        uniqueOutcomesWithoutVendorGuidList = []
                            
        ## For each unique outcome in the uniqueOutcomeVendorGuidDict
        for uniqueOutcome in uniqueOutcomeInfoDictOfDicts.keys():
            
            ## Find the vendor guid that is associated with the unique outcome within outcomesCsvDf
            vendorGuidValueList = outcomesCsvDf.loc[outcomesCsvDf["title"] == uniqueOutcome, "vendor_guid"].values

            ## If the vendor guid value list is not empty
            if vendorGuidValueList.size > 0:

                ## Set the vendor guid as the first value in the list
                vendorGuid = vendorGuidValueList[0]

            ## Otherwise
            else:

                ## Add the unique outcome to the uniqueOutcomesWithoutVendorGuidList
                uniqueOutcomesWithoutVendorGuidList.append(uniqueOutcome)

                ## Log that the outcome was not found in the outcomes csv and handle the error
                logger.warning(f"Outcome {uniqueOutcome} was not found in the Canvas outcomes csv. Skipping it.")
                error_handler (functionName, p1_ErrorInfo = f"Outcome {uniqueOutcome} was not found in the Canvas outcomes csv. Skipping it.")

                ## Skip the value
                continue
            
            ## Set the vendor guid as the value for the unique outcome in the uniqueOutcomeVendorGuidDict
            uniqueOutcomeInfoDictOfDicts[uniqueOutcome]["Vendor_Guid"] = vendorGuid

        ## For each unique outcome in the uniqueOutcomesWithoutVendorGuidList
        for uniqueOutcome in uniqueOutcomesWithoutVendorGuidList:

            ## Remove it from the uniqueOutcomeVendorGuidDict
            del uniqueOutcomeInfoDictOfDicts[uniqueOutcome]

            ## Replace all instances of it from activeCanvasOutcomeCoursesDf
            activeCanvasOutcomeCoursesDf.replace(uniqueOutcome, "", inplace=True)

        ## Define a api url to get all outcome links for the targetCanvasAccountId
        accountOutcomeLinkApiUrl = f"{CoreCanvasAPIUrl}accounts/{targetCanvasAccountId}/outcome_group_links"

        ## Make an api call to get the outcome links related to the account id
        accountOutcomeLinksObject = makeApiCall(
            p1_header = {"Authorization": f"Bearer {canvasAccessToken}"}
            , p1_apiUrl = accountOutcomeLinkApiUrl
            )

        ## Define a variable to hold the raw object link list/s
        rawAccountOutcomeLinksList = []

        ## If the object is actually a list of objects
        if isinstance(accountOutcomeLinksObject, list):

            ## For each response object in the list
            for accountOutcomeLinkObject in accountOutcomeLinksObject:
                    
                ## If the response was a 200
                if accountOutcomeLinkObject.status_code == 200:

                    ## Extend the accountOutcomeLinksList with the object
                    rawAccountOutcomeLinksList.extend(accountOutcomeLinkObject.json())

        ## Otherwise there was just one response
        else:
                
            ## If the status code was a 200
            if accountOutcomeLinksObject.status_code == 200:

                ## Extend the accountOutcomeLinksList with the object
                rawAccountOutcomeLinksList.extend(accountOutcomeLinksObject.json())

        ## For each object in the rawAccountOutcomeLinksList
        for responseOjbect in rawAccountOutcomeLinksList:
                
            ## Get its outcome title
            outcomeTitle = responseOjbect["outcome"]["title"].replace('\u200b', '')

            ## If that outcome title matches one of the keys in the uniqueOutcomeVendorGuidDict
            if outcomeTitle in uniqueOutcomeInfoDictOfDicts.keys():
                    
                ## Set the canvas id as the value for the outcome title in the uniqueOutcomeInfoDictOfDicts
                uniqueOutcomeInfoDictOfDicts[outcomeTitle]["Outcome_Id"] = responseOjbect["outcome"]["id"]

        ## Get the undergrad outcome results as a df
        undgOutcomeResultsPathAndName = termGetOutcomeResults (p1_inputTerm, targetAccountName, p1_targetDesignator)

        ## Read the undergrad outcome result input path as a df
        undgOutcomeResultDF = pd.read_csv(undgOutcomeResultsPathAndName)

        ## Get the grad term outcome results path and name
        gradOutcomeResultsPathAndName = termGetOutcomeResults (relevantGradTerm, targetAccountName, p1_targetDesignator)

        ## Read the grad outcome result input path as a df
        gradOutcomeResultDF = pd.read_csv(gradOutcomeResultsPathAndName)

        ## Combine the undg and grad outcome result dfs
        targetOutcomeResultsDf = pd.concat([undgOutcomeResultDF, gradOutcomeResultDF], ignore_index=True)
        
        # Fill NA/NaN values with an empty string
        targetOutcomeResultsDf["learning outcome name"].fillna("", inplace=True)

        ## If the course's Outcome Area is GE
        if p1_targetDesignator == "GE":

            ## Some outcome ratings don't have the intial descriptor word (i.e. exemplary, target, etc.)
            ## The following dictionary has snippets of these descriptions and the words that need to be added to them
            ## The contents of the following if was generated by Chat GPT as a clean up of my original code
            # Define a dictionary to map the first four words to the descriptor word
            descriptor_mapping = {
                "Design and carry out a research study": "Exemplary",
                "Students will evaluate conclusions relative to": "Exemplary",
                "Engage in redemptive service to the world": "Exemplary",
                "Engage substantially in body stewardship practices": "Exemplary",
                "Create personal visual": "Exemplary",
                "Generate solutions, using epistemologies": "Exemplary",
                "Initiate interactions with individuals from diverse cultures": "Exemplary",
                "Engage in the practice and application of the humanities": "Exemplary",
                "Construct a persuasive historical interpretation": "Exemplary",
                "Deliver a persuasive presentation, written or oral": "Exemplary",
                "Analyze their own and others' assumptions": "Exemplary",
                "Effectively problem-solve in contexts demanding quantitative literacy": "Exemplary",
                "Research multiple sources of information on a topic": "Exemplary",
                "In any popular communication (e.g., article, interview, blog, movie, documentary) students will assess": "Target",
                "Students will properly analyze data": "Target",
                "Investigate the influence of social, cultural, economic, and political institutions": "Target",
                "Assess their own health status and develop a plan": "Target",
                "Delineate the characteristics that make the historical composers/artists relevant": "Target",
                "Apply the diverse ways of knowing to analyze real-world problems": "Target",
                "Demonstrate cultural competence by observing, contrasting, comparing, and understanding": "Target",
                "Demonstrate knowledge of humanities and the skills": "Target",
                "Articulate an adequate historical interpretation, supported by ample historical evidence": "Target",
                "Effectively use the English language in writing and speaking": "Target",
                "Make an informed, logical judgment of the arguments of others": "Target",
                "Critically evaluate data and draw reasonable and appropriately qualified conclusions": "Target",
                "Research information in response to critical inquiry and synthesize": "Target",
                "In any popular communication (e.g., article, interview, blog, movie, documentary) students will identify": "Minimum",
                "Students will gather and analyze accurate data": "Minimum",
                "State the guiding theories of two area of the social sciences": "Minimum",
                "Describe a healthy lifestyle": "Minimum",
                "Identify and define historical characteristics of visual, musical, and literary art": "Minimum",
                "Define ways of knowing and identify them within a major discipline": "Minimum",
                "Compare and contrast their own culture with another culture": "Minimum",
                "Identify the contribution of the humanities in culture": "Minimum",
                "Identify a possible historical interpretation, which references some historical evidence": "Minimum",
                "Create a clear and coherent written or oral presentation for an audience": "Minimum",
                "Develop reasoned and meaningful arguments and positions": "Minimum",
                "Use quantitative concepts and methods to interpret data and form plausible arguments": "Minimum",
                "Locate, access, and utilize information in a research study": "Minimum"
            }
            
            ## Iterate through the outcome descriptions and add the descriptor word if applicable
            for index, row in targetOutcomeResultsDf.iterrows():

                ## Skip rows where the learning outcome rating is NaN
                if pd.isna(row["learning outcome rating"]):
                    continue
                
                for descriptor, rating in descriptor_mapping.items():
                    if row["learning outcome rating"].startswith(descriptor):
                        row["learning outcome rating"] = f"{rating}: {row['learning outcome rating']}"
                        break  ## Stop searching for descriptor if found

            ## Iterate through each row in the filtered term report df
            ## The contents of the following for was generated by Chat GPT as a clean up of my original code
            for index, row in targetOutcomeResultsDf.iterrows():
                
                ## If there is no learning outcome rating points
                ## Determine the rating points based on the rating
                if pd.isna(row["learning outcome rating points"]):
                    ## Skip rows with NaN rating
                    if pd.isna(row["learning outcome rating"]):
                        continue
        
                    ## Determine rating points based on the starting of the rating
                    rating = row["learning outcome rating"]
                    rating_points = 0
                    outcome_mastered = 0
                    if rating.startswith("Exemplary"):
                        rating_points = 3
                        outcome_mastered = 1
                    elif rating.startswith("Target"):
                        rating_points = 2
                        outcome_mastered = 1
                    elif rating.startswith("Minimum"):
                        rating_points = 1
                        outcome_mastered = 0
                    elif rating.startswith("Outcome_Not Met"):
                        rating_points = 0
                        outcome_mastered = 0
        
                    ## Set the learning outcome rating points and mastered
                    targetOutcomeResultsDf.at[index, "learning outcome rating points"] = rating_points
                    targetOutcomeResultsDf.at[index, "learning outcome mastered"] = outcome_mastered
                    
                    ## Fix any outcome name spelling errors
                    targetOutcomeResultsDf.at[index, "learning outcome name"] = targetOutcomeResultsDf.at[index, "learning outcome name"].replace("S1", "SC1").replace("S2", "SC2").replace("S3", "SC3").replace("H1", "HU1").replace("H3", "HU3").replace("H4", "HU4").replace("H5", "HU5")

        ## If the course's Outcome Area is I-EDUC
        elif p1_targetDesignator == "I-EDUC":
            
            ## Iterate through each row in the filtered term report df
            for index, row in targetOutcomeResultsDf.iterrows():
                
                ## If there is a rating points value
                if pd.isna(row["learning outcome rating points"]):
                    ## Skip rows with NaN rating as the points for those can't be determined
                    if pd.isna(row["learning outcome rating"]):
                        continue
        
                    ## Determine rating points based on the starting of the rating
                    rating = row["learning outcome rating"]
                    rating_points = 0
                    outcome_mastered = 0
                    if rating.startswith("Distinguished"):
                        rating_points = 4
                        outcome_mastered = 1
                    elif rating.startswith("Proficient"):
                        rating_points = 3
                        outcome_mastered = 1
                    elif rating.startswith("Basic"):
                        rating_points = 2
                        outcome_mastered = 0
                    elif rating.startswith("Unsatisfactory"):
                        rating_points = 1
                        outcome_mastered = 0
                    elif rating.startswith("No Evidence"):
                        rating_points = 0
                        outcome_mastered = 0
        
                    ## Set the learning outcome rating points and mastered
                    targetOutcomeResultsDf.at[index, "learning outcome rating points"] = rating_points
                    targetOutcomeResultsDf.at[index, "learning outcome mastered"] = outcome_mastered
                    
                    ## Create a variable to hold the target unique outcome info dict
                    targetUniqueOutcomeInfoDict = None

                    ## For each unique outcome info dict
                    for uniqueOutcomeInfoDict in uniqueOutcomeInfoDictOfDicts.values():
                        
                        ## If the unique outcome info dict's outcome id is equal to the learning outcome id
                        if uniqueOutcomeInfoDict["Outcome_Id"] == row["learning outcome id"]:
                            
                            ## Set the target unique outcome info dict to the unique outcome info dict
                            targetUniqueOutcomeInfoDict = uniqueOutcomeInfoDict
                            break

                    ## Set the name of the outcome to the title paired with the id in the unique outcome info dict
                    targetOutcomeResultsDf.at[
                        index
                        , "learning outcome name"
                        ] = targetUniqueOutcomeInfoDict[
                            "Outcome_Title"
                            ]

        ## Open the term relevant enrollment file as a df
        rawUndgEnrollmentDf = pd.read_csv(termGetEnrollments(p1_inputTerm))

        ## Open the Grad relevent enrollment file as a df
        rawGradEnrollmentDf = pd.read_csv(termGetEnrollments(relevantGradTerm))
        
        ## Define a rawTermEnrollmentDf by combining the rawUndgEnrollmentDf and rawGradEnrollmentDf
        rawTermEnrollmentDf = pd.concat([rawUndgEnrollmentDf, rawGradEnrollmentDf])
        
        ## Filter the rawTermEnrollmentDf to only contain rows with student as the role 
        ## and active or concluded as the status
        termEnrollmentDf = rawTermEnrollmentDf[
            (rawTermEnrollmentDf["role"] == "student") 
            & (
                (rawTermEnrollmentDf["status"] == "active")
                | (rawTermEnrollmentDf["status"] == "concluded")
                )
            ]
        #termEnrollmentDf = rawTermEnrollmentDf[rawTermEnrollmentDf["role"] == "student"]
        
        ## Fill any na values of user id with -1
        termEnrollmentDf.loc[:, "user_id"] = termEnrollmentDf["user_id"].fillna(-1)

        ## Create a thread for the current target designation
        targetDesignatorProcessOutcomeResults (
            p2_inputTerm = p1_inputTerm
             , p1_schoolYear = schoolYear
             , p1_destinationFilePathDict = destinationFilePathDict
             , p1_uniqueOutcomeInfoDictOfDicts = uniqueOutcomeInfoDictOfDicts
             , p1_outcomeResultDF = targetOutcomeResultsDf
             , p1_activeCanvasOutcomeCoursesDf = activeCanvasOutcomeCoursesDf
             , p1_accountInfoDF = accountInfoDF
             , p1_termEnrollmentDf = termEnrollmentDf
             )
        
            

    except Exception as error:
        error_handler (functionName, p1_ErrorInfo = error)
        
## This function opens the CSV file, the save locations json file, sends the information on, and closes both files
def runOutcomeResultsReport(inputTerm, targetDesignator):
    functionName = "Run OutcomeAttachment Report"
    
    try:
    
        ## Run the termOutcomeAttachmentReport function
        termProcessOutcomeResults (p1_inputTerm = inputTerm
                                     , p1_targetDesignator = targetDesignator
                                     )
     
    except Exception as error:
        error_handler (functionName, error)

if __name__ == "__main__":

    ## Start and download the Canvas report
    runOutcomeResultsReport (inputTerm = input("Enter the desired term in four character format (FA20, SU20, SP20): ")
        , targetDesignator = input("Enter the desired target designator (GE, I-EDUC, U-ENGR): ")
        )

    input("Press enter to exit")
