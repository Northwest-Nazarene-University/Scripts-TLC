## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller


from datetime import datetime
import traceback, os, logging, sys, re, shutil, ast, json
import pandas as pd, numpy as np

## Add Script repository to syspath
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

## New resource modules
from Local_Setup import LocalSetup
from TLC_Common import makeApiCall, isFileRecent, flattenApiObjectToJsonList, isPresent, isMissing, runThreadedRows, runUnthreadedRows
from Canvas_Report import CanvasReport
from Common_Configs import coreCanvasApiUrl
from Error_Email import errorEmail

## Create the localsetup variable
localSetup = LocalSetup(datetime.now(), __file__)  ## sets cwd, paths, logs, date parts

from Common_Configs import undgTermsCodesToWordsDict, gradTermsCodesToWordsDict

## Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = os.path.basename(__file__).replace(".py", "")

scriptPurpose = r"""
This script (Outcome_Results_Report) views the active GE course lists and Outcome Results reports for a given term,
and creates a report that shows which courses have outcome data for the required GE outcomes. The script also creates
a csv report to that shows the highest scoring outcome for each student in each course. The script saves the reports
in the Canvas Resources folder and the Institutional Effectiveness shared drive.
"""
externalRequirements = r"""
To function properly this script requires access to the institutions Canvas instance via an Active Canvas Bearer Token
"""

## Setup the error handler
errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)

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
        if isPresent(p2_outcomeResultDF):
            
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
                , "Outcome_group title" : []
                , "Vendor_Guid": []
                , "Number_of_Students_With_Submission_Records_for_Outcome_Assignments" : []
                , "Students_Missing_Submission_Records_for_Outcome_Assignments" : []
                , "Number of Students assessed" : []
                , "Assessment_Status" : []
                }

            ## For each course in the p1_targetDesignatorActiveCanvasOutcomeCoursesDf
            for index, course in p2_activeCanvasOutcomeCoursesDf.iterrows():

                ## If the courseDict's course_sis_id has 3400 in it
                ##if "EDUC3750" in course["Course_sis_id"]:

                    ## Define a target course sis id
                    targetCourseSisId = course["Course_sis_id"]
                    termID = course["Course_sis_id"].split("_")[0][:2] + course["Course_sis_id"].split("_")[0][4:]
                    termYear = course["Course_sis_id"].split("_")[0][4:]

                    ## If there is a non nan Parent_Course_sis_id
                    if not pd.isna(course["Parent_Course_sis_id"]) and course["Parent_Course_sis_id"] not in ["", None]:

                        ## Set the target course sis id to the Parent_Course_sis_id
                        targetCourseSisId = course["Parent_Course_sis_id"]

                    ## Make a course info dict
                    courseInfoDict = {"Term_ID" : termID
                                      , "School_Year" : p2_schoolYear
                                      , "Term_Year" : termYear
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
                
                    #######################################################################
                    ## The following for course code uses the Account_ID column to determine each 
                    ## course's department and college and add them to the college and department 
                    ## list to be added to the outcomeResultReportDF in place of the Account_ID column
                    #######################################################################

                    ## If the account id is not already in the dict
                    if courseInfoDict["Canvas_Account_id"] not in p1_targetAccountDataDict.keys():

                        ## Determine the college/department/discpline directly from account hierarchy
                        accountStructureDict = CanvasReport.determineCollegeDepartmentDiscipline(
                            localSetup,
                            courseInfoDict["Canvas_Account_id"]
                        )

                        ## Skip this account if the structure could not be resolved
                        if accountStructureDict.get("College", "") == "":
                            localSetup.logWarningThreadSafe(f"Could not resolve account structure for account {courseInfoDict['Canvas_Account_id']} -- skipping")
                            continue

                        ## Populate course structure metadata
                        courseInfoDict["College"] = accountStructureDict.get("College", "").replace("College of ", "")
                        courseInfoDict['Discpline'] = accountStructureDict.get("Discpline", "")
                        courseInfoDict["Department"] = accountStructureDict.get("Department", "")

                        ## Cache values for reuse by account id
                        p1_targetAccountDataDict[courseInfoDict["Canvas_Account_id"]] = [
                            courseInfoDict["College"],
                            courseInfoDict['Discpline'],
                            courseInfoDict["Department"],
                        ]
                        
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
                            if str(course[column]).strip() == "" or str(course[column]).lower() == "nan":
                            
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
                            ##targetCourseOutcomeResults = p1_combinedTermoutcomeResultDF[p1_combinedTermoutcomeResultDF["course name"] == course["Course_name"]]

                            ## Find the number of students with an outcome result after filtering to only contain rows that have a "learning outcome rating points" value
                            numOfStuWithOutcomeResults = targetCourseOutcomeResults.dropna(
                                subset=["learning outcome rating points"]
                                )["student id"].nunique()
                        
                            ##numOfStuWithOutcomeResults = targetCourseOutcomeResults["student id"].nunique()

                            ## Record the number of students with outcome results
                            outcomeInfoDict["Number of Students assessed"] = numOfStuWithOutcomeResults
                        
                            ## Define lists to track what students submitted to outcome assignments
                            listOfStudentsWithSubmissionRecordsForOutcomeAssignments = []
                            outcomeInfoDict["Students_Missing_Submission_Records_for_Outcome_Assignments"] = []
                        
                            ## If there are any outcome results for the course
                            if numOfStuWithOutcomeResults > 0:

                                ## If the course sis id == FA2024_PHYS1010_1U
                                ## if course["Course_sis_id"] == "FA2024_PHYS1010_1U":

                                ##     print (1)
                        
                                ## Get the unique assignment ids
                                uniqueAssignmentIds = targetCourseOutcomeResults["assessment id"].unique()
                            
                                ## For each unique assignment id
                                for assignmentId in uniqueAssignmentIds:
                                
                                    ## Define a assignment results api url
                                    assignmentResultsApiUrl = f"{coreCanvasApiUrl}courses/sis_course_id:{targetCourseSisId}/assignments/{assignmentId}/submissions"
                                
                                    ## Make a call to the assignment results api
                                    assignmentResultsObject, assignmentResultsObjectList = makeApiCall(
                                        localSetup
                                        , p1_apiUrl = assignmentResultsApiUrl
                                        , p1_payload = {"include[]": ['user']}
                                        )
                                
                                    ## Flatten all responses into a single list of JSON objects
                                    rawAccountOutcomeLinksList = flattenApiObjectToJsonList(
                                        localSetup,
                                        assignmentResultsObjectList if assignmentResultsObjectList else [assignmentResultsObject],
                                        assignmentResultsApiUrl
                                        )

                                    ## For each object in the rawAccountOutcomeLinksList
                                    for responseOjbect in rawAccountOutcomeLinksList:

                                        ## Filter the p2_termEnrollmentDf to only contain the current student
                                        targetStudentDf = p2_termEnrollmentDf[p2_termEnrollmentDf["canvas_user_id"] == responseOjbect["user_id"]]

                                        ## Further filter it to contain only the student's enrollment in the current course if it exists
                                        targetStudentCourseDf = targetStudentDf[targetStudentDf["course_id"] == targetCourseSisId]

                                        ## If the targetStudentCourseDf is not empty and the status is active
                                        if isPresent(targetStudentCourseDf) and targetStudentCourseDf["status"].values[0] != "deleted":
                                    
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

    except Exception as Error:
        errorHandler.sendError (functionName, Error)

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

        ## If the outcome result report df is empty there is no instructor/college/department
        ## metadata for this course (e.g. account could not be resolved), so skip it
        if isMissing(p1_targetOutcomeResultReportDf):
            localSetup.logWarningThreadSafe(
                f"{functionName}: No outcome result report row found for course "
                f"'{p1_CourseDict.get('Course_sis_id', 'unknown')}' -- skipping"
            )
            return

        ## For each unique student of the course
        #for studentID in p1_targetTermEnrollmentDf["user_id"].astype(int).unique():
        for studentID in p1_targetTermEnrollmentDf["user_id"].unique():
            
            ## For each column with a value that has outcome and not area in the title of p1_CourseDict
            for column in (column 
                           for column in p1_CourseDict.index.tolist() 
                           if ("Outcome" in column 
                               and "Area" not in column
                               and pd.notna(p1_CourseDict[column])
                               and str(p1_CourseDict[column]).strip() not in ("", "nan", "none", "NaN", "None")
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
                if (isPresent(targetStudentOutcomeResults)
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
                outcomeGroupTitle = targetUniqueOutcomeInfoDict["Outcome_group title"]
                
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
                                            , "Outcome_group title" : outcomeGroupTitle
                                            , "College" : p1_targetOutcomeResultReportDf["College"].values[0]
                                            , "Discpline" : p1_targetOutcomeResultReportDf["Discpline"].values[0]
                                            , "Department" : p1_targetOutcomeResultReportDf["Department"].values[0]
                                            , "Student Canvas Id" : studentID
                        }
                    
                ## If the targetStudentOutcomeResults is not empty and the student name dict is not empty
                if (isPresent(targetStudentOutcomeResults) 
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

                    ## Add the highest rating date points to the outcomeDashboardDataDict if a valid entry was found
                    if isPresent(highestRatingPointsEntry):
                        outcomeDashboardDataDict.update({
                            "Assignment_Type" : highestRatingPointsEntry["assessment type"].values[0]
                            , "Outcome_Mastered" : highestRatingPointsEntry["learning outcome mastered"].values[0]
                            , "Outcome_rating" : highestRatingPointsEntry["learning outcome rating"].values[0]
                            , "Outcome_rating points" : highestRatingPointsEntry["learning outcome rating points"].values[0]
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
                                    , "Outcome_rating" : "Outcome Not Met"
                                    , "Outcome_rating points" : 1 if "G-EDUC" in outcomeFullTitle else 0
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
                                    , "Outcome_rating" : "Outcome Not Met"
                                    , "Outcome_rating points" : 1 if "G-EDUC" in outcomeFullTitle else 0
                                })

                                ## Break the loop
                                break
                            
                    ## If there is no Outcome Raiting Points key in the outcomeDashboardDataDict
                    if "Outcome_rating points" not in outcomeDashboardDataDict.keys():
                        
                        ## Record the outcomeDashboardDataDict with the outcome rating points set to -1
                        outcomeDashboardDataDict.update({
                            "Assignment_Type" : "No Assignment"
                            , "Outcome_Mastered" : -1
                            , "Outcome_rating" : "Outcome Not Met"
                            , "Outcome_rating points" : -1
                        })

                ## Make the dict df conversion compatible
                modifiedoutcomeDashboardDataDict = {key: [value] if isinstance(value, (str, int, float, np.int32, np.int64)) else value for key, value in outcomeDashboardDataDict.items()}

                ## Convert the created dict to a df
                outcomeDashboardDataDf = pd.DataFrame(modifiedoutcomeDashboardDataDict)
        
                ## Append the df to the outcomeResultsDashboardDataDictList
                p1_outcomeResultsDashboardDataDictList.append(outcomeDashboardDataDf)

                ## localSetup.logger.info the current length of the outcomeResultsDashboardDataDictList
                localSetup.logInfoThreadSafe(len(p1_outcomeResultsDashboardDataDictList))
            
        ## End the function
        return

    except Exception as Error:
        errorHandler.sendError (functionName, Error)

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
        if isFileRecent(localSetup, p1_destinationFilePathDict["Internal Output Report File Path and Name"]):

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
            if outcomeResultReportDF is not None and isPresent(outcomeResultReportDF):
            
                ## Save the DF into an excel file
                outcomeResultReportDF.to_excel(p1_destinationFilePathDict["Internal Output Report File Path and Name"], sheet_name = "General", index=False)

                ## Copy it to the external resources folder
                shutil.copy2(p1_destinationFilePathDict["Internal Output Report File Path and Name"], p1_destinationFilePathDict["External Output Report File Path and Name"])
                
            ## Otherwise
            else:
                
                ## Log that there are no outcome results to report on
                localSetup.logInfoThreadSafe("No Outcome Results to Report on. Exiting function")
                
                ## Exit the function
                return

        ## Create a list to hold the outcome results dashboard data
        outcomeResultsDashboardDataDictList = []
            
        ## Create a new column that is a combination of the student id, course id, and outcome id
        p1_outcomeResultDF["student-course-outcome id"] = p1_outcomeResultDF["student id"].astype(str) + p1_outcomeResultDF["course sis id"].astype(str) + p1_outcomeResultDF["learning outcome id"].astype(str)

        ## Create a list of the unique student-course-outcome ids
        ##uniqueStudentCourseIds = p1_outcomeResultDF["student-course-outcome id"].unique()

        ## Process each course concurrently
        def _worker(courseDict):
            ## If the courseDict's course_sis_id has 3400 in it
            # if "EDUC3410" not in courseDict["Course_sis_id"]:
                # return

            ## Define a target course variables
            targetCourseSisId = courseDict["Course_sis_id"]
            targetCourseName = courseDict["Course_name"]
            targetSectionId = courseDict["Section_id"]

            ## If there is a non nan Parent_Course_sis_id
            if not pd.isna(courseDict["Parent_Course_sis_id"]) and courseDict["Parent_Course_sis_id"] not in ["", None]:

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

            ## Compile the Course outcome scores
            termCompileCourseOutcomesScores(
                courseDict
                , targetTermEnrollmentDf
                , targetOutcomeResultsDf
                , targetOutcomeResultReportDf
                , outcomeResultsDashboardDataDictList
                , p1_uniqueOutcomeInfoDictOfDicts
            )

        ## Default threaded run
        runThreadedRows(p1_activeCanvasOutcomeCoursesDf, _worker)

        ## For testing, comment out runThreadedRows above and uncomment below
        # runUnthreadedRows(p1_activeCanvasOutcomeCoursesDf, _worker)
            

        ## If there are any dashboard dicts in the outcomeResultsDashboardDataDictList
        if outcomeResultsDashboardDataDictList:
                
            ## Combine the dashboard data dicts in the dasboard data list to become one dataframe
            outcomeResultsDashboardDataDF = pd.concat(outcomeResultsDashboardDataDictList)

            ## Save the dataframe in the relavent Canvas Resources folder
            outcomeResultsDashboardDataDF.to_excel(p1_destinationFilePathDict["Second Internal Output Report File Path and Name"], sheet_name = "General", index=False)

            ## Copy it to the external resources folder
            shutil.copy2(p1_destinationFilePathDict["Second Internal Output Report File Path and Name"], p1_destinationFilePathDict["Second External Output Report File Path and Name"])


    
    except Exception as Error:
        errorHandler.sendError (functionName, Error)

## This function processes the outcome results for a given term
def termProcessOutcomeResults(p1_inputTerm
                              , p1_targetDesignator
                              ):
    functionName = "Term Get Outcome Results"

    try:
        ## Extract term prefix and decade
        termCodePrefix = p1_inputTerm[:2]  ## e.g., "FA", "SP", "SU"
        termWord = undgTermsCodesToWordsDict.get(termCodePrefix, gradTermsCodesToWordsDict.get(termCodePrefix))
        termYear = int(str(localSetup.dateDict["century"]) + p1_inputTerm[2:])

        ## Use LocalSetup to calculate school year dynamically
        schoolYear = localSetup.getSchoolYear(termWord, termYear)

        ## Build lcoal paths  
        designatorLocalOutputPath = localSetup.getTargetDesignatedOutputPath(termWord, termYear, p1_targetDesignator)
            
        ## Create a dict of the first and second, internal and external output report file paths
        destinationFilePathDict = {
            "Internal Output Report File Path and Name" : os.path.join(
                designatorLocalOutputPath, 
                f"{p1_inputTerm}_{p1_targetDesignator}_Outcome_Results_Course_Data.xlsx"
                ),
            "Second Internal Output Report File Path and Name" : os.path.join(
                designatorLocalOutputPath, 
                f"{p1_inputTerm}_{p1_targetDesignator}_Outcome_Results_Dashboard_Data.xlsx"
                ),
            "External Output Report File Path and Name" : os.path.join(
                localSetup.getExternalResourcePath("IE"), 
                schoolYear, 
                p1_inputTerm, 
                f"{p1_inputTerm}_{p1_targetDesignator}_Outcome_Results_Course_Data.xlsx"
                ),
            "Second External Output Report File Path and Name" : os.path.join(
                localSetup.getExternalResourcePath("IE"), 
                schoolYear, 
                p1_inputTerm, 
                f"{p1_inputTerm}_{p1_targetDesignator}_Outcome_Results_Dashboard_Data.xlsx"
                )
            }
        
        ## For each file path in the destinationFilePathDict
        for filePath in destinationFilePathDict.values():
            ## Make sure it exists
            os.makedirs(os.path.dirname(filePath), exist_ok=True)
        
        ## If the internal output report file and the external output report file already exist
        if (isFileRecent(localSetup, destinationFilePathDict["Internal Output Report File Path and Name"]) 
            and isFileRecent(localSetup, destinationFilePathDict["Second Internal Output Report File Path and Name"])
            ):
                ## Return from the function
                return (
                    destinationFilePathDict["Internal Output Report File Path and Name"],
                    destinationFilePathDict["Second Internal Output Report File Path and Name"],
                )
                
        ## Retrieve the Automated Outcome Tool Variables excel file as a df    
        automatedOutcomeToolVariablesDf = pd.read_excel(
            os.path.join(
                localSetup.getExternalResourcePath("TLC"), 
                "Automated Outcome Tool Variables.xlsx"
                )
        )

        ## Get the account name associated with the target designator
        targetAccountName = automatedOutcomeToolVariablesDf.loc[
            automatedOutcomeToolVariablesDf["Target Designator"] == p1_targetDesignator, 
            "Outcome Location Account Name"
            ].values[0]

        ## Read the outcomes csv into a pandas dataframe
        outcomesCsvDf = CanvasReport.getOutcomesDf(localSetup, p1_inputTerm, targetAccountName, p1_targetDesignator)
        
        ## Remove the unicode character from the title column
        outcomesCsvDf['title'] = outcomesCsvDf['title'].str.replace('\u200b', '')

        ## Retrieve the active Canvas Outcome Courses excel file as a df, updating it if necessary
        activeCanvasOutcomeCoursesDf = CanvasReport.getActiveOutcomeCoursesDf(localSetup, p1_inputTerm, p1_targetDesignator)
        
        ## Open the accounts csv as a df
        accountInfoDF = CanvasReport.getAccountsDf(localSetup)
        
        ## Retrieve Automated Outcome Tool Variables
        automatedOutcomeToolVariablesDf = pd.read_excel(
            os.path.join(localSetup.getExternalResourcePath("SIS"), "Internal Tool Files", "Automated Outcome Tool Variables.xlsx")
        )
        targetAccountName = automatedOutcomeToolVariablesDf.loc[
            automatedOutcomeToolVariablesDf["Target Designator"] == p1_targetDesignator,
            "Outcome Location Account Name"
        ].values[0]

        ## Get the canvas account id associated with the targetAccountName
        targetCanvasAccountId = (
            1 if targetAccountName == "NNU" 
            else accountInfoDF.loc[accountInfoDF["name"] == targetAccountName, "canvas_account_id"].values[0]
            )

        ## Resolve structure metadata for the target account (used for reporting logic, not path parsing)
        CanvasReport.determineCollegeDepartmentDiscipline(localSetup, targetCanvasAccountId)

        ## Create a Unique Outcome Title : Vendor guids dict
        uniqueOutcomeInfoDictOfDicts = {}

        ## For each column in the activeCanvasOutcomeCoursesDf
        for column in activeCanvasOutcomeCoursesDf.columns:

            ## If the column  has outcome in the title and doesn't have area in the title
            if "Outcome" in column and "Area" not in column:    

                ## Get all the unique outcomes in the column, but only if they are not empty strings or nan
                rawUniqueOutcomes = activeCanvasOutcomeCoursesDf[column].dropna().astype(str)
                uniqueOutcomes = rawUniqueOutcomes[rawUniqueOutcomes.str.strip() != ""].unique()
                
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

            ## Find the learning outcome group title associated with the vendor guid within outcomesCsvDf
            outcomeParentGuidList = outcomesCsvDf.loc[outcomesCsvDf["title"] == uniqueOutcome, "parent_guids"].values
            
            ## Create variables for the vendor guid and outcome group title
            vendorGuid = ""
            outcomeGroupTitle = ""

            ## If the vendor guid value list is not empty
            if vendorGuidValueList.size > 0:

                ## Set the vendor guid as the first value in the list
                vendorGuid = vendorGuidValueList[0]

                ## Set the parentGuid as the first value in the list
                parentGuid = outcomeParentGuidList[0]

                ## Find the title coresponding to the parent guid
                outcomeGroupTitleList = outcomesCsvDf.loc[outcomesCsvDf["vendor_guid"] == parentGuid, "title"].values

                ## Set the outcome group title as the first value in the list
                outcomeGroupTitle = (
                    targetAccountName if str(parentGuid).strip() == "nan" 
                    else outcomeGroupTitleList[0]
                    )

            ## Otherwise
            else:

                ## Add the unique outcome to the uniqueOutcomesWithoutVendorGuidList
                uniqueOutcomesWithoutVendorGuidList.append(uniqueOutcome)

                ## Log that the outcome was not found in the outcomes csv and handle the error
                localSetup.logWarningThreadSafe(f"Outcome {uniqueOutcome} was not found in the Canvas outcomes csv. Skipping it.")
                errorHandler.sendError (functionName, f"Outcome {uniqueOutcome} was not found in the Canvas outcomes csv. Skipping it.")

                ## Skip the value
                continue
            
            ## Set the vendor guid as the value for the unique outcome in the uniqueOutcomeVendorGuidDict
            uniqueOutcomeInfoDictOfDicts[uniqueOutcome]["Vendor_Guid"] = vendorGuid

            ## Also set the Outcome Group value in uniqueOutcomeInfoDictOfDicts to the learning outcome group title
            uniqueOutcomeInfoDictOfDicts[uniqueOutcome]["Outcome_group title"] = outcomeGroupTitle

        ## For each unique outcome in the uniqueOutcomesWithoutVendorGuidList
        for uniqueOutcome in uniqueOutcomesWithoutVendorGuidList:

            ## Remove it from the uniqueOutcomeVendorGuidDict
            del uniqueOutcomeInfoDictOfDicts[uniqueOutcome]

            ## Replace all instances of it from activeCanvasOutcomeCoursesDf
            activeCanvasOutcomeCoursesDf.replace(uniqueOutcome, "", inplace=True)

        ## Define a api url to get all outcome links for the targetCanvasAccountId
        accountOutcomeLinkApiUrl = f"{coreCanvasApiUrl}accounts/{targetCanvasAccountId}/outcome_group_links"

        ## Make an api call to get the outcome links related to the account id
        accountOutcomeLinksObject, accountOutcomeLinksObjectList = makeApiCall(
            localSetup,
            accountOutcomeLinkApiUrl
            )

        ## Flatten all paginated responses into a single list of JSON objects
        ## If there were multiple pages, use the objectList; otherwise use the single response
        rawAccountOutcomeLinksList = flattenApiObjectToJsonList(
            localSetup,
            accountOutcomeLinksObjectList if accountOutcomeLinksObjectList else [accountOutcomeLinksObject],
            accountOutcomeLinkApiUrl
            )

        ## For each object in the rawAccountOutcomeLinksList
        for responseOjbect in rawAccountOutcomeLinksList:
                
            ## Get its outcome title
            outcomeTitle = responseOjbect["outcome"]["title"].replace('\u200b', '')

            ## If that outcome title matches one of the keys in the uniqueOutcomeVendorGuidDict
            if outcomeTitle in uniqueOutcomeInfoDictOfDicts.keys():
                    
                ## Set the canvas id as the value for the outcome title in the uniqueOutcomeInfoDictOfDicts
                uniqueOutcomeInfoDictOfDicts[outcomeTitle]["Outcome_Id"] = responseOjbect["outcome"]["id"]

        ## Get the outcome results df for the target designator
        targetOutcomeResultsDf = CanvasReport.getOutcomeResultsDf(localSetup, p1_inputTerm, targetAccountName, p1_targetDesignator)

        ## Convert student sis id column to string to match enrollment dataframe types
        targetOutcomeResultsDf["student sis id"] = targetOutcomeResultsDf["student sis id"].astype(str)

        ## Fill NA/NaN values with an empty string
        targetOutcomeResultsDf["learning outcome name"].fillna("", inplace=True)

        ## If the course's Outcome Area is GE
        if p1_targetDesignator == "GE":

            ## Some outcome ratings don't have the intial descriptor word (i.e. exemplary, target, etc.)
            ## The following dictionary has snippets of these descriptions and the words that need to be added to them
            ## The contents of the following if was generated by Chat GPT as a clean up of my original code
            ## Define a dictionary to map the first four words to the descriptor word
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
                "In any popular communication (## e.g., article, interview, blog, movie, documentary) students will assess": "Target",
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
                "In any popular communication (## e.g., article, interview, blog, movie, documentary) students will identify": "Minimum",
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

                    ## If targetUniqueOutcomeInfoDict is still none
                    if targetUniqueOutcomeInfoDict is None:

                        ## Log a warning and continue
                        localSetup.logWarningThreadSafe(f"Could not find unique outcome info dict for outcome id {row['learning outcome id']}. Skipping row.")
                        continue

                    ## Set the name of the outcome to the title paired with the id in the unique outcome info dict
                    targetOutcomeResultsDf.at[
                        index
                        , "learning outcome name"
                        ] = targetUniqueOutcomeInfoDict[
                            "Outcome_Title"
                            ]

        ## Open the target enrollment file as a df
        rawTermEnrollmentDf = CanvasReport.getEnrollmentsDf(localSetup, p1_inputTerm)
        
        ## Filter the rawTermEnrollmentDf to only contain rows with student as the role 
        ## and active or concluded as the status
        termEnrollmentDf = rawTermEnrollmentDf[
            (rawTermEnrollmentDf["role"] == "student")
            ]
        ##termEnrollmentDf = rawTermEnrollmentDf[rawTermEnrollmentDf["role"] == "student"]
        
        ## Fill any na values of user id with -1
        termEnrollmentDf.loc[:, "user_id"] = termEnrollmentDf["user_id"].fillna(-1).astype(str)

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
        
        return (
                    destinationFilePathDict["Internal Output Report File Path and Name"],
                    destinationFilePathDict["Second Internal Output Report File Path and Name"],
                )

    except Exception as Error:
        errorHandler.sendError (functionName, Error)
        
## This function opens the CSV file, the save locations json file, sends the information on, and closes both files
def runOutcomeResultsReport(inputTerm, targetDesignator):
    functionName = "Run OutcomeAttachment Report"
    
    try:
    
        ## Run the termOutcomeAttachmentReport function
        termProcessOutcomeResults (p1_inputTerm = inputTerm
                                     , p1_targetDesignator = targetDesignator
                                     )
     
    except Exception as Error:
        errorHandler.sendError (functionName, Error)

if __name__ == "__main__":

    ## Start and download the Canvas report
    runOutcomeResultsReport (inputTerm = input("Enter the desired term in four character format (FA20, SU20, SP20): ")
        , targetDesignator = input("Enter the desired target designator (GE, I-EDUC, U-ENGR): ")
        )

    input("Press enter to exit")
