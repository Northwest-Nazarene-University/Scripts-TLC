## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

## Import Generic Moduels

from math import fabs
import traceback, os, sys, logging, threading, time, numpy, csv, requests, time, json, pdfkit, re, os, shutil, io, os.path
import base64
from datetime import datetime
from datetime import date
import pandas as pd

## Set working directory
os.chdir(os.path.dirname(__file__))

## Add Script repository to syspath
sys.path.append(f"{os.getcwd()}\ResourceModules")

## Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = "Syllabus Addendum Report"

## Script file identifier
scriptRequirementMissingFolderIdentifier = "Missing_Addendum"

scriptPurpose = r"""
The Course Addendum Checker Script was written by NNU's IDT department to check whether NNU's canavs courses have the static Syllabus Addendum link, make .csv lista of the courses that do not have the link, and store the .csv files under \Employees-Read Only\University Syllabi by college and department.
"""
externalRequirements = r"""
To function properly, this script requires that the static Syllabus Addendum link "https://my.nnu.edu/ics/syllabus_addendum.aspx" (which redirects to the current addendum) be placed in the Canvas Syllabus tab.
"""

## Time variables
currentDate = date.today()
currentYear = currentDate.year
currentMonth = currentDate.month
lastYear = currentYear - 1
nextYear = currentYear + 1
century = str(currentYear)[:2]
decade = str(currentYear)[2:]

##pdfkit (which enables the script to convert html code into .pdf and save it) needs to access wkhtmltopdf.exe which is easier if it has a direct path configured instead of try:ing to find it generally
path_wkhtmltopdf = r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe' ##This is the default location of wkhtmltopdf.exe and would need to be changed if the default installation location for wkhtmltopdf was edited.
config = pdfkit.configuration(wkhtmltopdf=path_wkhtmltopdf)

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

## Import local modules
from Error_Email_API import errorEmailApi
from Download_File import downloadFile

from Create_Sub_Account_Save_Path import determineDepartmentSavePath

## Local Path Variables
baseLogPath = f"{PFAbsolutePath}Logs\\{scriptName}\\"
configPath = f"{PFAbsolutePath}\\Configs TLC\\"
baseLocalInputPath = f"{PFAbsolutePath}Canvas Resources\\"  ## This is only the base path as the real path requires the requested term

## External Path Variables

## Define a variable to hold the output path 
baseExternalOutputPath = None ## Where the syllabus repository will be created and relavent reports stored

## Open Base_External_Paths.json from the config path and get the baseExternalInputPath and baseExternalOutputPath values
with open (f"{configPath}Base_External_Paths.json", "r") as file:
    fileJson = json.load(file)
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

## List of courses that don't need a syllabus. Syllabi for such courses are still gathered but they are not listed in the missing_syllabi.csv
list_of_courses_that_dont_need_syllabi = []
with open(f"{configPath}List_of_uneeded_syllabi.csv", 'r') as tempCsvFile:
    tempcsvReader = csv.DictReader(tempCsvFile, delimiter = ',')
    for row in tempcsvReader:
        list_of_courses_that_dont_need_syllabi.append(row['course_id'])
    tempCsvFile.close()

##Primary API call header and payload
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
 The intended purpose of this function is to make a log of courses without a syllabus addendum
 with the ulimate goal that departments work through and add/fix the syllabi for these courses.
"""
def addendumReportSaveCourseInfo(p1_saveLocation, p1_college_saveLocation, p1_courseName, issue, p1_instructor_name,\
   p1_start_date, p1_end_date, p1_term_id, p1_department, p2_collegeOrDeptMissingRequirement):
    functionName = "addendumReportSaveCourseInfo"

    try:
        ## This function creates a csv file to record when a syllabus is missing or it is uncertain if a syllabus was retrieved.
        ## Create a new missing_syllabi csv for that context the first time that a department is missing a syllabi
        departmentReportLocation = f"{p1_saveLocation}\\{scriptRequirementMissingFolderIdentifier}\\"
        deptmentMissingRequirementCsv = departmentReportLocation + f"{p1_term_id}_{scriptRequirementMissingFolderIdentifier}.csv"
        collegeMissingRequirementCsv = f"{p1_college_saveLocation}\\{p1_term_id}_{scriptRequirementMissingFolderIdentifier}.csv"

        if (deptmentMissingRequirementCsv not in p2_collegeOrDeptMissingRequirement):
            ## If the path doesn't exist, create it
            if not (os.path.exists(departmentReportLocation)):
                os.makedirs(departmentReportLocation, mode=0o777, exist_ok=False)
            ## If it does exist, ensure the last iteration of this department's missing requirement file for the relavent term has been removed
            else:
                if os.path.exists(deptmentMissingRequirementCsv):
                    os.remove(deptmentMissingRequirementCsv)
            ## Create a new department MissingRequirement csv
            with open (deptmentMissingRequirementCsv, "w", newline="") as csvFile_2:
                fieldnames = ["courseName", "Issue", "Required Action", "Instructor Name", "start_date", "end_date", "department"]
                csvWriter = csv.DictWriter(csvFile_2, fieldnames=fieldnames, delimiter = ',')
                csvWriter.writeheader()
                csvWriter.writerow({"courseName": p1_courseName, "Issue": issue, "Instructor Name": p1_instructor_name, "start_date": p1_start_date, "end_date": p1_end_date, "department": p1_department})
                csvFile_2.close()
        ## If it is the second time (or more) in the current run of the script the missing requirement file is added onto
        else:
            with open (deptmentMissingRequirementCsv, "a", newline="") as csvFile_2:
                fieldnames = ["courseName", "Issue", "Required Action", "Instructor Name", "start_date", "end_date", "department"]
                csvWriter = csv.DictWriter(csvFile_2, fieldnames=fieldnames, delimiter = ',')
                csvWriter.writerow({"courseName": p1_courseName, "Issue": issue, "Instructor Name": p1_instructor_name, "start_date": p1_start_date, "end_date": p1_end_date, "department": p1_department})
                csvFile_2.close()
        ## A college level version of the missing requirement is made in addition to the department level file
        if (collegeMissingRequirementCsv not in p2_collegeOrDeptMissingRequirement):
            ## If the path doesn't exist, create it
            if not (os.path.exists(p1_college_saveLocation)):
                os.makedirs(p1_college_saveLocation, mode=0o777, exist_ok=False)
            ## If it does exist, ensure the last iteration of this department's missing requirement file for the relavent term has been removed
            else:
                if os.path.exists(collegeMissingRequirementCsv):
                    os.remove(collegeMissingRequirementCsv)
            ## Create a new college MissingRequirement csv
            with open (collegeMissingRequirementCsv, "w", newline="") as csvFile_2:
                fieldnames = ["courseName", "Issue", "Required Action", "Instructor Name", "start_date", "end_date", "Term", "department"]
                csvWriter = csv.DictWriter(csvFile_2, fieldnames=fieldnames, delimiter = ',')
                csvWriter.writeheader()
                csvWriter.writerow({"courseName": p1_courseName, "Issue": issue, "Instructor Name": p1_instructor_name, "start_date": p1_start_date, "end_date": p1_end_date, "department": p1_department})
                csvFile_2.close()
        ## If it is the second time (or more) in the current run of the script the missing requirement file is added onto    
        else:
            with open (collegeMissingRequirementCsv, "a", newline="") as csvFile_2:
                fieldnames = ["courseName", "Issue", "Required Action", "Instructor Name", "start_date", "end_date", "Term", "department"]
                csvWriter = csv.DictWriter(csvFile_2, fieldnames=fieldnames, delimiter = ',')
                csvWriter.writerow({"courseName": p1_courseName, "Issue": issue, "Instructor Name": p1_instructor_name, "start_date": p1_start_date, "end_date": p1_end_date, "department": p1_department})
                csvFile_2.close()
                
        p2_collegeOrDeptMissingRequirement.append (deptmentMissingRequirementCsv)
        p2_collegeOrDeptMissingRequirement.append (collegeMissingRequirementCsv)

    except Exception as error:
        error_handler (functionName, error)

## This function processes the urls found within the course syllabus tab and makes a note if the course doesn't have the syllabus addendum
## Script Specific
def process_url_matches (p1_all_url_matches):
    functionName = "process_url_matches"

    try:
        ## The following set enable the script to skip previously processed urls
        processed_urls = set()
        
        ## Define Requirement to hold whether or not the requirement is met
        requirementMet = False

        for element in p1_all_url_matches:
            if (element in processed_urls):
                logger.info("Link Skipped: Previously processed URL")
            else:
                ## URL has not been seen before - attempt to process it.
                processed_urls.add(str(element))
                if re.search(r'syllabus_addendum.aspx',  element, re.IGNORECASE):
                    requirementMet = True
        return requirementMet
    except Exception as error:
        error_handler (functionName, error)
        return True

## This function makes a get call to the and processes the course listed on the most recent 
## row of the CSV file and looks for urls within its syllabus tab
## This function processes the rows of the CSV file and sends on the relavent data to process_course
def courseAddendumReport (row, p3_inputTerm, p1_departmentSavePaths, p1_CollegeOrDeptMissingRequirement, p1_schoolYear):
    functionName = "Course Addendum Report"

    try:
    
        ## Define the variables that will be used to save the course's information
        requirementMet = None
        reportSaveLocation = None
        reportCollegeSaveLocation = None
    
        ## If the row's course name is pd.nan, return
        if pd.isna(row['long_name']):
            return
           
        ## For each row in our CSV file we only pull two columns: course_id, and account_id.

        ## Sample course_id values: FA2021_BIOL2220_01, FA2021_EDUC2110_1L, FA2021_ACCT2060_01
        courseSisId = row['course_id'] 

        ## Sample account_id values: U_HSPS, U_LLIT, U_MUSI_APP
        courseAccountId = row['account_id']  

        ## Replace unsaveable characters in the course name with spaces
        courseName = row['short_name'].replace("<", " ").replace(">", " ").replace(":", " ").replace('"', " ")\
            .replace("/", " ").replace("\\", " ").replace("|", " ").replace("?", " ").replace("*", " ") 

        ## Begin a new course entry: in the log
        logger.info("\n     Course: " + courseSisId)
            
        ## Create the URL the API call will be made to
        course_API_url = CoreCanvasAPIUrl + "courses/sis_course_id:" + courseSisId
                
        ## Make the API call and save the result as course_object
        course_object = requests.get(course_API_url, headers = header, params = payload)
                
        ## If the API status code is anything other than 200 it is an error, so log it and skip
        if (course_object.status_code != 200):
            logger.info(f"\n     {courseName} Error: {str(course_object.status_code)}" \
                + f"\n{course_API_url})" \
                + f"\n{course_object.url}")
        else:

            ## Save the primary body of information retrieved by the API call as course_text_jsonString 
            course_text_jsonString = course_object.text
        
            ## Convert the json body of information as a Python Dictionary
            course_text_jsonObject = json.loads(course_text_jsonString)
        
            ## From course_text_jsonString, isolate the course's syllabus body and sections
            syllabus_body = course_text_jsonObject["syllabus_body"]
            sections = course_text_jsonObject["sections"]
        
            ## Define empty variables to hold the department and college specific save paths 
            courseDepartmentPath = None
            courseCollegePath = None

            ## Check whether the courseAccountId is listed in the departmentSavePaths dict. If it is, 
            ## retrieve the department's associated filepath (example: /College of Natural & Applied Sciences/Chemistry:/).
            if courseAccountId in p1_departmentSavePaths:
                courseDepartmentPath = p1_departmentSavePaths[courseAccountId]

            ## Otherwise create use the Canvas sub-account structure above the course to create the department's filepath 
            else:
                ## If the course account ID is blank, set the course department path to the Misc Folder
                if pd.isna(courseAccountId):
                    courseDepartmentPath = "Misc\\Uncategorized\\"

                else:
                    courseDepartmentPath = p1_departmentSavePaths[courseAccountId] = determineDepartmentSavePath \
                        (courseAccountId = courseAccountId)

            ## If the determined path has the manually created courses parent account name in it, skip the course
            if "Manually-Created Courses" in courseDepartmentPath:
                logger.info(f"\n     {courseName} Skipped: Manually created course so no need for a syllabi")
                return
            
            ## Isolate the college piece of the department file path and save it as courseCollegePath.
            rawcourseCollegePath = (courseDepartmentPath.rsplit("\\"))[0]
            courseCollegePath = f"\{rawcourseCollegePath}\\"
            
            schoolYearPath = (f"{p1_schoolYear}\\")
        
            ## If the course has its own indivigual start and end date seperate from the term start and end date, save them in case the syllbus is missing or unidentifiable.
            start_date = ""
            end_date = ""
            if course_text_jsonObject["start_at"]:
                start_date = course_text_jsonObject["start_at"]
            if course_text_jsonObject["end_at"]:
                end_date = course_text_jsonObject["end_at"]
        
            ## Save the instructor name in case the syllbus is missing or unidentifiable, but save it as none initially because not all courses have instructors.
            course_teacher_1_name = None
            if (course_text_jsonObject["teachers"]):
                course_teacher_1_name = course_text_jsonObject["teachers"][0]["display_name"]
        
            ## Create the save and college save location
            saveLocation = os.path.abspath(baseExternalOutputPath + courseDepartmentPath + schoolYearPath)
            college_saveLocation = os.path.abspath(baseExternalOutputPath + courseCollegePath + "\College_" + scriptRequirementMissingFolderIdentifier + "\\" + schoolYearPath)
            if not (os.path.exists(saveLocation + "\Other_Course_Files")):
                ## Create the sub-account & department specific directory if it doesn't already exist.
                os.makedirs(saveLocation + "\Other_Course_Files", mode=0o777, exist_ok=False)
                logger.info(str("\n" + saveLocation + "\Other_Course_Files: directories created\n"))
        
            ## If the course doesn't have a syllabus body, skip it and add the relavent info to the Missing_Syllabi.csv file.
            if not (course_text_jsonObject["syllabus_body"]):
                logger.info("\n     Course Skipped: No Syllabus_Body")
                return "", "", False
            elif (course_text_jsonObject['total_students'] == 0):
                logger.info("\n     Course Skipped: No Students")
                return "", "", False
            else:
                ## If the course has a template syllabus body, skip it and add the relavent info to the Missing_Syllabi.csv file.
                ## 323 is the known character length of one version of the syllabus template, and the <span> listed is a known piece of the version of the syllabus template current when this comment was written (11/5/21).
                if (len(syllabus_body) == 323 or re.search(r'<span style="font-size: 36pt;">Replace with Syllabus Content</span>', syllabus_body)):
                    logger.info("\n     Course Skipped: Template Syllabus Body")
                    return "", "", False
                else:
                    ## Find all http and https links. Beginning the search with " helps ensure that only valid urls are found.
                    all_url_matches = re.findall(r'"https?://[^"]+', syllabus_body)

                    contains_requirement = False
                    if (all_url_matches):
                        ## The JSON.syllabus_body contains at least one URL
                        ## Iterate through all the URLs and process them one at a time.
                        contains_requirement = process_url_matches (all_url_matches)
                    else:
                        ## JSON.syllabus_body did not contain any URLs
                        logger.info("\n     \nNo url matches")
                    if (contains_requirement):
                        ## Course contains syllabus addendum link
                        logger.info ("Course contains external addendum link")
                        return (saveLocation + "\\" + p3_inputTerm), (college_saveLocation + "\\" + p3_inputTerm), True
                    else:
                        if not (sections):
                            ## Courses without sections do not need a syllabus addendum
                            return (saveLocation + "\\" + p3_inputTerm), (college_saveLocation + "\\" + p3_inputTerm), True
                        else:
                            addendumReportSaveCourseInfo(saveLocation, college_saveLocation, courseName, "Course doesn't have the most recent addendum link",
                                                         course_teacher_1_name, start_date, end_date, p3_inputTerm, courseAccountId, p1_CollegeOrDeptMissingRequirement)
                        logger.info ("Course does not contain external addendum link")
                        return (saveLocation + "\\" + p3_inputTerm), (college_saveLocation + "\\" + p3_inputTerm), False

    except Exception as error:
        error_handler (functionName, error)

## This function processes the rows of the CSV file and sends on the relavent data to process_course
def termAddendumReport (p2_inputTerm):
    functionName = "Term Addendum Report"

    try:
        ## The collegeOrDeptMissingRequirement list is for the addendumReportSaveCourseInfo function. 
        ## It enables the function to overwrite the previous version of the missing_syllabi file at the beginning and then append all new information after the first overwite.
        collegeOrDeptMissingRequirement = []

        ## Create a list to save the department save pathes determined by the relavent function based
        ## off of the Canvas sub-account structure
        departmentSavePaths = {}

        ## Determine and save the term's school year
        schoolYear = None
        if re.search("AF|FA|GF", p2_inputTerm):
            ## Fall terms are the first terms of a new school year so FA20 is part of the 2020-21 school year.
            schoolYear = (century + p2_inputTerm[2:] + "-" + str(int(p2_inputTerm[2:]) + 1))
        elif re.search("SP|GS|AS|SG|SA|SU", p2_inputTerm):
            ## Spring and Summer terms belong in the same school year as the fall terms before them, so SP21 is part of the same 2020-21 school year as FA20.
            schoolYear = (century + str(int(p2_inputTerm[2:]) - 1) + "-" + p2_inputTerm[2:])

        ## Read the relavent term's courses file into a pandas dataframe
        rawTermCoursesDF = pd.read_csv(f"{baseLocalInputPath}{schoolYear}\\{p2_inputTerm}\\{p2_inputTerm}_Canvas_Courses.csv")

        ## Remove any rows where the course is unpublished or is the chapel course
        termCoursesDF = rawTermCoursesDF[rawTermCoursesDF['status'] != "unpublished"]
        termCoursesDF = termCoursesDF[termCoursesDF['account_id'] != "U_CHPL"]
        termCoursesDF = termCoursesDF.dropna(how='all')
        termCoursesDF = termCoursesDF.dropna(subset=['course_id'])

        ## Apply courseSyllabiReport to each row
        termCoursesDF.apply(courseAddendumReport, args=(p2_inputTerm, departmentSavePaths, collegeOrDeptMissingRequirement, schoolYear), axis=1)

    except Exception as error:
        error_handler (functionName, error)

## This function opens the CSV file, the save locations json file, sends the information on, and closes both files
def runAddendumReport(p1_inputTerm = ""):
    functionName = "Run Addendum Report"
    
    try:
        currentTerm = ""

        ## If a term is not given, determine it off of the current year
        if not p1_inputTerm:
            currentTerm = None
            
            ## January through May is the Spring Term
            if currentMonth >= 1 and currentMonth <= 5:
                currentTerm = f"SP{str(currentYear)[2:]}"

            ## June through August is the Summer Term
            elif currentMonth >= 6 and currentMonth <= 8:
                currentTerm = f"SU{str(currentYear)[2:]}"

            ## The other months (September through December) is the Fall Term
            else:
                currentTerm = f"FA{str(currentYear)[2:]}"

        termAddendumReport (p2_inputTerm = p1_inputTerm)
     
    except Exception as error:
        error_handler (functionName, error)

if __name__ == "__main__":

    ## Define the API Call header using the retreived Canvas Token
    ##header = {'Authorization' : f"Bearer {canvasAccessToken}"}

    ## Start and download the Canvas report
    runAddendumReport (p1_inputTerm = input("Enter the desired term in \
four character format (FA20, SU20, SP20): "))

    input("Press enter to exit")