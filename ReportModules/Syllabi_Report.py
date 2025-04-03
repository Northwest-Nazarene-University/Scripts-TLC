# Author: Bryce Miller - brycezmiller@nnu.edu
# Last Updated by: Bryce Miller

## Import Generic Moduels

import os, sys, logging, csv, requests, json, pdfkit, re, os, shutil, os.path, re, threading, time
from datetime import date
import pandas as pd

## Set working directory
os.chdir(os.path.dirname(__file__))

## Add Script repository to syspath
sys.path.append(f"{os.getcwd()}\ResourceModules")

# Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = "Syllabi Report"

# Script file identifier
scriptRequirementMissingFolderIdentifier = "Missing_Syllabi"

scriptPurpose = r"""
Search active Canvas course's Syllabus tabs for syllabi. Download those that are found and make notes regarding what is retrieved and what is missing.
Downloaded Syllabi are stored in S:\Employees-Read Only\University Syllabi
"""
externalRequirements = r"""
To function properly, this script requires that syllabi be placed in the Canvas Syllabus tab as a word document, pdf, microsoft doc, or as basic text (copied and pasted). 
If the syllabus is in a document, the document needs to have "Syllabus" in the file title (which can only be changed prior to the file being uploaded to Canvas).
"""


# Time variablesff
currentDate = date.today()
currentMonth = currentDate.month
currentYear = currentDate.year
lastYear = currentYear - 1
nextYear = currentYear + 1
century = str(currentYear)[:2]
decade = str(currentYear)[2:]

#pdfkit (which enables the script to convert html code into .pdf and save it) needs to access wkhtmltopdf.exe which is easier if it has a direct path configured instead of :ing to find it generally
path_wkhtmltopdf = r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe' #This is the default location of wkhtmltopdf.exe and would need to be changed if the default installation location for wkhtmltopdf was edited.
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
from Core_Microsoft_Api import downloadSharedMicrosoftFile
from Create_Sub_Account_Save_Path import determineDepartmentSavePath

## Local Path Variables
baseLogPath = f"{PFAbsolutePath}Logs\\{scriptName}\\"
configPath = f"{PFAbsolutePath}\\Configs TLC\\"
baseLocalInputPath = f"{PFAbsolutePath}Canvas Resources\\"

## External Path Variables

## Define a variable to hold the base external input path and output path 
baseExternalOutputPath = None ## Where the output files are stored

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

## This function clears the syllabi folders connected to the relavent terms
def clearRelaventSyllabiFolders(inputTerm):
    functionName = "Clear Relavent Syllabi Folders"
    try:
        targetSchoolYear = None

        ## Define the current school year by whether it is before or during/after september
        if re.search("AF|FA|GF", inputTerm):
            ## Fall terms are the first terms of a new school year so FA20 is part of the 2020-21 school year.
            targetSchoolYear = f"{century}{inputTerm[2:]}-{int(inputTerm[2:]) + 1}"
        if re.search("SP|GS|AS|SG|SA|SU", inputTerm):
            ## Spring and Summer terms belong in the same school year as the fall terms before them, so SP21 is part of the same 2020-21 school year as FA20.
            targetSchoolYear = f"{century}{int(inputTerm[2:]) - 1}-{inputTerm[2:]}"
        
        ## Retrieve a list of the college syllabi folders
        list_of_college_folders = os.listdir(baseExternalOutputPath)

        ## Clear the current year's syllabi in preparation to retrieve their current versions
        for college_folder in list_of_college_folders:
            
            ## Skip anything that is not a college
            if "College" in college_folder or "Academic Services" in college_folder: 
                ## Define the College Folder Path
                college_folder_path = f"{baseExternalOutputPath}{college_folder}\\"
            
                ## Retrieve a dict of of the contents of the college syllabi folders
                college_folder_contents = os.listdir(college_folder_path)

                for firstLvlDepartmentitem in college_folder_contents:
                        
                    ## Skip the college missing syllabi and college missing addendum folders
                    if "College_Missing" not in firstLvlDepartmentitem:

                        ## Define the current department path
                        firstLvlDepartment_path = college_folder_path + firstLvlDepartmentitem + "\\"

                        ## Retrieve a list of the department's syllabi for the current year
                        firstLvlDepartment_folder_contents = os.listdir(firstLvlDepartment_path)

                        ## Skip the department if it doesn't have the current year
                        if targetSchoolYear in firstLvlDepartment_folder_contents:

                            ## Define the department target year path
                            firstLvlDepartmentTargetYearPath = f"{firstLvlDepartment_path}{targetSchoolYear}\\"
                            firstLvlDepartmentTargetYearFolderContents = os.listdir(firstLvlDepartmentTargetYearPath)
                                
                            ## Skip the department if it doesn't have the current year
                            if inputTerm in firstLvlDepartmentTargetYearFolderContents:
                                ## Define the department target term path
                                firstLvlDepartmentTargetTermPath = f"{firstLvlDepartmentTargetYearPath}{inputTerm}\\"

                                ## Retrieve a list of the department's syllabi for the current year
                                firstLvlDepartmentTargetTermFolderContents = os.listdir(firstLvlDepartmentTargetTermPath)

                                ## Clear the syllabi from the college's current year folder
                                ## Skip the Other_course_files folder and Missing Syllabi Folder
                                for item in firstLvlDepartmentTargetTermFolderContents:
                                    if "Other_Course_Files" not in item and "Missing_Syllabi" not in item:
                                        try: ## Irregular try clause, do not comment out in testing
                                            os.remove(firstLvlDepartmentTargetTermPath + item)
                                            logger.info (item + " removed from " + firstLvlDepartmentTargetTermPath)
                                        except Exception as error: ## Irregular except clause, do not comment out in testing
                                            logger.info ("/n" + item + " not deleted due to error: " + str(error))
                                logger.info ("Files cleared from " + firstLvlDepartmentTargetTermPath)

                        ## For non-wholistic colleges go down another folder level each
                        elif "Graduate" in firstLvlDepartment_folder_contents or "Undergraduate" in firstLvlDepartment_folder_contents:
                            
                            for secondLvlDepartmentitem in firstLvlDepartment_folder_contents:

                                ## Define the second level current department path
                                secondLvlDepartmentPath = f"{firstLvlDepartment_path}{secondLvlDepartmentitem}\\"

                                ## Retrieve a list of the second level department's syllabi for the current year
                                secondLvlDepartmentFolderContents = os.listdir(secondLvlDepartmentPath)

                                if targetSchoolYear in secondLvlDepartmentFolderContents:

                                    ## Define the department target year path
                                    secondLvlDepartmentTargetYearPath = f"{secondLvlDepartmentPath}{targetSchoolYear}\\"
                                    secondLvlDepartmentTargetYearFolderContents = os.listdir(secondLvlDepartmentTargetYearPath)
                                
                                    ## Skip the department if it doesn't have the current year
                                    if inputTerm in secondLvlDepartmentTargetYearFolderContents:
                                        ## Define the department target term path
                                        secondtLvlDepartmentTargetTermPath = f"{secondLvlDepartmentTargetYearPath}{inputTerm}\\"

                                        ## Retrieve a list of the department's syllabi for the current year
                                        secondLvlDepartmentTargetTermFolderContents = os.listdir(secondtLvlDepartmentTargetTermPath)

                                        ## Clear the syllabi from the college's current year folder
                                        ## Skip the Other_course_files folder and Missing Syllabi Folder
                                        for item in secondLvlDepartmentTargetTermFolderContents:
                                            if "Other_Course_Files" not in item and "Missing_Syllabi" not in item:
                                                try: ## Irregular try clause, do not comment out in testing
                                                    os.remove(secondtLvlDepartmentTargetTermPath + item)
                                                    logger.info (item + " removed from " + secondtLvlDepartmentTargetTermPath)
                                                except Exception as error: ## Irregular except clause, do not comment out in testing
                                                    logger.info ("/n" + item + " not deleted due to error: " + str(error))
                                        logger.info ("Files cleared from " + secondtLvlDepartmentTargetTermPath)
                                else:

                                    ## Check for the presence of third level departments
                                    for secondlvlFolder in secondLvlDepartmentFolderContents:
                                        
                                        ## All years have - in them, so if a second lvl folder doesn't have - it is a third level department
                                        if "-" not in secondlvlFolder:

                                            ## Define the second level current department path
                                            thirdLvlDepartmentPath = f"{secondLvlDepartmentPath}{secondlvlFolder}\\"

                                            ## Retrieve a list of the second level department's syllabi for the current year
                                            thirdLvlDepartmentFolderContents = os.listdir(thirdLvlDepartmentPath)

                                            if targetSchoolYear in thirdLvlDepartmentFolderContents:

                                                ## Define the department target year path
                                                thirdLvlDepartmentTargetYearPath = f"{thirdLvlDepartmentPath}{targetSchoolYear}\\"
                                                thirdLvlDepartmentTargetYearFolderContents = os.listdir(thirdLvlDepartmentTargetYearPath)
                                
                                                ## Skip the department if it doesn't have the current year
                                                if inputTerm in thirdLvlDepartmentTargetYearFolderContents:
                                                    ## Define the department target term path
                                                    thirdtLvlDepartmentTargetTermPath = f"{thirdLvlDepartmentTargetYearPath}{inputTerm}\\"

                                                    ## Retrieve a list of the department's syllabi for the current year
                                                    thirdLvlDepartmentTargetTermFolderContents = os.listdir(thirdtLvlDepartmentTargetTermPath)

                                                    ## Clear the syllabi from the college's current year folder
                                                    ## Skip the Other_course_files folder and Missing Syllabi Folder
                                                    for item in thirdLvlDepartmentTargetTermFolderContents:
                                                        if "Other_Course_Files" not in item and "Missing_Syllabi" not in item:
                                                            try: ## Irregular try clause, do not comment out in testing
                                                                os.remove(thirdtLvlDepartmentTargetTermPath + item)
                                                                logger.info (item + " removed from " + thirdtLvlDepartmentTargetTermPath)
                                                            except Exception as error: ## Irregular except clause, do not comment out in testing
                                                                logger.info ("/n" + item + " not deleted due to error: " + str(error))
                                                    logger.info ("Files cleared from " + thirdtLvlDepartmentTargetTermPath)
    except Exception as error:
        error_handler (functionName, error)

""" 
 This fuction saves the course ID and other identifiers of the course in question.
 The intended purpose of this function is to make a csv of missing syllabi made up of courses without
 a syllabus or with short syllabi (which generally indicate that the link wasn't named properly)
 with the ulimate goal that all syllabi are gathered because departments are able to find and 
 add/fix the syllabi in the log.
"""
def syllabiReportSaveCourseInfo(p1_save_location, p1_collegeReportLocation, p1_courseName, issue, required_action, \
   p1_instructor_name, p1_start_date, p1_end_date, p1_term_id, p1_department, p2_collegeOrDeptMissingRequirement):
    functionName = "syllabiReportSaveCourseInfo"
    try:
        ## This function creates a csv file to record when the requirement is missing or if it's existence is uncertain.
        ## Create a new missingRequirement csv for that context the first time that a department is missing the requirement
        departmentReportLocation = p1_save_location.replace(p1_term_id, f"{scriptRequirementMissingFolderIdentifier}")
        deptmentMissingRequirementCsv = departmentReportLocation + f"{p1_term_id}_{scriptRequirementMissingFolderIdentifier}.csv"
        collegeMissingRequirementCsv = p1_collegeReportLocation + f"{p1_term_id}_{scriptRequirementMissingFolderIdentifier}.csv"
        
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
                csvWriter.writerow({"courseName": p1_courseName, "Issue": issue, "Required Action": required_action, "Instructor Name": p1_instructor_name, "start_date": p1_start_date, "end_date": p1_end_date, "department": p1_department})
                csvFile_2.close()
        ## If it is the second time (or more) in the current run of the script the missing requirement file is added onto
        else:
            with open (deptmentMissingRequirementCsv, "a", newline="") as csvFile_2:
                fieldnames = ["courseName", "Issue", "Required Action", "Instructor Name", "start_date", "end_date", "department"]
                csvWriter = csv.DictWriter(csvFile_2, fieldnames=fieldnames, delimiter = ',')
                csvWriter.writerow({"courseName": p1_courseName, "Issue": issue, "Required Action": required_action, "Instructor Name": p1_instructor_name, "start_date": p1_start_date, "end_date": p1_end_date, "department": p1_department})
                csvFile_2.close()
        ## A college level version of the missing requirement is made in addition to the department level file
        if (collegeMissingRequirementCsv not in p2_collegeOrDeptMissingRequirement):
            ## If the path doesn't exist, create it
            if not (os.path.exists(p1_collegeReportLocation)):
                os.makedirs(p1_collegeReportLocation, mode=0o777, exist_ok=False)
            ## If it does exist, ensure the last iteration of this department's missing requirement file for the relavent term has been removed
            else:
                if os.path.exists(collegeMissingRequirementCsv):
                    os.remove(collegeMissingRequirementCsv)
            ## Create a new college MissingRequirement csv
            with open (collegeMissingRequirementCsv, "w", newline="") as csvFile_2:
                fieldnames = ["courseName", "Issue", "Required Action", "Instructor Name", "start_date", "end_date", "Term", "department"]
                csvWriter = csv.DictWriter(csvFile_2, fieldnames=fieldnames, delimiter = ',')
                csvWriter.writeheader()
                csvWriter.writerow({"courseName": p1_courseName, "Issue": issue, "Required Action": required_action, "Instructor Name": p1_instructor_name, "start_date": p1_start_date, "end_date": p1_end_date, "department": p1_department})
                csvFile_2.close()
        ## If it is the second time (or more) in the current run of the script the missing requirement file is added onto    
        else:
            with open (collegeMissingRequirementCsv, "a", newline="") as csvFile_2:
                fieldnames = ["courseName", "Issue", "Required Action", "Instructor Name", "start_date", "end_date", "Term", "department"]
                csvWriter = csv.DictWriter(csvFile_2, fieldnames=fieldnames, delimiter = ',')
                csvWriter.writerow({"courseName": p1_courseName, "Issue": issue, "Required Action": required_action, "Instructor Name": p1_instructor_name, "start_date": p1_start_date, "end_date": p1_end_date, "department": p1_department})
                csvFile_2.close()
        return deptmentMissingRequirementCsv, collegeMissingRequirementCsv
    except Exception as error:
        error_handler (functionName, error)
        return "", ""

## This function processes the urls found within the course syllabus tab and downloads course files and microsoft files
def process_url_matches (p1_all_url_matches, p2_courseName, p1_save_location):
    functionName = "process_url_matches"

    try:
        ## The following list and sets enable the tracking of whether a likely syllabus has been
        ## downloaded and ebles the script to skip previously processed urls and course files
        list_of_syllabi_downloaded = []
        processed_urls = set()
        processed_files = set()
                    
        ## Track the number of downloads for the course
        download_order = []
        
        for element in p1_all_url_matches:
            ## Skip previously encountered URLs
            if (element in processed_urls):
                logger.info("Link Skipped: Previously processed URL")
            ## URL has not been seen before - attempt to process it.
            else:
                ## Creating a file_number out of the length of download_order when length is greater than 1 enables the script to differentiate
                ## between the first and second download and ensure that the second doesn't overwrite the first.
                ## If more than one file needs to be downloaded, append a number on to the end to ensure the first file isn't overwritten
                download_number = len(download_order) + 1
                file_number = ""
                if download_number > 1:
                    file_number = "_" + str(download_number)
                logger.info(f"\n     {p2_courseName}: {element}")
                processed_urls.add(str(element))
                ## Check if the URL is to a course file
                course_file = re.findall(r'courses/\d+/files/\d+', element)
                ## Check if the URL is to a onedrive file
                microsoftFile = re.findall(r'nnuedu.sharepoint.com\S+', element)
                ## Check if the URL is the syllabus addendum link
                external_addendum = re.search(r'University_Syllabus_Addendum', element, re.IGNORECASE)
                ## If the URL is to a course file, check the course file number to determine if the file hasn't been seen (there are often two urls to the same file)
                ## Skip previously processed files
                if (course_file):
                    if (course_file[0] in processed_files):
                        logger.info(f"\n     {p2_courseName} Link Skipped: Previously processed course file\
                        {str(course_file[0])}")
                    ## We have not yet downloaded this course file - attempt to download it.
                    else:
                        processed_files.add(str(course_file[0]))
                        course_file_API_url = CoreCanvasAPIUrl + str(course_file[0])
                        file_object = requests.get(course_file_API_url, headers = header)
                        if not (file_object.status_code == 200):
                            ## Unable to fetch course file info from Canvas. API error.
                            logger.info (f"     \n{p2_courseName} Course File Error: {str(file_object.status_code)}\
                                {course_file_API_url}")
                        else:
                            ## Course File info was successfully fetched from Canvas.
                            file_jsonString = file_object.text
                            file_jsonObject = json.loads(file_jsonString)
                            display_name = file_jsonObject["display_name"]
                            mime_class = file_jsonObject["mime_class"]
                            content_type = file_jsonObject["content-type"]
                            course_file_download_url = (file_jsonObject["url"])
                            logger.info(f"\n     {p2_courseName} Course File: " + str(display_name))
                            if ((re.search(r'Addendum' or r'addendum', display_name)) or (re.search(r'image', mime_class))):
                                if re.search(r'Addendum' or r'addendum', display_name):
                                    ## Filename (display_name) identifies this course file as the syllabus addendum. Skip it.
                                    logger.info(f"\n     {p2_courseName} Course File Skipped: Syllabus Addendum")
                                elif re.search(r'image', mime_class):
                                    ## This is an image file. Skip it.
                                    logger.info(f"\n     {p2_courseName} Course File Skipped: {content_type}")
                            else:
                                if (re.search('syll' or 'syllabus' or 'syllabi', display_name, re.IGNORECASE)):
                                    ## Filename indicates that this is a syllabus. Download and put it into the primary department directory.
                                    download_order.append(display_name)
                                    list_of_syllabi_downloaded.append(display_name)
                
                                    file_name = ""
                                    file_extension = ""
                
                                    ## Determine file exstension
                                    if re.search(r'application/vnd.openxmlformats', content_type):
                                        file_extension = ".docx"
                                    elif re.search(r'application/pdf', content_type):
                                        file_extension = ".pdf"
                                    elif re.search(r'application/msword', content_type):
                                        file_extension = ".doc"
                                    else:
                                        ## Look into file handling for if the display_name makes the p1_save_location + file_name longer than Windows will allow.
                                        file_extension = (" %s" % display_name)
                    
                                    file_name = (p2_courseName + file_number + file_extension)
                                    downloadFile(course_file_download_url, p1_save_location + "\\" + file_name, "w")
                                    if ((file_extension != ".pdf") and (len(list_of_syllabi_downloaded) < 1)):
                                        try: ## Irregular try clause, do not comment out in testing
                                            os.remove(p1_save_location + "\\" + p2_courseName + ".pdf")
                                            logger.info (f"     \n{p2_courseName} old .pdf file removed")
                                        except: ## Irregular except clause, do not comment out in testing
                                            logger.info ("no old .pdf file found")
                                    logger.info(f"\n     {p2_courseName} Download: Probable Syllabus")
                                else:
                                    ## File name did not indicate syllabus. Download it anyway, but put it into Other_Course_Files.
                                    download_order.append(display_name)
                                    raw_file_name = p2_courseName + "---" + file_number + "-" + display_name
                                    file_name = raw_file_name.replace(':','-')
                                    downloadFile(course_file_download_url, p1_save_location + "\Other_Course_Files\\" + file_name, "w")
                                    logger.info(f"\n     {p2_courseName} Download: Other Course File")
                elif (external_addendum):
                    ## This URL match is the University_Syllabus_Addendum
                    logger.info(f"\n     {p2_courseName} Link Skipped: Syllabus Addendum")
                elif (microsoftFile):
                    ## This URL match is a OneDrive file.
                    if (microsoftFile[0] in processed_files):
                        logger.info(f"\n     {p2_courseName} Link Skipped: Previously processed One Drive file\
                        \n{str(microsoftFile[0])}")
                    else:
                        ## We have not yet downloaded this microsoft file - attempt to download it.
                        processed_files.add(str(microsoftFile[0]))
                        download_order.append(element)

                        ## Download the microsoft file
                        microsoftFile = downloadSharedMicrosoftFile(p1_microsoftUserName = "lmsservice@nnu.edu"
                                                                    , p1_fileShareUrl = element
                                                                    , p1_downloadSavePath = p1_save_location)
                        
                        ## If a microsoft file was successfuly downloaded
                        if microsoftFile:
                            
                            ## Separate the name from the path
                            microsoftFileName = microsoftFile.split("\\")[-1]

                            ## Look at the downloaded file and move it to the releavent other course files folder if it doesn't have Syllabus in the title
                            if not (re.search('syll' or 'syllabus' or 'syllabi', microsoftFileName, re.IGNORECASE)):
                                microsoftFileNewDestination = os.path.abspath(f"{p1_save_location}Other_Course_Files")
                                if os.path.exists(f"{microsoftFileNewDestination}\\{microsoftFileName}"):
                                    os.remove(f"{microsoftFileNewDestination}\\{microsoftFileName}")
                                shutil.move(microsoftFile, microsoftFileNewDestination)
                                
                            ## Otherwise add it to the list of downloaded syllabi
                            else:
                                list_of_syllabi_downloaded.append(microsoftFile)
                        else:
                            logger.warning(f"\n     {p2_courseName} Link Skipped: Unable to download microsoft file")

                else:
                    ## This URL match is not a recognizable file source. Skip it.
                    logger.info(f"\n     {p2_courseName} Link Skipped: URL is niether course file nor microsoft Doc")
        return list_of_syllabi_downloaded
    except Exception as error:
        error_handler (functionName, f"{error} \nCourse: {p2_courseName}")
        return ""

## This function makes a get call to the course API URL and processes the course listed on the most recent 
## p1_row of the CSV file and looks for urls within the contents syllabus tab
## Script Specific
def courseSyllabiReport (p1_row, p2_inputTerm, p1_departmentSavePaths, p1_CollegeOrDeptMissingRequirement):

    ## Record the function name for error reporting
    functionName = "Course Syllabi Report"

    try:
        
        ## Define the variables that will be used to track the course's information and save paths    
        requirementMet = None
        departmentReportLocation = None
        collegeReportLocation = None
    
        ## If the p1_row's course name is pd.nan, return
        if not pd.notna(p1_row['short_name']):
            return

        ## For each p1_row in our CSV file we only pull three columns, long_name, courseId, and account_id.

        ## Sample course_id values: FA2021_BIOL2220_01, FA2021_EDUC2110_1L, FA2021_ACCT2060_01
        courseSisId = p1_row['course_id'] 

        ## Sample status values: active, deleted, unpublished
        courseAccountId = p1_row['account_id']  

        ## Sample account_id values: U_HSPS, U_LLIT, U_MUSI_APP
        ## Replace unsaveable characters in the course name with spaces
        courseName = p1_row['short_name'].replace("<", " ").replace(">", " ").replace(":", " ").replace('"', " ")\
            .replace("/", " ").replace("\\", " ").replace("|", " ").replace("?", " ").replace("*", " ") 

        # Begin a new course entry: in the log (the --'s are to increase the readability of the log file by adding
        # easy to see seperations between each course entry:
        logger.info("\n     Course: " + courseSisId)
            
        ## Create the URL the API call will be made to
        course_API_url = CoreCanvasAPIUrl + "courses/sis_course_id:" + courseSisId
                
        ## Make the API call and save the result as course_object
        course_object = requests.get(course_API_url, headers = header, params = payload)
                
        ## If the API status code is anything other than 200 it means the call was unsucessful
        ## In such cases log the error and skip the course
        if (course_object.status_code != 200):
            logger.info(f"\n     {courseName} Error: {str(course_object.status_code)}" \
                + f"\n{course_API_url})" \
                + f"\n{course_object.url}")
            requirementMet = True

        ## Successfully fetched course info from Canvas.
        else:

            ## Save the primary body of information retrieved by the API call as courseTextJsonString 
            courseTextJsonString = course_object.text
        
            ## Convert the json body of information as a Python Dictionary
            courseTextDict = json.loads(courseTextJsonString)
        
            ## Skip the course if it doesn't have students
            if (courseTextDict['total_students'] == 0):
                logger.info(f"\n     {courseName} Skipped: No students so no need for a syllabi")
                return
        
            ## From courseTextJsonString, isolate the course's syllabus body
            syllabusBody = courseTextDict["syllabus_body"]
        
            ## Define empty variables to hold the department and college specific save paths 
            courseDepartmentPath = None
            courseCollegePath = None
            school_year_path = None

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

            ## Determine and save the course's school year for file path purposes and in case the syllbus is missing or unidentifiable
            if re.search("AF|FA|GF", p2_inputTerm):
                ## Fall terms are the first terms of a new school year so FA20 is part of the 2020-21 school year.
                school_year = (century + p2_inputTerm[-2:] + "-" + str(int(p2_inputTerm[-2:]) + 1))
                school_year_path = (f"{school_year}\\")
            elif re.search("SP|GS|AS|SG|SA|SU", p2_inputTerm):
                ## Spring and Summer terms belong in the same school year as the fall terms before them, so SP21 is part of the same 2020-21 school year as FA20.
                school_year = (century + str(int(p2_inputTerm[-2:]) - 1) + "-" + p2_inputTerm[-2:])
                school_year_path = (f"{school_year}\\")
        
            ## If the course has its own indivigual start and end date seperate from the term start and end date, save them in case the syllbus is missing or unidentifiable.
            start_date = ""
            end_date = ""
            if courseTextDict["start_at"]:
                start_date = courseTextDict["start_at"]
            if courseTextDict["end_at"]:
                end_date = courseTextDict["end_at"]
        
            ## Save the instructor name in case the syllbus is missing or unidentifiable, but save it as none initially because not all courses have instructors.
            course_teacher_1_name = None
            if (courseTextDict["teachers"]):
                course_teacher_1_name = courseTextDict["teachers"][0]["display_name"]
        
            ## Create save location variables
            save_location = None
            collegeReportLocation = None
        
            ## Save all course related files to the corresponding location found in departmentSavePaths
            save_location = f"{baseExternalOutputPath}{courseDepartmentPath}{school_year_path}{p2_inputTerm}\\"
            collegeReportLocation = f"{baseExternalOutputPath}{courseCollegePath}\\College_{scriptRequirementMissingFolderIdentifier}\\{school_year_path}"
            if not (os.path.exists(save_location + "\Other_Course_Files")):
                ## Create the sub-account & department specific directory if it doesn't already exist.
                os.makedirs(save_location + "\Other_Course_Files", mode=0o777, exist_ok=False)
                logger.info(f"\n     {save_location}\Other_Course_Files: directories created")
        
            ## Make note of whether or not the course is on the list of courses that don't need syllabi
            course_needs_syllabi = True
            for course_code in list_of_courses_that_dont_need_syllabi:
                if course_code in courseName:
                    course_needs_syllabi = False
        
            ## Proceed if the course is not one of the courses that doesn't need a syllabi
                
            ## If the course doesn't have a syllabus body, skip it and add the relavent info to the Missing_Syllabi.csv file.
            if not (syllabusBody):
                if course_needs_syllabi:
                    departmentReportLocation, collegeReportLocation = syllabiReportSaveCourseInfo (p1_save_location = save_location, p1_collegeReportLocation = collegeReportLocation, p1_courseName = courseName, \
                        issue = "Course Skipped: No Syllabus_Body", required_action = "Embed the syllabus file in the Syllabus Tab", \
                        p1_instructor_name = course_teacher_1_name, p1_start_date = start_date, p1_end_date = end_date, p1_term_id = p2_inputTerm, \
                        p1_department = courseAccountId, p2_collegeOrDeptMissingRequirement = p1_CollegeOrDeptMissingRequirement)

                    logger.info(f"\n     {courseName} Course Skipped: No Syllabus_Body")
                    
                    requirementMet = False
                else:
                    logger.info (f"     \n{courseName} Course does not have a syllabus but doesn't need one.")
                    requirementMet = True
            else:
                ## If the course has a template syllabus body, skip it and add the relavent info to the Missing_Syllabi.csv file.
                ## 323 is the known character length of one version of the syllabus template, and the <span> listed is a known piece of the version of the syllabus template current when this comment was written (11/5/21).
                if (len(syllabusBody) == 323 or re.search(r'<span style="font-size: 36pt;">Replace with Syllabus Content</span>', syllabusBody)):
                    if course_needs_syllabi:
                        departmentReportLocation, collegeReportLocation = syllabiReportSaveCourseInfo (p1_save_location = save_location, p1_collegeReportLocation = collegeReportLocation, p1_courseName = courseName, \
                            issue = "Course Skipped: Template Syllabus Body", required_action = "Embed the syllabus file in the Syllabus Tab", \
                            p1_instructor_name = course_teacher_1_name, p1_start_date = start_date, p1_end_date = end_date, p1_term_id = p2_inputTerm, \
                            p1_department = courseAccountId, p2_collegeOrDeptMissingRequirement = p1_CollegeOrDeptMissingRequirement)
                        
                        logger.info(f"\n     {courseName} Course Skipped: Template Syllabus Body")
                        
                        requirementMet = False
                    else:
                        logger.info (f"     \n{courseName} Course doesn't have a syllabus but doesn't need one.")
                        requirementMet = True
                else:
                    ## Find all http and https links. Beginning the search with " helps ensure that only valid urls are found.
                    all_url_matches = re.findall(r'"https?://[^"]+|"/courses/\d+/files/\d+', syllabusBody)

                    ## syllabi_downloaded will contain the names of any downloaded syllabi
                    syllabi_downloaded = []
                
                    if (all_url_matches):
                        ## The JSON.syllabus_body contains at least one URL
                        ## Iterate through all the URLs and process them one at a time.
                        syllabi_downloaded.extend(process_url_matches (all_url_matches, courseName, save_location))
                    else:
                        ## JSON.syllabus_body did not contain any URLs
                        logger.info(f"\n     {courseName} No url matches")
                    if (syllabi_downloaded):
                        ## At least one potential syllabus was found.
                        logger.info(f"\n     {courseName} Syllabi Downloaded: " + str(syllabi_downloaded))
                        requirementMet = True
                    else:
                        ## None of the URL matches (i.e. links in the HTML) were a course or microsoft file containing the word syllabus in the filename
                        ## or there were just no URLs. Either way, save the json.syllabus_body retrieved earlier and convert it to PDF.
                        logger.info(f"\n     {courseName} No known Syllabi downloaded")
                        try: ## Irregular try clause, do not comment out in testing
                            if (len(syllabusBody) < 1500):
                                ## The syllabus body is short. It is probably bogus so make note of it by saving the courseName
                                ## Convert HTML to PDF, download it, and put it into the primary directory
                                pdfkit.from_string("<meta charset='utf-8'>" + courseName + syllabusBody, save_location + "\\" + courseName + ".pdf", configuration=config)
                                if course_needs_syllabi:
                                    departmentReportLocation, collegeReportLocation = syllabiReportSaveCourseInfo (p1_save_location = save_location, p1_collegeReportLocation = collegeReportLocation, p1_courseName = courseName, \
                                        issue = "Short Syllabus Downloaded", required_action = "Check whether a syllabus is embedded on the course's syllabus tab. " \
                                            + "If there is a syllabus, download it, rename it to have syllabus in its file name, and upload " \
                                            + "the new file to replace the old file. If there isn't a syllabus file in the syllabus tab, embed one", \
                                        p1_instructor_name = course_teacher_1_name, p1_start_date = start_date, p1_end_date = end_date, p1_term_id = p2_inputTerm, \
                                        p1_department = courseAccountId, p2_collegeOrDeptMissingRequirement = p1_CollegeOrDeptMissingRequirement)
                                    
                                    logger.info(f"\n     {courseName} Download: Short Syllabus Body to converted pdf")
                                    
                                    requirementMet = False
                                else:
                                    logger.info (f"     \n{courseName} has a short syllabus but doesn't need one.")
                                    requirementMet = True
                            else:
                                ## Convert HTML to PDF and download. Save to Probable_Syllabus.
                                pdfkit.from_string("<meta charset='utf-8'>" + courseName + syllabusBody, save_location + "\\" + courseName + ".pdf", configuration=config)
                                logger.info(f"\n     {courseName} Download: Syllabus Body converted to pdf")
                                requirementMet = True
                                departmentReportLocation = f"{save_location}{scriptRequirementMissingFolderIdentifier}\\"
                                collegeReportLocation = collegeReportLocation
                        except Exception as error: ## Irregular except clause, do not comment out in testing
                            logger.warning ("Saving the syllabus_body as a pdf", f" {error} \nCourse: {courseName}")
            
            ## Track the results and actions of the call to support the collegeOrDeptMissingRequirement list
            if (requirementMet == False):
                if departmentReportLocation not in p1_CollegeOrDeptMissingRequirement:
                    p1_CollegeOrDeptMissingRequirement.append(departmentReportLocation)
                    logger.info (f"     \nMissing Syllabi csv created at {departmentReportLocation}")

                if (collegeReportLocation):
                    if collegeReportLocation not in p1_CollegeOrDeptMissingRequirement:
                        p1_CollegeOrDeptMissingRequirement.append(collegeReportLocation)
                        logger.info ("Missing Syllabi csv created at " + collegeReportLocation)
    except Exception as error:
        error_handler (f"functionName Course: {courseName}", error)

## This function processes the rows of the CSV file and sends on the relavent data to process_course
def termSyllabiReport (p1_inputTerm):
    functionName = "Term Syllabi Report"
    
    try:
        ## The collegeOrDeptMissingRequirement list is for the syllabiReportSaveCourseInfo function. 
        ## It enables the function to overwrite the previous version of the missing_syllabi file at the beginning and then append all new information after the first overwite.
        collegeOrDeptMissingRequirement = []

        ## Create a list to save the department save pathes determined by the relavent function based
        ## off of the Canvas sub-account structure
        departmentSavePaths = {}

        targetSchoolYear = None

        ## Define the current school year by whether it is before or during/after september
        if re.search("AF|FA|GF", p1_inputTerm):
            ## Fall terms are the first terms of a new school year so FA20 is part of the 2020-21 school year.
            targetSchoolYear = f"{century}{p1_inputTerm[2:]}-{int(p1_inputTerm[2:]) + 1}"
        if re.search("SP|GS|AS|SG|SA|SU", p1_inputTerm):
            ## Spring and Summer terms belong in the same school year as the fall terms before them, so SP21 is part of the same 2020-21 school year as FA20.
            targetSchoolYear = f"{century}{int(p1_inputTerm[2:]) - 1}-{p1_inputTerm[2:]}"

        ## Read the relavent term's courses file into a pandas dataframe
        rawTermCoursesDF = pd.read_csv(f"{baseLocalInputPath}{targetSchoolYear}\\{p1_inputTerm}\\{p1_inputTerm}_Canvas_Courses.csv")

        ## Remove any rows where the course is unpublished or is the chapel course, or were not created by the SIS
        termCoursesDF = rawTermCoursesDF[rawTermCoursesDF['status'] != "unpublished"]
        termCoursesDF = termCoursesDF[termCoursesDF['account_id'] != "U_CHPL"]
        termCoursesDF = termCoursesDF[termCoursesDF['created_by_sis'] == True]
        termCoursesDF = termCoursesDF.dropna(how='all')
        
        ## Create a list to hold the ongoing syllabus retrieval threads
        ongoingcourseSyllabiReportThreads = []
        
        ## For each row in the termCoursesDF dataframe
        for index, row in termCoursesDF.iterrows():

            ## Define a outcome reports and actions thread
            courseSyllabiReportThread = threading.Thread(target=courseSyllabiReport, args=(row, p1_inputTerm, departmentSavePaths, collegeOrDeptMissingRequirement,))
                
            ## Start the outcome reports and actions thread
            courseSyllabiReportThread.start()
                
            ## Add the outcome reports and actions thread to the list of ongoing threads
            ongoingcourseSyllabiReportThreads.append(courseSyllabiReportThread)
                
            ## Wait a second to ensure there is a gap before the next thread
            time.sleep(1)
        
        ## Check if all ongoing outcome threads have completed
        for thread in ongoingcourseSyllabiReportThreads:
            thread.join()

        ## Apply courseSyllabiReport to each row
        #termCoursesDF.apply(courseSyllabiReport, args=(p1_inputTerm, departmentSavePaths, collegeOrDeptMissingRequirement,), axis=1)

    except Exception as error:
        error_handler (functionName, error)

## This function opens the CSV file, the save locations json file, sends the information on, and closes both files
def runSyllabiReport(inputTerm = ""):
    functionName = "Run Syllabi Report"
    
    try:
        currentTerm = ""

        if inputTerm:
            currentTerm = inputTerm
    
        else:
            ## January through May is the Spring Term
            if currentMonth >= 1 and currentMonth <= 5:
                currentTerm = f"SP{str(currentYear)[2:]}"

            ## June through August is the Summer Term
            elif currentMonth >= 6 and currentMonth <= 8:
                currentTerm = f"SU{str(currentYear)[2:]}"

            ## The other months (September through December) is the Fall Term
            else:
                currentTerm = f"FA{str(currentYear)[2:]}"

        termSyllabiReport (p1_inputTerm = currentTerm)
     
    except Exception as error:
        error_handler (functionName, error)

if __name__ == "__main__":

    ## Start and download the Canvas report
    runSyllabiReport (inputTerm = input("Enter the desired term in \
four character format (FA20, SU20, SP20): "))

    input("Press enter to exit")



