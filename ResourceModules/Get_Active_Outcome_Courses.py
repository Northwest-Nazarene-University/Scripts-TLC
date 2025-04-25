# Author: Bryce Miller - brycezmiller@nnu.edu
# Last Updated by: Bryce Miller

## External Pips: openpxpl https://pypi.org/project/openpyxl/,
from __future__ import print_function
from Error_Email_API import errorEmailApi
from Core_Microsoft_Api import downloadSharedMicrosoftFile, uploadSharedMicrosoftFile
from datetime import datetime

import pandas as pd
import traceback, os, logging, sys, csv, re, asyncio, subprocess, json

## Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = "Get Active Outcome Courses"

## Script variables
scriptPurpose = r"""
The Get Active Outcome Courses Courses script takes downloads a xlsx file from the Outcome google sheet, transforms it into a useable .csv, and makes a new .csv of Outcome courses that have students and are published in Canvas.
"""
externalRequirements = r"""
To function properly this script requires access to the Outcome outcome list on the Outcome Council's google drive
"""

## Date Variables
currentDate = datetime.now()
currentMonth = currentDate.month
currentYear = currentDate.year
lastYear = currentYear - 1
nextYear = currentYear + 1
century = str(currentYear)[:2]
decade = str(currentYear)[2:]

## Relative Path (this changes depending on the working directory of the main script)
PFRelativePath = r".\\"

## If the Canvas directory is not in the folder the relative path points to
## find the Canvas directory and set the relative path to its parent folder
test = os.listdir(PFRelativePath)
while "Scripts TLC" not in os.listdir(PFRelativePath):

    PFRelativePath = f"..\\{PFRelativePath}"

## Change the relative path to an absolute path
PFAbsolutePath = f"{os.path.abspath(PFRelativePath)}\\"

## Add Input Modules to the sys path
sys.path.append(f"{PFAbsolutePath}Scripts TLC\\ResourceModules")
sys.path.append(f"{PFAbsolutePath}Scripts TLC\\ActionModules")

## Import local modules
from Make_Api_Call import makeApiCall
from Get_Courses import createCoursesCSV

## Local Path Variables
baseLogPath = f"{PFAbsolutePath}Logs\\{scriptName}\\"
outputPath = f"{PFAbsolutePath}Canvas Resources\\"
baseInputPath = f"{PFAbsolutePath}Canvas Resources\\"
configPath = f"{PFAbsolutePath}Configs TLC\\"

## External Path Variables

## Define a variable to hold the base external input path and output path 
baseExternalInputPath = None ## Where the sis input files are stored
baseExternalOutputPath = None ## Where the output files are stored

## Open Base_External_Paths.json from the config path and get the baseExternalInputPath and baseExternalOutputPath values
with open (f"{configPath}Base_External_Paths.json", "r") as file:
    fileJson = json.load(file)
    baseExternalInputPath = fileJson["baseExternalInputPath"]


## If the base log path doesn't already exist, create it
if not (os.path.exists(baseLogPath)):
    os.makedirs(baseLogPath, mode=0o777, exist_ok=False)

if not (os.path.exists(outputPath)):
        os.makedirs(outputPath, mode=0o777, exist_ok=False)

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

## The variable below holds a set of the functions that have had errors. This enables the error_handler function to only send
## an error email the first time the function triggeres an error
setOfFunctionsWithErrors = set()

## This function handles function errors
def error_handler (p1_ErrorLocation, p1_ErrorInfo, sendOnce = True):
    functionName = "error_handler"
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

## This function calls the Outcome Council's Outcome course code list Google Sheet and saves it as a csv
def get_outcome_course_code_list (p1_termOutputPath
                                  , p2_inputTerm
                                  , p2_targetDesignator
                                  ):
    functionName = "get_outcome_course_code_list"

    try:
        
        ## Define the filepath and name that the input term's course and outcome associations will be saved under
        dataDestinationPath = f"{p1_termOutputPath}\\{p2_inputTerm}_{p2_targetDesignator}_Active_Course_Outcome_Associations.xlsx"

        ## If the file already exists
        if os.path.exists(dataDestinationPath):
            
            ## If the file was modified in the last three hours
            if (datetime.now() - datetime.fromtimestamp(os.path.getmtime(dataDestinationPath))).total_seconds() < 10800:

                ## Log that the file is up to date
                logger.info (f"     \nThe {p2_inputTerm}_{p2_targetDesignator} course association file is up to date.")
                
                ## Return the file as a dataframe
                return pd.read_excel(dataDestinationPath)

        ## Retrieve the Automated Outcome Tool Variables excel file as a df    
        automatedOutcomeToolVariablesDf = pd.read_excel(f"{baseExternalInputPath}Internal Tool Files\\Automated Outcome Tool Variables.xlsx")

        ## Find the Outcome Course association list url that is on the same row as the target designator
        outcomeMicrosoftCourseListUrl = automatedOutcomeToolVariablesDf[automatedOutcomeToolVariablesDf["Target Designator"] == p2_targetDesignator]["Outcome Course Association List URL"].values[0]

        ## Define a variable to hold the course association's data
        courseOutcomeAssociationsDf = pd.DataFrame() 

        ## Download the Outcome Council's Outcome course code alignment file as a worksheet
        outcomeCoursesAssocationFilePath = downloadSharedMicrosoftFile(p1_microsoftUserName = "lmsservice@nnu.edu"
                                                                            , p1_fileShareUrl = outcomeMicrosoftCourseListUrl
                                                                            , p1_downloadSavePath = p1_termOutputPath
                                                                            , p1_fileName = f"{p2_inputTerm}_{p2_targetDesignator}_Course_Outcome_Associations.xlsx"
        )

        ## Define a veriable to track if the file was downloadedq
        fileDownloaded = False

        ## Double check that the file now exists to ensure the download was successful
        if os.path.exists(outcomeCoursesAssocationFilePath):
                
            ## Get the file's last modified date
            outcomeFileLastModifiedDate = datetime.fromtimestamp(os.path.getmtime(outcomeCoursesAssocationFilePath))
                
            ## If the file was modified today
            if outcomeFileLastModifiedDate.date() == datetime.now().date():
                    
                ## Set the fileDownloaded variable to True
                fileDownloaded = True        

        ## If the file wasn't downloaded log an error
        if not fileDownloaded:
            logger.error (f"     \nFailed to download the {p2_inputTerm}_{p2_targetDesignator} course association file.")
            return

        # ## Make a variable to hold the file lines
        # downloadedFileLines = []

        # ## While the list is empty
        # while not downloadedFileLines:

        #     ## Open the CSV file as a text file
        #     with open(outcomeCoursesAssocationFilePath, 'r', encoding='utf-8') as file:
        #         downloadedFileLines = file.readlines()
                
        #     ## If the list is empty wait 5 seconds
        #     if not downloadedFileLines:
        #         time.sleep(5)

        # ## Join the lines back into a single string 
        # downloadedFileAsSingleString = ''.join(downloadedFileLines)

        # ## Use io.StringIO to read the CSV data into a df, using csv.QUOTE_MINIMAL to turing commas within the strings into their own columns
        # downloadedFileDf = pd.read_csv(io.StringIO(downloadedFileAsSingleString), quoting=csv.QUOTE_MINIMAL, encoding='utf-8')


        # ## While there are any instances of 'â€‹' in downloadedFileDf
        # if downloadedFileDf['title'].str.contains(r'Ã¢â‚¬â€¹').any():

        #     ## Replace all instances of 'â€‹' with ''
        #     downloadedFileDf['title'] = downloadedFileDf['title'].str.replace(r'Ã¢â‚¬â€¹', '')

        # ## While there are any instances of Ã¢â‚¬â€œ in downloadedFileDf
        # if downloadedFileDf['title'].str.contains(r'Ã¢â‚¬â€œ').any():
            
        #     ## Replace all instances of Ã¢â‚¬â€œ with -
        #     downloadedFileDf['title'] = downloadedFileDf['title'].str.replace(r'Ã¢â‚¬â€œ', '-')
                
        ## Define a variable to hold the file's contents
        fileDF = None
            
        ## Check if the excel file has sheets
        excelFile = pd.ExcelFile(outcomeCoursesAssocationFilePath)
            
        ## If the excel file has sheets
        if len(excelFile.sheet_names) > 0:

            ## Define a value to retain the designator's target sheet value
            designatedTargetSheetValueList = automatedOutcomeToolVariablesDf[automatedOutcomeToolVariablesDf["Target Designator"] == p2_targetDesignator]["Outcome Course Association Target Sheet Name"].values


            ## If the designatedTargetSheetValueList is not empty and if the first value is neither empty nor nan
            if (designatedTargetSheetValueList 
                and str(designatedTargetSheetValueList[0]) not in ["nan", ""]
                ):

                ## Read in the sheet name from the automatedOutcomeToolVariablesDf
                fileDF = pd.read_excel (outcomeCoursesAssocationFilePath, sheet_name = designatedTargetSheetValueList[0])

            ## Otherwise if the file has a sheet named "By Course"
            elif "By Course" in excelFile.sheet_names:
                
                ## Read in the by course sheet
                fileDF = pd.read_excel (outcomeCoursesAssocationFilePath, sheet_name = "By Course")
                
        ## If there is no file data from looking for the sheet names
        if fileDF is None:
                
            ## Read the excel file normally
            fileDF = pd.read_excel (outcomeCoursesAssocationFilePath)

        ## If there is a column which in lowercase is "course number" replace it with "Number"
        if "course number" in fileDF.columns.str.lower():
            fileDF.rename(columns={col: "Number" for col in fileDF.columns if col.lower() == "course number"}, inplace=True)


        ## If the first column in lowercase is not named "prefix"
        if fileDF.columns[0].lower() != "prefix":
                
            ## Then the first row is the header so replace the header file with the first row
            new_header_row = fileDF.iloc[0]
            fileDF = fileDF[1:]
            fileDF.columns = new_header_row

        ## If the first column in lowercase is named "prefix" but is not equal to "Prefix"
        if fileDF.columns[0].lower() == "prefix" and fileDF.columns[0] != "Prefix":

            ## For each column
            for column in fileDF.columns:

                ## Make the first letter of the column name upper case and the rest lower case
                fileDF.rename(columns={column: column.capitalize()}, inplace=True)

        ## If any of the values in the Prefix column are equal to the outcome area
        if p2_targetDesignator != "GE" and fileDF["Prefix"].str.contains(p2_targetDesignator).any():

            ## Remove the first two characters from the Prefix column
            fileDF["Prefix"] = fileDF["Prefix"].str[2:]

        ## Change all columns that, when all lowercase, matches 'outcome' to capitalized first letter of words
        for column in fileDF.columns[fileDF.columns.str.lower().str.contains("outcome")]:

            ## Make the first letter of the column name upper case and the rest lower case
            fileDF.rename(columns={column: column.capitalize()}, inplace=True)

        ## Add Department as a new first column with the key as the value
        fileDF.insert(0, "Outcome Area", p2_targetDesignator)

        ## Find the first outcome column that isn't outcome area
        outcomeColumnIndex = fileDF.columns[fileDF.columns.str.contains("Outcome") & ~fileDF.columns.str.contains("Outcome Area")][0]

        ## Remove all rows that have an empty cell in the first column, the second column, or the first outcome column
        fileDF = fileDF.dropna(subset = [fileDF.columns[0], fileDF.columns[1], outcomeColumnIndex])
                
        ## IF the key is Outcome
        if p2_targetDesignator == "GE":
                    
            ## Define the Outcome pre and postfix
            OutcomePrefix = "GE_"
            OutcomePostfix = "_V2.0" 

            ## Add the prefix and postfix to the Outcome 1 and Outcome 2 columns
            fileDF["Outcome 1"] = OutcomePrefix + fileDF["Outcome 1"] + OutcomePostfix
            fileDF["Outcome 2"] = OutcomePrefix + fileDF["Outcome 2"] + OutcomePostfix

        ## If the target designator is G-EDUC
        elif p2_targetDesignator == "G-EDUC":
            
            ## Define the outcome prefix, postfix, and subDelineator
            OutcomePrefix = "G-EDUC_"
            OutcomePostfix = "_1.0"
            OutcomeSubDelineator = ": "

            ## For each column that has "Outcome" in it but not "Outcome Area"
            for column in fileDF.columns[fileDF.columns.str.contains("Outcome") & ~fileDF.columns.str.contains("Outcome Area")]:

                ## Make the column values that are not nan all caps
                fileDF[column] = fileDF[column].astype(str).str.upper()

                ## Add the prefix. postfix, and replace the first space to the subdelineator to the column
                fileDF[column] = OutcomePrefix + fileDF[column] + OutcomePostfix
                fileDF[column] = fileDF[column].str.replace(" ", OutcomeSubDelineator, n=1)

                ## Replace all column values that contain "NAN" with ""
                fileDF = fileDF.applymap(lambda cellValue: "" if isinstance(cellValue, str) and "NAN" in cellValue else cellValue)


                
        ## If the courseOutcomeAssociationsDf is empty
        if courseOutcomeAssociationsDf.empty:
                
            ## Set the courseOutcomeAssociationsDf to the fileDF
            courseOutcomeAssociationsDf = fileDF
                
        ## Else concatenate the fileDF to the courseOutcomeAssociationsDf
        else:
                
            courseOutcomeAssociationsDf = pd.concat([courseOutcomeAssociationsDf, fileDF], ignore_index=True)

        ## replace any '"' characters with an empty string
        courseOutcomeAssociationsDf = courseOutcomeAssociationsDf.replace('"', "", regex=True)

        ## Remove any rows that have an empty cell in column 1, 2, or the first column with outcome but not outcome area in the column name
        courseOutcomeAssociationsDf = courseOutcomeAssociationsDf.dropna(
            subset = [
                      courseOutcomeAssociationsDf.columns[0]
                      , courseOutcomeAssociationsDf.columns[1]
                      , courseOutcomeAssociationsDf.columns[
                          courseOutcomeAssociationsDf.columns.str.contains("Outcome") 
                          & ~courseOutcomeAssociationsDf.columns.str.contains("Outcome Area")][0]
                      ]
            )

        ## Add a course code column after the outcome area column of the courseOutcomeAssociationsDf
        courseOutcomeAssociationsDf.insert(1, "Course Code", courseOutcomeAssociationsDf["Prefix"] + courseOutcomeAssociationsDf["Number"].astype(str))

        ## Remove the Prefix and Number columns
        courseOutcomeAssociationsDf = courseOutcomeAssociationsDf.drop(columns = ["Prefix", "Number"])
                
        ## Save the data as a excel        
        courseOutcomeAssociationsDf.to_excel (dataDestinationPath, index = None, header = True)

        return courseOutcomeAssociationsDf

    except exception as error:
        error_handler (functionName, p1_ErrorInfo = error)
        
## This function gets the crosslisted course ids
def getCrosslistedCourseIds (p1_header, p1_courseId):
    functionName = "getCrosslistedCourseIds"
    
    try:
        

        ## Define the list course sections api url
        courseSectionsApiUrl = f"{CoreCanvasAPIUrl}courses/sis_course_id:{p1_courseId}/sections"

        ## Make an api call to get a list of the course's sections
        courseSectionsApiObject = makeApiCall(p1_header = p1_header, p1_apiUrl = courseSectionsApiUrl)
        
        ## Save the primary body of information retrieved by the API call
        courseSectionsApiText = courseSectionsApiObject.text
        
        ## Convert the json body of information into a Python Dictionary
        courseSectionsApiResponseList = json.loads(courseSectionsApiText)

        ## Define a list to hold the section names
        sectionNamesList = []
        
        ## Define a list to hold the crosslisted section ids
        crosslistedSectionIds = []

        ## Define a list to hold the crosslisted_course_id
        crosslistedCanvasCourseIdList = []
        
        ## For each section in the courseSectionsApiResponseList
        for section in courseSectionsApiResponseList:

            ## If the section's crosslisted_course_id isn't none
            if section["crosslisted_course_id"] != None:

                ## Add the crosslistedCanvasCourseId to the crosslistedCourseIds list
                crosslistedCanvasCourseIdList.append(section["crosslisted_course_id"])
                
            ## Add the crosslisted section id to the crosslistedSectionIds list
            crosslistedSectionIds.append(section["id"])

            ## Add the section name to the sectionNamesList
            sectionNamesList.append(section["name"])
                
        return sectionNamesList, crosslistedSectionIds, crosslistedCanvasCourseIdList
        

        
    except exception as error:
        error_handler (functionName, p1_ErrorInfo = error)

## This function creates a csv of the active Canvas courses for the user inputed year that match the Outcome Course Codes
def create_csv_of_active_Outcome_courses (p1_inputTerm, p1_targetDesignator):
    functionName = "create_csv_of_active_Outcome_courses"

    try:
            
        ## Define the API Call header using the retreived Canvas Token
        header = {'Authorization' : f"Bearer {canvasAccessToken}"}

        ## Determine and save the term's school year
        schoolYear = None
        if re.search("AF|FA|GF", p1_inputTerm):
            ## Fall terms are the first terms of a new school year so FA20 is part of the 2020-21 school year.
            schoolYear = (century + p1_inputTerm[2:] + "-" + str(int(p1_inputTerm[2:]) + 1))
        elif re.search("SP|GS|AS|SG|SA|SU", p1_inputTerm):
            ## Spring and Summer terms belong in the same school year as the fall terms before them, so SP21 is part of the same 2020-21 school year as FA20.
            schoolYear = (century + str(int(p1_inputTerm[2:]) - 1) + "-" + p1_inputTerm[2:])
        
        ## Create the school year relavent output path
        schoolYearOutputPath = f"{outputPath}\\{schoolYear}\\"
        
        ## Define the term specific output path
        termOutputPath = f"{schoolYearOutputPath}{p1_inputTerm}\\"
        termInputPath = termOutputPath

        ## Define the target output file path and name
        targetOutputPathAndFileName = f"{termOutputPath}{p1_inputTerm}_{p1_targetDesignator}_Active_Outcome_Courses.xlsx"
                                      
        ## If the output log path doesn't already exist, create it
        if not (os.path.exists(termOutputPath)):
            os.makedirs(termOutputPath, mode=0o777, exist_ok=False)

        ## Otherwise
        else:

            ## Check if the file already exists
            if os.path.exists(targetOutputPathAndFileName):

                ## If the file was modified in the last three hours
                if (datetime.now() - datetime.fromtimestamp(os.path.getmtime(targetOutputPathAndFileName))).total_seconds() < 10800:

                    ## Log that the file is up to date
                    logger.info (f"     \nThe {p1_inputTerm}_{p1_targetDesignator} active outcome course file is up to date.")

                    ## Return the file path
                    return targetOutputPathAndFileName

        ## Get the Outcome Council's Outcome course code list
        courseOutcomeAssociationsDf = get_outcome_course_code_list(p1_termOutputPath = termOutputPath
                                                                   , p2_inputTerm = p1_inputTerm
                                                                   , p2_targetDesignator = p1_targetDesignator
                                                                   )

        ## If the course outcome associations df is empty or = None
        if courseOutcomeAssociationsDf is None or courseOutcomeAssociationsDf.empty:

            ## Then there are no active outcome courses for the target designation and term, so save a blank excel
            pd.DataFrame().to_excel (targetOutputPathAndFileName, index = None, header = True)
            return
        
        ## Create a dict for the active outcome courses and their information
        activeOutcomeCoursesDict = {
            "Term": []
            , "Outcome Area": []
            , "Course_sis_id": []
            , "Parent_Course_sis_id": []
            , "Section_id": []
            , "Course_name": []
            , "Canvas_Account_id": []
            , "Number_of_students": []
            , "Instructor_#1_ID": []
            , "Instructor_#1_name": []
            , "Instructor_#1_email": []
            }
        
        ## Get the outcome column names from the course outcome associations df
        outcome_columns = [col for col in courseOutcomeAssociationsDf.columns if "Outcome" in col and "Area" not in col]
        
        ## Add the outcome columns to the activeOutcomeCoursesDict
        for outcome_column in outcome_columns:
            
            activeOutcomeCoursesDict[outcome_column] = []

        ## Open the relevant course list as a data frame
        canvasTermCoursesDf = pd.read_csv(f"{termInputPath}{p1_inputTerm}_Canvas_Courses.csv")

        ## Open the relevant section list as a data frame
        canvasTermSectionsDf = pd.read_csv(f"{termInputPath}{p1_inputTerm}_Canvas_Sections.csv")

        ## Open the all courses csv as a df
        canvasAllCoursesDf = pd.read_csv(createCoursesCSV(header, p1_inputTerm = "All"))

        ## Open the all sections csv as a df
        canvasAllSectionsDf = pd.read_csv(f"{baseInputPath}All_Canvas_Sections.csv")
    
        ## For each row in the canvasTermCoursesDf
        for index, row in canvasTermCoursesDf.iterrows():
            
            ## If the course has a course_id value
            if row["course_id"] and row["created_by_sis"] == True:
                    
                #if "SP2025_ENGL4980" in row["course_id"]:
                        

                    ## If the course has a _ in its course_id (which all for credit courses have) at NNU
                    if "_" in row["course_id"]:

                        ## If the course id is not already in the activeOutcomeCoursesDict["Course_sis_id"] list
                        if row["course_id"] not in activeOutcomeCoursesDict["Course_sis_id"]:
                                
                            ## Make a list to hold crosslisted course sis ids and section ids
                            crosslistedCanvasCourseIdList = []  
                            crosslistedCanvasSectionIdsList = []
            
                            ## If the course code appears in the course code column of the courseOutcomeAssociationsDf
                            ## (The second element of a course id when separated by _ is the course code at NNU)
                            if ((row["course_id"].split('_')[1] in courseOutcomeAssociationsDf["Course Code"].values)
                                or (p1_targetDesignator == "GE"
                                    and ("KINE1020" in row["course_id"] 
                                            or (
                                                "ENGL1030" in row["course_id"] 
                                                and not "ENGL1030L" in row["course_id"]
                                                )
                                            or "PHYS2120L" in row["course_id"]
                                            )
                                    )
                                ):

                                ## For each row that it appears in in the courseOutcomeAssociationsDf
                                for index in courseOutcomeAssociationsDf[courseOutcomeAssociationsDf["Course Code"] == row["course_id"].split('_')[1]].index:

                                    ## Add the course to the active Outcome Courses Dict
                                    activeOutcomeCoursesDict["Term"].append(p1_inputTerm)
                                    activeOutcomeCoursesDict["Outcome Area"].append(courseOutcomeAssociationsDf.loc[index, "Outcome Area"])
                                    activeOutcomeCoursesDict["Course_sis_id"].append(row["course_id"])
                                    activeOutcomeCoursesDict["Course_name"].append(row["long_name"])
                                    activeOutcomeCoursesDict["Canvas_Account_id"].append(row["canvas_account_id"])
                                    activeOutcomeCoursesDict["Parent_Course_sis_id"].append("")
                                    activeOutcomeCoursesDict["Number_of_students"].append(0)

                                    ## Make a DF of the sections with the row canvas course id
                                    targetCourseSectionsDf = canvasAllSectionsDf[canvasAllSectionsDf["canvas_course_id"] == row["canvas_course_id"]]

                                    ## Get the index where the name column of the canvasAllSectionsDf contains the course_id
                                    primarySectionIndex = canvasAllSectionsDf[
                                        canvasAllSectionsDf["name"].fillna("").str.contains(
                                            row["course_id"]
                                            )
                                        ].index[0]

                                    ## Add the section id that matches the course name to the active Outcome Courses Dict
                                    activeOutcomeCoursesDict["Section_id"].append(canvasAllSectionsDf.loc[primarySectionIndex, "canvas_section_id"])

                                    ## If the targetCourseSectionsDf has more than one section
                                    if len(targetCourseSectionsDf) > 1:

                                        ## For each additional section in the targetCourseSectionsDf
                                        for sectionIndex in targetCourseSectionsDf.index:

                                            ## If the section name is not the same as the course name
                                            if targetCourseSectionsDf.loc[sectionIndex, "name"] != row["long_name"]:

                                                ## Add the crosslisted section and canvas_course id id to the crosslistedCanvasSectionIdsList and crosslistedCanvasCourseIdList
                                                crosslistedCanvasSectionIdsList.append(targetCourseSectionsDf.loc[sectionIndex, "canvas_section_id"])
                                                crosslistedCanvasCourseIdList.append(targetCourseSectionsDf.loc[sectionIndex, "canvas_course_id"])
                                
                                    ## For each outcome column in the courseOutcomeAssociationsDf
                                    for outcome_column in outcome_columns:
                                    
                                        ## Add the outcome to the active Outcome Courses Dict
                                        activeOutcomeCoursesDict[outcome_column].append(courseOutcomeAssociationsDf.loc[index, outcome_column])
                                    
                                    ## Make blank instructor 1 values
                                    activeOutcomeCoursesDict["Instructor_#1_ID"].append("")
                                    activeOutcomeCoursesDict["Instructor_#1_name"].append("")
                                    activeOutcomeCoursesDict["Instructor_#1_email"].append("")                                    

                            ## If there are crosslisted courses
                            if crosslistedCanvasSectionIdsList:

                                ## For each crosslisted course sis id and crosslisted course section id
                                for crosslistedCanvasSectionId, crosslistedCanvasCourseId in zip(crosslistedCanvasSectionIdsList, crosslistedCanvasCourseIdList):

                                    ## Get the index of the crosslistedCanvasCourseId in from the canvasAllCoursesDf
                                    crosslistedSectionIndex = canvasAllSectionsDf[canvasAllSectionsDf["canvas_section_id"] == crosslistedCanvasSectionId].index[0]

                                    ## Get the long name of the crosslistedCanvasCourseId
                                    crosslistedCourseName = canvasAllSectionsDf.loc[crosslistedSectionIndex, "name"]

                                    ## Define variables to hold the course code and sis id
                                    crosslistedCourseCode = None
                                    crosslistedCourseSisId = None

                                    ## Attempt to isolate the crosslisted course Code as it would show up in the courseOutcomeAssociationsDf
                                    try: ## Irregular try clause, do not comment out in testing
                                        
                                        ## Isolate the course by getting the last element after spliting by " " and removing the "I_" if it is an independent study course
                                        crosslistedCourseCode = (
                                            crosslistedCourseName.replace('I_', '_').split('_')[1]
                                            if "IS:" in crosslistedCourseName 
                                            else crosslistedCourseName.split('_')[1]
                                            )

                                        ## Isolate the crosslisted course Sis Id by getting the last element after spliting by " "
                                        crosslistedCourseSisId = crosslistedCourseName.split(' ')[-1]

                                    ## If there is an error, the section was not an official course section
                                    except: ## Irregular except clause, do not comment out in testing

                                        ## Skip the course
                                        continue

                                    ## If the crosslisted course sis id appears in the courseOutcomeAssociationsDf
                                    if crosslistedCourseCode in courseOutcomeAssociationsDf["Course Code"].values:

                                        ## For each row that appears in in the courseOutcomeAssociationsDf
                                        for crosslistedIndex in courseOutcomeAssociationsDf[courseOutcomeAssociationsDf["Course Code"] == crosslistedCourseCode].index:

                                            ## Add the course to the active Outcome Courses Dict
                                            activeOutcomeCoursesDict["Term"].append(p1_inputTerm)
                                            activeOutcomeCoursesDict["Outcome Area"].append(courseOutcomeAssociationsDf.loc[crosslistedIndex, "Outcome Area"])
                                            activeOutcomeCoursesDict["Course_sis_id"].append(crosslistedCourseSisId)
                                            activeOutcomeCoursesDict["Course_name"].append(crosslistedCourseName)
                                            activeOutcomeCoursesDict["Canvas_Account_id"].append(row["canvas_account_id"])
                                            activeOutcomeCoursesDict["Number_of_students"].append(0)
                                                
                                            ## For each outcome column in the courseOutcomeAssociationsDf
                                            for outcome_column in outcome_columns:
                                                    
                                                ## Add the outcome to the active Outcome Courses Dict
                                                activeOutcomeCoursesDict[outcome_column].append(courseOutcomeAssociationsDf.loc[crosslistedIndex, outcome_column])
                                                    
                                            ## Make blank instructor 1 values
                                            activeOutcomeCoursesDict["Instructor_#1_ID"].append("")
                                            activeOutcomeCoursesDict["Instructor_#1_name"].append("")
                                            activeOutcomeCoursesDict["Instructor_#1_email"].append("")

                                            ## Add the parent course sis id to the active Outcome Courses Dict
                                            activeOutcomeCoursesDict["Parent_Course_sis_id"].append(row["course_id"])
                                                
                                            ## Get the index of the crosslisted course id in the crosslistedCourseNamesList
                                            crosslistedCanvasCourseIdIndex = crosslistedCanvasCourseIdList.index(crosslistedCanvasCourseId)

                                            ## Add the section id that matches the crosslisted course name + 1 to the active Outcome Courses Dict
                                            activeOutcomeCoursesDict["Section_id"].append(crosslistedCanvasSectionIdsList[crosslistedCanvasCourseIdIndex])

        ## If the Course_sis_id column is empty
        if not activeOutcomeCoursesDict["Course_sis_id"]:

            ## Then there are no active outcome courses for the target designation and term, so save a blank excel
            pd.DataFrame().to_excel (targetOutputPathAndFileName, index = None, header = True)
            return
                                
        ## Define the activeCanvasEnrollmentsDf
        activeCanvasEnrollmentsDf = pd.DataFrame()
    
        ## For each unique term in the active outcome courses dict found by combining the first two and 4th and 5th str elements of the course sis ids
        for term in set([term[:2] + term[4:6] for term in activeOutcomeCoursesDict["Course_sis_id"]]):

            ## Determine and save the term's school year
            termSchoolYear = None
            if re.search("FA|GF", p1_inputTerm):
                ## Fall terms are the first terms of a new school year so FA20 is part of the 2020-21 school year.
                termSchoolYear = (century + p1_inputTerm[2:] + "-" + str(int(p1_inputTerm[2:]) + 1))
            elif re.search("SP|GS|SG|SU", p1_inputTerm):
                ## Spring and Summer terms belong in the same school year as the fall terms before them, so SP21 is part of the same 2020-21 school year as FA20.
                termSchoolYear = (century + str(int(p1_inputTerm[2:]) - 1) + "-" + p1_inputTerm[2:])
            
            ## Get the term canvas enrollments file and add it to the activeCanvasEnrollmentsDf
            activeCanvasEnrollmentsDf = pd.concat([activeCanvasEnrollmentsDf, pd.read_csv(f"{baseInputPath}{termSchoolYear}\\{term}\\{term}_Canvas_Enrollments.csv")])

        ## Remove any nan's from the activeCanvasEnrollmentsDf's user_id column and convert them to strings, then remove any "0"
        activeCanvasEnrollmentsDf["user_id"] = activeCanvasEnrollmentsDf["user_id"].fillna(0).astype(int).astype(str).replace("0", "")
        
        ## Look at each in the activeCanvasEnrollmentsDf
        for index, row in activeCanvasEnrollmentsDf.iterrows():

            #if "SP2024_MUSC1010_1U" in row["course_id"]:
                
                ## If the enrollment status is "active" or "concluded"
                if row["status"] == "active" or row["status"] == "concluded":
                    
                    ## Check if the course on the current enrollment is a Outcome course
                    if row["course_id"] in activeOutcomeCoursesDict["Course_sis_id"]:

                        ## Define the target index for the course in the activeOutcomeCoursesDict
                        targetIndex = activeOutcomeCoursesDict["Course_sis_id"].index(row["course_id"])

                        ## If the row canvas section id appears in the activeOutcomeCoursesDict
                        if row["canvas_section_id"] in activeOutcomeCoursesDict["Section_id"]:
                        
                            ## Get the target index
                            targetIndex = activeOutcomeCoursesDict["Section_id"].index(row["canvas_section_id"])

                        ## Otherwise
                        else:

                            ## The section the student is enrolled in is not a outcome course, so skip it
                            continue
                           
                        ## If it is a Outcome course and the enrollment type is a student enrollment
                        if row["base_role_type"] == "StudentEnrollment":

                            ## Increment the student count in the course dict within the ge_active_courses_and_outcomes dict
                            activeOutcomeCoursesDict["Number_of_students"][targetIndex] += 1
                            
                        ## Else if the enrollment is a instructor enrollment 
                        elif row["base_role_type"] == "TeacherEnrollment":

                            ## If the id is not 63232 which is the TBD user id
                            if row["user_id"] not in ["63232.0", "63232"]:
                            
                                ## If the instructor #1 ID at the index is empty
                                if activeOutcomeCoursesDict["Instructor_#1_ID"][targetIndex] == "":

                                    ## Add the instructor's id to the activeOutcomeCoursesDict at the index of the 
                                    activeOutcomeCoursesDict["Instructor_#1_ID"][targetIndex] = row["user_id"]

                                ## If there is already a instructor #1 ID
                                else:

                                    ## Make a variable to hold the key name that the instructor's id will be added to
                                    targetInstructorIDKey = None
                                    
                                    ## Make a list of the keys that have instructor and id in them
                                    instructorIDKeys = [key for key in activeOutcomeCoursesDict.keys() if "Instructor" in key and "ID" in key]
                                    
                                    ## Make a list of the processed instructor ids using the instructorIDKeys
                                    processedInstructorIds = [activeOutcomeCoursesDict[key][targetIndex] for key in instructorIDKeys if activeOutcomeCoursesDict[key][targetIndex]]

                                    ## If the user id is already in the processed instructor ids
                                    if row["user_id"] in processedInstructorIds:
                                        
                                        ## Skip it
                                        continue
                                    
                                    ## For each key in the instructorIDKeys
                                    for key in instructorIDKeys:
                                        
                                        ## If the key is not the first instructor id key
                                        if key != "Instructor_#1_ID":
                                            
                                            ## If the key's value at the index is empty
                                            if activeOutcomeCoursesDict[key][targetIndex] == "":
                                                
                                                ## Set the targetInstructorIDKey to the key
                                                targetInstructorIDKey = key
                                                break

                                    ## If there still is no targetInstructorIDKey
                                    if not targetInstructorIDKey:
                                        
                                        ## Create new instructor id, name, and email keys using the length of the instructorIDKeys alist +1
                                        newInstructorIDKey = f"Instructor_#{len(instructorIDKeys) + 1}_ID"
                                        newInstructorNameKey = f"Instructor_#{len(instructorIDKeys) + 1}_name"
                                        newInstructorEmailKey = f"Instructor_#{len(instructorIDKeys) + 1}_email"
                                        
                                        ## Add the new instructor id key to the instructorIDKeys list
                                        instructorIDKeys.append(newInstructorIDKey)
                                        
                                        ## Add the new keys to the activeOutcomeCoursesDict
                                        activeOutcomeCoursesDict[newInstructorIDKey] = []
                                        activeOutcomeCoursesDict[newInstructorNameKey] = []
                                        activeOutcomeCoursesDict[newInstructorEmailKey] = []
                                        
                                        ## For for the length of the Instructor_#1_ID list
                                        for i in range(len(activeOutcomeCoursesDict["Instructor_#1_ID"])):
                                                
                                            ## Add a blank value to the new lists so they are the same length as the Instructor_#1_ID list
                                            activeOutcomeCoursesDict[newInstructorIDKey].append("")
                                            activeOutcomeCoursesDict[newInstructorNameKey].append("")
                                            activeOutcomeCoursesDict[newInstructorEmailKey].append("")
                                        
                                        ## Set the targetInstructorIDColumn to the newInstructorIDColumn
                                        targetInstructorIDColumn = newInstructorIDKey

                                    ## Set the instructor's id to the targetInstructorIDColumn at the courseIndex
                                    activeOutcomeCoursesDict[targetInstructorIDColumn][targetIndex] = row["user_id"]
            
        ## For each course in the activeOutcomeCoursesDict
        for course in activeOutcomeCoursesDict["Course_sis_id"]:

            ## For each row that it appears in in the activeOutcomeCoursesDict
            for courseIndex in [index for index, dictRowCourseId in enumerate(activeOutcomeCoursesDict["Course_sis_id"]) if dictRowCourseId == row["course_id"]]:  
                
                ## If the course has no students
                if activeOutcomeCoursesDict["Number_of_students"][courseIndex] == 0:
                    
                    ## For each key in the activeOutcomeCoursesDict
                    for key in activeOutcomeCoursesDict.keys():
                        
                        ## Remove the course details from the activeOutcomeCoursesDict
                        activeOutcomeCoursesDict[key].pop(courseIndex)
    
        ## Get the name and email that match the instructor Ids
        with open (baseInputPath + "\Canvas_Users.csv", newline='') as active_canvas_users:
            active_canvas_users_reader = csv.DictReader(active_canvas_users, delimiter = ',')
            
            ## Make lists of the user ids, names, and emails
            userIds = []
            userNames = []
            userEmails = []
            
            ## For each row in the active_canvas_users_reader
            for row in active_canvas_users_reader:
                
                ## Add the user id, name, and email to the lists
                userIds.append(row["user_id"])
                userNames.append(row["full_name"])
                userEmails.append(row["email"])

            ## Make a list of the instructor id keys
            instructorIDKeys = [key for key in activeOutcomeCoursesDict.keys() if "Instructor" in key and "ID" in key]

            ## For each key in the instructorIDKeys
            for key in instructorIDKeys:
                
                ## For each user value in the key
                for user_id in activeOutcomeCoursesDict[key]:
                    
                    ## If the user id is not empty
                    if user_id:
                    
                        ## Make a list of all of the indexes where the user id appears in the key
                        userIndexes = [i for i, x in enumerate(activeOutcomeCoursesDict[key]) if x == user_id]
                    
                        ## Get the index of the user id within the active_canvas_users_reader
                        userSecondaryIndex = userIds.index(user_id)

                        ## For each index in the userIndexes list
                        for userIndex in userIndexes:
                    
                            ## Replace the empty name and email values with the user's name and email
                            activeOutcomeCoursesDict[key.replace("ID", "name")][userIndex] = userNames[userSecondaryIndex]
                            activeOutcomeCoursesDict[key.replace("ID", "email")][userIndex] = userEmails[userSecondaryIndex]

        ## Turn the dict into a dataframe
        activeOutcomeCoursesDf = pd.DataFrame(activeOutcomeCoursesDict)

        ## Define the current term
        currentTerm = ""

        ## January through May is the Spring Term
        if currentMonth >= 1 and currentMonth <= 5:
            currentTerm = f"SP{str(currentYear)[2:]}"

        ## June through August is the Summer Term
        elif currentMonth >= 6 and currentMonth <= 8:
            currentTerm = f"SU{str(currentYear)[2:]}"

        ## The other months (September through December) is the Fall Term
        else:
            currentTerm = f"FA{str(currentYear)[2:]}"

        ## If the input term is the same as the current term or if the input term's decade is less than the current term's decade
        if p1_inputTerm == currentTerm or int(p1_inputTerm[2:]) < int(currentTerm[2:]):    
            
            ## Remove any rows where the course has no students
            activeOutcomeCoursesDf = activeOutcomeCoursesDf[activeOutcomeCoursesDf["Number_of_students"] > 0]

            ## Remove any rows where there is no instructor 
            activeOutcomeCoursesDf = activeOutcomeCoursesDf[activeOutcomeCoursesDf["Instructor_#1_ID"] != ""]    
        
        ## Remove any values that are all whitespace
        activeOutcomeCoursesDf = activeOutcomeCoursesDf.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        
        ## Remove zero-width spaces from the DataFrame
        activeOutcomeCoursesDf = activeOutcomeCoursesDf.applymap(lambda x: x.replace('\u200b', '') if isinstance(x, str) else x)
        
        ## Save the dataframe as a excel
        activeOutcomeCoursesDf.to_excel (targetOutputPathAndFileName, index = None, header = True)

        ## Record the completion of the function
        logger.info (f"     \nSucessfully created {p1_inputTerm} Active Outcome Courses Excel File")

    except exception as error:
        error_handler (functionName, p1_ErrorInfo = error)
  
def termGetActiveOutcomeCourses(inputTerm, targetDesignator):
    functionName = "Get Active Outcome Courses"

    try:

        ## Start and download the Canvas report
        create_csv_of_active_Outcome_courses(p1_inputTerm = inputTerm, p1_targetDesignator = targetDesignator)

    except exception as error:
        error_handler (functionName, p1_ErrorInfo = error)

if __name__ == "__main__":

    ## Start and download the Canvas report
    create_csv_of_active_Outcome_courses (
        p1_inputTerm = input("Enter the desired term in four character format (FA20, SU20, SP20): ")
        , p1_targetDesignator = input("Enter the desired target designator (GE, I-EDUC, U-ENGR): ")
        )

    input("Press enter to exit")