# Author: Bryce Miller - brycezmiller@nnu.edu
# Last Updated by: Bryce Miller

## Import Generic Moduels
from __future__ import print_function
import traceback, os, sys, logging, requests, re, os, os.path, threading, math, json
from datetime import datetime, date, timedelta
from dateutil import parser
import pandas as pd

## Set working directory
os.chdir(os.path.dirname(__file__))

## Add Script repository to syspath
sys.path.append(f"{os.getcwd()}\ResourceModules")

# Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = "Add Outcomes to Active Courses"

scriptPurpose = r"""
The Outcome Exporter script is to copy the most recent relative outcome/s into the c ourses that need them.
"""
externalRequirements = r"""
To function properly this script requires a spreadsheet of the most recent outcomes and the courses they are assigned to.
"""

## Date Variables
currentDate = datetime.now()
todaysDateDateTime = datetime.combine(currentDate, datetime.min.time())
currentMonth = currentDate.month
currentYear = currentDate.year
century = str(currentYear)[:2]
decade = str(currentYear)[2:]

# Time variables
currentDate = date.today()
current_year = currentDate.year
lastYear = current_year - 1
nextYear = current_year + 1
century = str(current_year)[:2]
decade = str(current_year)[2:]

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
from Make_Api_Call import makeApiCall
from Get_Courses import createCoursesCSV

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

## Define a variable to hold the Canvas Access Token
canvasAccessToken = ""

## Open and retrieve the Canvas Access Token
with open (fr"{configPath}Canvas_Access_Token.txt", "r") as file:
    canvasAccessToken = file.readlines()[0]

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

## This function takes in a start date and end date and returns what course week the course is currently in and what week the final week is
def determineCourseWeek (p1_startDate, p2_endDate):
    
    ## Record the course start and end date as date time variables
    courseStartDateTime = datetime.strptime(p1_startDate, "%m/%d/%Y") + timedelta(weeks=3) ## Add 3 weeks as the sis date sent over is always 3 weeks earlier than the actual start date
    courseEndDateTime = datetime.strptime(p2_endDate, "%m/%d/%Y") - timedelta(weeks=3) ## Subtract 3 weeks as the sis date sent over is always 3 weeks earlier than the actual start date

    ## Determine the course's final week (e.g. 16 if it is a 16 week course)
    courseFinalWeek = math.ceil((courseEndDateTime - courseStartDateTime).days / 7) ## Round up as even a partial week is a week 

    ## Record the day of the week that the course starts
    courseStartWeekDay = courseStartDateTime.weekday()

    ## Determine what week the course is currently in
    courseWeek = (((todaysDateDateTime - (courseStartDateTime- timedelta(days=courseStartWeekDay))).days // 7) + 1) ## Add one week to make the first week be considered week 1

    ## Return the course week and the course final week
    return courseWeek, courseFinalWeek

## This function retrieves the data neccessary for determining and sending out relevent communication
def retrieveDataForRelevantCommunication (p2_inputTerm
                                          , p3_targetDesignator
                                          , p1_header
                                          ):
    
    functionName = "Retrieve Data For Relevant Communication"
    
    try:
    
        ## Define an auxillary data dict and auxillary df dict
        auxillaryDataDict = {}
        auxillaryDFDict = {}

        ## Define the current school year by whether it is before or during/after september
        if re.search("AF|FA|GF", p2_inputTerm):
            ## Fall terms are the first terms of a new school year so FA20 is part of the 2020-21 school year.
            auxillaryDataDict["Target School Year"] = f"{century}{p2_inputTerm[2:]}-{int(p2_inputTerm[2:]) + 1}"
        if re.search("SP|GS|AS|SG|SA|SU", p2_inputTerm):
            ## Spring and Summer terms belong in the same school year as the fall terms before them, so SP21 is part of the same 2020-21 school year as FA20.
            auxillaryDataDict["Target School Year"] = f"{century}{int(p2_inputTerm[2:]) - 1}-{p2_inputTerm[2:]}"
            
        ## Define a school year related path
        schoolYearPath = f"{baseLocalInputPath}{auxillaryDataDict['Target School Year']}\\"
        
        ## Define a term related path
        termPath = f"{schoolYearPath}{p2_inputTerm}\\"

        ## Define the active outcome courses path
        activeOutcomeCoursesPath = f"{termPath}{p2_inputTerm}_{p3_targetDesignator}_Active_Outcome_Courses.xlsx"

        ## Retrieve the csv of Active GE courses which includes course code, required outcome/s, and the relevant instructor name/s, id/s, and email/s
        rawActiveOutcomeCourseDf = pd.read_excel(activeOutcomeCoursesPath)

        ## If the raw active outcome course df is empty
        if rawActiveOutcomeCourseDf.empty:

            ## Return an empty dataframe for the active outcome courses df and the auxillary df dict
            return rawActiveOutcomeCourseDf, auxillaryDFDict
        
        ## Make a list of the unique outcomes that are not blank 
        ## and a dict to hold the course id of the course named after each outcome
        auxillaryDFDict["Unique Outcomes"], auxillaryDFDict["Outcome Course Dict"] = getUniqueOutcomesAndOutcomeCoursesDict(rawActiveOutcomeCourseDf, p1_header)
        
        ## Remove any outcomes that don't have corresponding courses
        auxillaryDFDict["Active Outcome Courses DF"] = removeMissingOutcomes (
            rawActiveOutcomeCourseDf
            , auxillaryDFDict["Unique Outcomes"]
            , auxillaryDFDict["Outcome Course Dict"]
            )
        
        ## Retrieve the csv of courses being uploaded to Canvas
        rawTermSisCoursesDF = pd.read_csv(f"{baseExternalInputPath}canvas_course.csv")

        ## Keep only the courses with a status of active and a term_id of the input term
        activeSisCoursesDF = rawTermSisCoursesDF[(rawTermSisCoursesDF["status"] == "active") 
                                                 & (rawTermSisCoursesDF["term_id"] == p2_inputTerm)]

        ## Remove all columns from the active Sis courses df except the course_id column, the start_date, and the end_date
        reducedActiveSisCoursesDF = activeSisCoursesDF[["course_id", "start_date", "end_date"]]

        ## Retrieve the Undg csv of term related Canvas courses from the term path
        rawTermUndgCanvasCoursesDF = pd.read_csv(createCoursesCSV(p1_header, p2_inputTerm))

        ## Determine the grad term
        relevantGradTerm = p2_inputTerm.replace("FA", "GF").replace("SP", "GS").replace("SU", "SG")

        ## Retrieve the grad csv of term related Canvas courses from the term path
        rawTermGradCanvasCoursesDF = pd.read_csv(createCoursesCSV(p1_header, relevantGradTerm))

        ## Combine the Undg and Grad csvs of related canvas courses
        rawTermCanvasCoursesDF = pd.concat([rawTermUndgCanvasCoursesDF, rawTermGradCanvasCoursesDF])

        ## Reset the index to ensure unique indices
        rawTermCanvasCoursesDF.reset_index(drop=True, inplace=True)

        ## Keep only the courses that are active and created_by_sis
        activeCanvasCoursesDF = rawTermCanvasCoursesDF[(rawTermCanvasCoursesDF["status"] != "deleted") 
                                                       & (rawTermCanvasCoursesDF["created_by_sis"] == True)]

        ## Add a Parent_Course_sis_id column to the completeActiveCanvasCoursesDF
        activeCanvasCoursesDF["Parent_Course_sis_id"] = ""

        ## Merge the two DataFrames prioritizing the start_date and end_date from reducedActiveSisCoursesDF
        ## and then using the data from rawCompleteActiveCanvasCoursesDF where the value from reducedActiveSisCoursesDF is nan or ""
        rawCompleteActiveCanvasCoursesDF = pd.merge(
            activeCanvasCoursesDF,
            reducedActiveSisCoursesDF,
            on="course_id",
            how="left",
            suffixes=('', '_sis')
        )


        ## Fill the start_date and end_date in rawCompleteActiveCanvasCoursesDF with the values from reducedActiveSisCoursesDF where they are nan or ""
        rawCompleteActiveCanvasCoursesDF['start_date'] = rawCompleteActiveCanvasCoursesDF['start_date_sis'].combine_first(rawCompleteActiveCanvasCoursesDF['start_date'])
        rawCompleteActiveCanvasCoursesDF['end_date'] = rawCompleteActiveCanvasCoursesDF['end_date_sis'].combine_first(rawCompleteActiveCanvasCoursesDF['end_date'])

        ## For any rows of auxillaryDFDict["Active Outcome Courses DF"] where there is a non nan Parent_Course_sis_id value
        for index, row in auxillaryDFDict["Active Outcome Courses DF"].dropna(subset=['Parent_Course_sis_id']).iterrows():

            ## Define a target course sis id
            targetCourseSisId = None

            ## If there is a parent course id
            if pd.notna(row["Parent_Course_sis_id"]):

                ## Define the target course sis id as the parent course id
                targetCourseSisId = row["Parent_Course_sis_id"]

            ## If there is no parent course id
            else:

                ## Define the target course sis id as the course id
                targetCourseSisId = row['Course_sis_id']

            ## Get the index of the rawCompleteActiveCanvasCoursesDF that matches the course id
            index = rawCompleteActiveCanvasCoursesDF[rawCompleteActiveCanvasCoursesDF["course_id"] == targetCourseSisId].index[0]

            ## Set the Parent_Course_sis_id value in the rawCompleteActiveCanvasCoursesDF to the Parent_Course_sis_id value in the auxillaryDFDict["Active Outcome Courses DF"]
            rawCompleteActiveCanvasCoursesDF.at[index, "Parent_Course_sis_id"] = row["Parent_Course_sis_id"]

        ## Retrieve the all terms file
        allCanvasTermsDf = pd.read_csv(f"{baseLocalInputPath}Canvas_Terms.csv")

        ## Drop the temporary columns
        rawCompleteActiveCanvasCoursesDF.drop(columns=['start_date_sis', 'end_date_sis'], inplace=True)

        ## Keep only the rows that have a canvas course id and a start_date
        completeActiveCanvasCoursesDF = rawCompleteActiveCanvasCoursesDF[
            (
                pd.notna(
                    rawCompleteActiveCanvasCoursesDF["canvas_course_id"]
                    )
             )
            & (
                pd.notna(
                    rawCompleteActiveCanvasCoursesDF["start_date"]
                    )
               )
            ]

        ## For each row in the completeActiveCanvasCoursesDF 
        for index, row in completeActiveCanvasCoursesDF.iterrows():

            ## If there is a parent course sis id
            if (pd.notna(row["Parent_Course_sis_id"]) 
            and row["Parent_Course_sis_id"] != ""
                    ):

                ## Find the index of the parent course sis id
                parent_index = rawCompleteActiveCanvasCoursesDF[rawCompleteActiveCanvasCoursesDF["course_id"] == row["Parent_Course_sis_id"]].index[0]

                ## Set the start_date value from the parent course to the value for the row
                row["start_date"] = rawCompleteActiveCanvasCoursesDF.at[parent_index, "start_date"]

                ## Set the end_date value from the parent course to the value for the row
                row["end_date"] = rawCompleteActiveCanvasCoursesDF.at[parent_index, "end_date"]

            ## Retrieve the Term of the course
            courseTerm = rawCompleteActiveCanvasCoursesDF.at[index, "term_id"]

            ## Get the index of the term within the term_id column of the allCanvasTermsDf
            term_index = allCanvasTermsDf[allCanvasTermsDf["term_id"] == courseTerm].index[0]
                    
            ## If the start date is nan or blank
            if not str(row["start_date"]) or str(row["start_date"]) == "nan":

                ## Set the start_date value from the term to the value for the row
                row["start_date"] = allCanvasTermsDf.at[term_index, "start_date"]

            ## If the end date is nan or blank
            if not str(row["end_date"]) or str(row["end_date"]) == "nan":

                ## Set the end_date value from the term to the value for the row
                row["end_date"] = allCanvasTermsDf.at[term_index, "end_date"]

            ## Get the start date and end date from the row
            start_date = parser.parse(row["start_date"])
            end_date = parser.parse(row["end_date"])

            ## Transform both to %m%d%Y format
            start_date = start_date.strftime("%m/%d/%Y")
            end_date = end_date.strftime("%m/%d/%Y")

            ## Set the start_date and end_date values in the rawCompleteActiveCanvasCoursesDF to the reformatted values
            completeActiveCanvasCoursesDF.at[index, "start_date"] = start_date
            completeActiveCanvasCoursesDF.at[index, "end_date"] = end_date

        ## If the complete active canvas courses df is empty
        if completeActiveCanvasCoursesDF.empty:
            
            ## Return an empty dataframe for the active outcome courses df and the auxillary df dict
            return completeActiveCanvasCoursesDF, auxillaryDFDict
        
        ## Define the term related path to the courses without attached outcomes report 
        outcomeCoursesWithoutAttachmentPath = f"{termPath}{p2_inputTerm}_{p3_targetDesignator}_Courses_Without_Required_Outcome_Attached.csv"

        ## If there is a courses without attached outcomes report for this term
        if os.path.exists(outcomeCoursesWithoutAttachmentPath):

            ## Retrieve the list of courses that do not have their outcome/s attached to a published assignment
            auxillaryDFDict["Outcome Courses Without Attachments DF"] = pd.read_csv(outcomeCoursesWithoutAttachmentPath)
            
        ## Otherwise
        else:
            
            ## Create an empty dataframe for the courses without attached outcomes
            auxillaryDFDict["Outcome Courses Without Attachments DF"] = pd.DataFrame()

        ## Define the term related path to the courses without outcome data report
        outcomeCoursesWithoutDataPath = f"{termPath}{p2_inputTerm}_{p3_targetDesignator}_Outcome_Results_Course_Data.xlsx"

        ## If there is a file at f"{baseLocalInputPath}{targetSchoolYear}\\{p2_inputTerm}\\{p2_inputTerm}_Courses_Without_Required_Outcome_Attached.csv"
        if os.path.exists(outcomeCoursesWithoutDataPath):
            
            ## Retrieve the list of courses that do not have the required outcome Results
            outcomeCoursesDataDF = pd.read_excel(outcomeCoursesWithoutDataPath)

            ## Filter the GE Outcomess data report to only those courses that did not record GE dataf
            auxillaryDFDict["Unassessed Outcome Courses DF"] = outcomeCoursesDataDF[outcomeCoursesDataDF["Assessment_Status"] != "Assessed"]
            
        ## Otherwise
        else:
            
            ## Create an empty dataframe for the courses without outcome data
            auxillaryDFDict["Unassessed Outcome Courses DF"] = pd.DataFrame()
            
        ## Create a new "Course Week" column and a "Course Final Week" column in the complete active canvas courses df by sending the start and end dates to determineCourseWeek
        completeActiveCanvasCoursesDF["Course Week"], completeActiveCanvasCoursesDF["Course Final Week"] = zip(
            *completeActiveCanvasCoursesDF.apply(
                lambda row: determineCourseWeek(
                    row["start_date"]
                    , row["end_date"]
                    )
                , axis=1
                )
            )
        
        ## Return the active outcome courses df, the complete active canvas courses df, and the auxillary df dict
        return completeActiveCanvasCoursesDF, auxillaryDFDict

    except Exception as error:
        error_handler (functionName, p1_ErrorInfo = error)

## This function processes the rows of the CSV file and sends on the relavent data to process_course
def addOutcomeToCourse (targetCourseDataDict
                        , auxillaryDFDict
                        , p1_header
                        ):
    functionName = "Add Outcome/s to courses"

    try:
        
        ## If the targetCourseDataDict's course_sis_id is not in the aux df dict's active outcome course df, or if it is empty, skip it
        if (targetCourseDataDict['course_id'] not in auxillaryDFDict["Active Outcome Courses DF"]["Course_sis_id"].values 
            or not targetCourseDataDict['course_id']):
            return

        ## Get the index of the course_id with the auxillaryDFDict's Active Outcome Courses Df
        targetCourseActiveOutcomeCourseDfIndex = auxillaryDFDict["Active Outcome Courses DF"][auxillaryDFDict["Active Outcome Courses DF"]["Course_sis_id"] == targetCourseDataDict['course_id']].index[0]

        ## Get the row of the targetCourseActiveOutcomeCourseDfIndex as a dict
        targetCourseActiveOutcomeCourseDataDict = auxillaryDFDict["Active Outcome Courses DF"].loc[targetCourseActiveOutcomeCourseDfIndex].to_dict()

        ## For each targetCourseDataDict in our CSV file pull the course sis id column and outcome column names
        ## Sample sess values: FA2022_PHIL2030_01
        ## Sample outcome value: GE_CF4_V1.0
        targetCourseSisId = None
        outcomeKeys = [col for col in targetCourseActiveOutcomeCourseDataDict.keys() if "Outcome" in col and "Area" not in col]

        ## If there is a parent course id
        if (
            pd.notna(
                targetCourseDataDict[
                    'Parent_Course_sis_id'
                    ]
                ) 
            and targetCourseDataDict[
                'Parent_Course_sis_id'
                ] != ""
            ):

                ## Define the target course sis id as the parent course id
                targetCourseSisId = targetCourseDataDict["Parent_Course_sis_id"]

        ## If there is no parent course id
        else:

            ## Define the target course sis id as the course id
            targetCourseSisId = targetCourseDataDict['course_id']
            
        ## Log the start of the process
        logger.info("\n     Course:" + targetCourseDataDict['course_id'])

        ## Create the base and specific course API urls
        baseCourseApiUrl = CoreCanvasAPIUrl + "courses/sis_course_id:" + targetCourseSisId
        contentMigrationApiUrl = baseCourseApiUrl + "/content_migrations"
        
        ## Make a content migration API call to find out what content has already been copied to the course
        courseMigrationsObject = makeApiCall (p1_header = p1_header, p1_apiUrl = contentMigrationApiUrl)
        
        ## If the API status code is anything other than 200 it is an error, so log it and skip
        if (courseMigrationsObject.status_code != 200):
            logger.error("\nCourse Error: " + str(courseMigrationsObject.status_code))
            logger.error(contentMigrationApiUrl)
            logger.error(courseMigrationsObject.url)
            return
        
        ## If the API status code is 200, save the result as courseMigrations
        courseMigrations = courseMigrationsObject.json()
        
        ## For each outcome in the targetCourseDataDict
        for outcome in outcomeKeys:
            
            ## If the outcome is empty skip it
            if pd.isna(targetCourseActiveOutcomeCourseDataDict[outcome]) or not targetCourseActiveOutcomeCourseDataDict[outcome] or not outcome or pd.isna(outcome):
                continue

            ## Get the canvas course id from the outcomeCourseDict
            outcomeCourseCanvasId = auxillaryDFDict[
                "Outcome Course Dict"
                ][
                    targetCourseActiveOutcomeCourseDataDict[
                        outcome
                        ]
                    ]
            
            ## If a migration that has settings has the outcome name in the migration's setting's source course name
            if any([migration['settings']['source_course_id'] == outcomeCourseCanvasId for migration in courseMigrations if 'settings' in migration.keys()]):

                ## Log the fact that the outcome has already been copied in
                logger.info(f"\n {targetCourseSisId} already has {targetCourseActiveOutcomeCourseDataDict[outcome]}")

                ## Skip to the next outcome
                continue

            ## Create the API Payload from the outcome sis id
            payload = {'migration_type': 'course_copy_importer', 'settings[source_course_id]': [outcomeCourseCanvasId], 'selective_import': True}
                
            ## Make the API call and save the result as course_object
            #courseCopyObject = requests.post(contentMigrationApiUrl, headers = header, params = payload)
            courseCopyObject = makeApiCall (p1_header = p1_header, p1_apiUrl = contentMigrationApiUrl, p1_payload = payload, apiCallType = "post")
            
            ## Turn the text of the API call into a json object
            courseCopy = courseCopyObject.json()

            ## Define the list items endpoint api url using the migration id
            listSelectiveImportItemsApiUrl = f"{contentMigrationApiUrl}/{courseCopy['id']}/selective_data"

            ## Make a get request to the list items endpoint
            listSelectiveImportItemsObject = requests.get(listSelectiveImportItemsApiUrl, headers = p1_header)

            ## If the API status code is anything other than 200 it is an error, so log it and skip
            if (listSelectiveImportItemsObject.status_code != 200):
                logger.error("\nCourse Error: " + str(listSelectiveImportItemsObject.status_code))
                logger.error(listSelectiveImportItemsApiUrl)
                logger.error(listSelectiveImportItemsObject.url)
                continue
            
            ## Turn the text of the API call into a json object
            listSelectiveImportItems = listSelectiveImportItemsObject.json()

            ## Find the list item that has the learning_outcomes as the value of the type key
            learningOutcomesListItem = [item for item in listSelectiveImportItems if item['type'] == 'learning_outcomes'][0]

            ## Save the value of the property key of the learning_outcomes list item as as the selected import item
            selectedImportItem = learningOutcomesListItem['property']

            ## Define a payload with the selected import item = 1
            updateContentMigrationApiPayload = {selectedImportItem: 1}

            ## Define the update content migration api url using the course copy id
            updateContentMigrationApiUrl = f"{contentMigrationApiUrl}/{courseCopy['id']}"

            ## Make a put request to the update content migration api url with the update content migration api payload
            updateContentMigrationObject = requests.put(updateContentMigrationApiUrl, headers = p1_header, params = updateContentMigrationApiPayload)

            ## If the API status code is anything other than 200 it is an error, so log it and skip
            if (updateContentMigrationObject.status_code != 200):
                logger.error("\nCourse Error: " + str(updateContentMigrationObject.status_code))
                logger.error(updateContentMigrationApiUrl)
                logger.error(updateContentMigrationObject.url)
                continue

            ## Log the fact that the outcome has been copied in
            logger.info(f"\n {targetCourseSisId} has {targetCourseActiveOutcomeCourseDataDict[outcome]}")



    except Exception as error:
        error_handler (functionName, error)

## This function removes any outcomes that don't have corresponding courses
def removeMissingOutcomes (p1_activeOutcomeCourseDf, p1_uniqueOutcomes, p1_outcomeCourseDict):
    functionName = "Remove Missing Outcomes"

    try:

        ## Get a list of all unique outcomes that are not in the keys of the outcomeCourseDict
        missingOutcomes = [outcome for outcome in p1_uniqueOutcomes if outcome not in p1_outcomeCourseDict.keys()]
        
        ## If there are missing outcomes
        if missingOutcomes:
            
            ## For each row of the active outcome course df
            for index, row in p1_activeOutcomeCourseDf.iterrows():
                
                ## Create a list of the outcome columns in the row
                outcomesColumns = [col for col in row.keys() if "Outcome" in col and "Area" not in col]
                
                ## For each outcome column in the row
                for outcome in outcomesColumns:
                    
                    ## If the outcome is in the missing outcomes list
                    if row[outcome] in missingOutcomes:
                        
                        ## Replace it with a blank string
                        p1_activeOutcomeCourseDf.loc[index, outcome] = ""

                        ## Send an error email about the missing outcome
                        error_handler (functionName, f"Outcome Missing Import Course: {row[outcome]}")
                        
                ## If all outcome values in the row are blank strings
                if all([pd.isna(row[outcome]) for outcome in outcomesColumns]):
                    
                    ## Drop the row
                    p1_activeOutcomeCourseDf.drop(index, inplace=True)

        ## Return the active outcome course df
        return p1_activeOutcomeCourseDf
    
    except Exception as error:
        error_handler (functionName, error)

## This function returns a dict with the course id of the course named after each outcome
def getUniqueOutcomesAndOutcomeCoursesDict (p1_activeOutcomeCourseDf, p2_header):
    functionName = "Get Unique Outcomes And Outcome Courses Dict"
    
    try:

        ## Make a df with one collumn where all outcome columns that don't have area in the name are stacked
        outcomesDF = p1_activeOutcomeCourseDf[[col for col in p1_activeOutcomeCourseDf.columns if "Outcome" in col and "Area" not in col]].stack().reset_index(drop=True)
        
        ## Make a list of the unique outcomes that are not blank
        uniqueOutcomes = [outcome for outcome in outcomesDF.unique() if outcome]

        ## Open the relevant All_Canvas_Courses.csv as a df
        allCanvasCoursesDF = pd.read_csv(createCoursesCSV(p2_header, p1_inputTerm = "All"))
        
        ## Replace all '​' in the long_name column with ''
        allCanvasCoursesDF['long_name'] = allCanvasCoursesDF['long_name'].str.replace('​', '')

        ## Make a dict to hold the course id of the course named after each outcome
        outcomeCourseDict = {}
        
        ## For each outcome in the unique outcomes list
        for outcome in uniqueOutcomes:
            
            ## Define a variable to hold the courseIndex
            courseIndex = None

            try: ## Irregular try clause, do not comment out in testing
            
                ## Find the index of the course with the outcome as the name
                courseIndex = allCanvasCoursesDF[allCanvasCoursesDF['long_name'] == outcome].index[0]
            
            ## If no course is found with the outcome as the long name
            except: ## Irregular except clause, do not comment out in testing
                
                ## Find the index of the course with the outcome as the short name
                courseIndex = allCanvasCoursesDF[allCanvasCoursesDF['short_name'] == outcome].index[0]
                
            ## Finally
            finally:

                ## If there is still no course index
                if courseIndex == None:

                    ## Log the fact that the course was not found
                    logger.error(f"\nOutcome not found: {outcome}")
                    
                    ## Email the fact that the course was not found
                    error_handler (functionName, f"Outcome course not found: {outcome}")

                    ## Skip to the next outcome
                    continue
                
            ## Use the course index to get the canvas course id from the course with the outcome as the name
            courseCanvasId = allCanvasCoursesDF.loc[courseIndex, 'canvas_course_id']
                
            ## Add the course id to the outcomeCourseDict
            outcomeCourseDict[outcome] = courseCanvasId

        ## Return the outcomeCourseDict
        return uniqueOutcomes, outcomeCourseDict    
    
    except Exception as error:
        error_handler (functionName, error)

# This function checks whether a term's outcome courses have their associated outcomes and adds them if they don't
def termOutcomeExporter(p1_inputTerm, p1_targetDesignator):
    functionName = "outcome_exporter"

    try:    

        ## Define the API Call header using the retreived Canvas Token
        header = {'Authorization' : f"Bearer {canvasAccessToken}"}

        ## Define the target school year
        targetSchoolYear = None

        ## Define the current school year by whether it is before or during/after september
        if re.search("AF|FA|GF", p1_inputTerm):
            ## Fall terms are the first terms of a new school year so FA20 is part of the 2020-21 school year.
            targetSchoolYear = f"{century}{p1_inputTerm[2:]}-{int(p1_inputTerm[2:]) + 1}"
        if re.search("SP|GS|AS|SG|SA|SU", p1_inputTerm):
            ## Spring and Summer terms belong in the same school year as the fall terms before them, so SP21 is part of thZe same 2020-21 school year as FA20.
            targetSchoolYear = f"{century}{int(p1_inputTerm[2:]) - 1}-{p1_inputTerm[2:]}"

        ## Make a list to hold the active add outcome threads
        activeThreads = []

        ## Retrieve the data for determining and sending out relevant communication
        completeActiveCanvasCoursesDF, auxillaryDFDict = (
            retrieveDataForRelevantCommunication(
                p2_inputTerm = p1_inputTerm
                , p3_targetDesignator = p1_targetDesignator
                , p1_header = header
                )
            )

        ## If the complete active canvas courses df is empty
        if completeActiveCanvasCoursesDF.empty:

            ## Log the fact that there are no active courses
            logger.info(f"\nNo {p1_targetDesignator} active courses within {p1_inputTerm}")

            ## Return
            return

        ## For each row in the active outcome course df
        for index, row in completeActiveCanvasCoursesDF.iterrows():

            ## If the course is in the auxillaryDFDict active
            
            ## Create an add outcome to course thread
            addOutcomeThread = threading.Thread(target=addOutcomeToCourse
                                                , args=(row
                                                        , auxillaryDFDict
                                                        , header
                                                        )
                                                )
            
            ## Start the thread
            addOutcomeThread.start()
            
            ## Add the thread to the active threads list
            activeThreads.append(addOutcomeThread)
            
        ## For each active thread
        for thread in activeThreads:
            
            ## Wait for the thread to finish
            thread.join()    
     
    except Exception as error:
        error_handler (functionName, error)

if __name__ == "__main__":

    ## Start and download the Canvas report
    termOutcomeExporter (p1_inputTerm = input("Enter the desired term in four character format (FA20, SU20, SP20): ")
        , p1_targetDesignator = input("Enter the desired target designator (GE, I-EDUC, U-ENGR): ")
        )

    input("Press enter to exit")