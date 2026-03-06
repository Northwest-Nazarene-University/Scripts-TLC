# Author: Bryce Miller - brycezmiller@nnu.edu
# Last Updated by: Bryce Miller

## Import Generic Moduels
import os, sys, threading, math
from datetime import datetime, timedelta
from dateutil import parser
import pandas as pd

## Add Script repository to syspath
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

# Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = __file__.replace(".py", "")

scriptPurpose = r"""
The Outcome Exporter script is to copy the most recent relative outcome/s into the courses that need them.
"""
externalRequirements = r"""
To function properly this script requires a spreadsheet of the most recent outcomes and the courses they are assigned to.
"""

## Initialize LocalSetup and resource helpers
try: ## Irregular try clause, do not comment out in testing
    from Local_Setup import LocalSetup
    from TLC_Common import makeApiCall, flattenApiObjectToJsonList
    from Canvas_Report import CanvasReport
    from Error_Email import errorEmail

except ImportError:
    from ResourceModules.Local_Setup import LocalSetup
    from ResourceModules.TLC_Common import makeApiCall, flattenApiObjectToJsonList
    from ResourceModules.Canvas_Report import CanvasReport
    from ResourceModules.Error_Email import errorEmail

# Create LocalSetup and localSetup.logger
localSetup = LocalSetup(datetime.now(), __file__)

## Bring in action module functions
from Outcome_Attachment_Report import termOutcomeAttachmentReport
from Outcome_Results_Report import termProcessOutcomeResults
from Common_Configs import coreCanvasApiUrl

## Setup error handlerF
errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)

todaysDateDateTime = datetime.now()

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
                                          ):
    
    functionName = "Retrieve Data For Relevant Communication"

    ## Define an auxillary data dict and auxillary df dict
    auxillaryDFDict = {}
    completeActiveCanvasCoursesDF = pd.DataFrame()
    
    try:

        ## Get the year of the term
        termYear = int(f"{localSetup.dateDict['century']}{p2_inputTerm[2:]}")
        termPrefix = p2_inputTerm[:2]
        termWord = localSetup._determineTermName(termPrefix)

        ## Retrieve the df of Active outcome courses which includes course code, required outcome/s, and the relevant instructor name/s, id/s, and email/s
        rawActiveOutcomeCourseDf = CanvasReport.getActiveOutcomeCoursesDf(localSetup, p2_inputTerm, p3_targetDesignator)

        ## If the raw active outcome course df is empty
        if rawActiveOutcomeCourseDf.empty:

            ## Return an empty dataframe for the active outcome courses df and the auxillary df dict
            return rawActiveOutcomeCourseDf, auxillaryDFDict
        
        ## Make a list of the unique outcomes that are not blank 
        ## and a dict to hold the course id of the course named after each outcome
        auxillaryDFDict["Unique Outcomes"], auxillaryDFDict["Outcome Canvas Data Dict"] = getUniqueOutcomesAndOutcomeCoursesDict(p2_inputTerm, rawActiveOutcomeCourseDf, p3_targetDesignator)
        
        ## Remove any outcomes that don't have corresponding courses
        auxillaryDFDict["Active Outcome Courses DF"] = removeMissingOutcomes (
            rawActiveOutcomeCourseDf
            , auxillaryDFDict["Unique Outcomes"]
            , auxillaryDFDict["Outcome Canvas Data Dict"]
            )
        
        ## Retrieve the csv of courses being uploaded to Canvas
        rawTermSisCoursesDF = pd.read_csv(f"{localSetup.getExternalResourcePath('SIS')}canvas_course.csv")

        ## Keep only the courses with a status of active and a term_id of the input term
        activeSisCoursesDF = rawTermSisCoursesDF[(rawTermSisCoursesDF["status"] == "active") 
                                                 & (rawTermSisCoursesDF["term_id"] == p2_inputTerm)]

        ## Remove all columns from the active Sis courses df except the course_id column, the start_date, and the end_date
        reducedActiveSisCoursesDF = activeSisCoursesDF[["course_id", "start_date", "end_date"]]

        ## Get the raw term canvas courses df
        rawTermCanvasCoursesDF = CanvasReport.getCoursesDf(localSetup, p2_inputTerm)

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
            if not pd.isna(row["Parent_Course_sis_id"]) and row["Parent_Course_sis_id"] not in ["", None]:

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
        allCanvasTermsDf = CanvasReport.getTermsDf(localSetup)

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
            if (
               not pd.isna(row["Parent_Course_sis_id"]) 
                and row["Parent_Course_sis_id"] not in ["", None]
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
            
        ## Define the term related path to the outcome attachment report
        termOutcomeAttachmentReportPath = termOutcomeAttachmentReport(p2_inputTerm, p3_targetDesignator)
        auxillaryDFDict["Outcome Courses Without Attachments DF"] = pd.read_csv(termOutcomeAttachmentReportPath)

        ## Define the term related path to the outcome results report
        termProcessOutcomeResultsPath = termProcessOutcomeResults(p2_inputTerm, p3_targetDesignator)[0]
        outcomeCoursesDataDF = pd.read_excel(termProcessOutcomeResultsPath)

        ## Create a df of outcome courses that have not been assessed
        auxillaryDFDict["Unassessed Outcome Courses DF"] = outcomeCoursesDataDF[outcomeCoursesDataDF["Assessment_Status"] != "Assessed"]
            
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

    except Exception as Error:
        errorHandler.sendError(functionName, Error)
        return completeActiveCanvasCoursesDF, auxillaryDFDict 

## This function processes the rows of the CSV file and sends on the relavent data to process_course
def addOutcomeToCourse (targetCourseDataDict
                        , auxillaryDFDict
                        ):
    functionName = "Add Outcome/s to courses"

    try:
        
        ## If the targetCourseDataDict's course_sis_id is not in the aux df dict's active outcome course df,
        ## or if it is empty, skip it
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
            pd.isna(targetCourseDataDict["Parent_Course_sis_id"]) 
            and targetCourseDataDict["Parent_Course_sis_id"] not in ["", None]
            ):

                ## Define the target course sis id as the parent course id
                targetCourseSisId = targetCourseDataDict["Parent_Course_sis_id"]

        ## If there is no parent course id
        else:

            ## Define the target course sis id as the course id
            targetCourseSisId = targetCourseDataDict['course_id']
            
        ## Log the start of the process
        localSetup.logger.info("\n     Course:" + targetCourseDataDict['course_id'])

        ## Create the base course API urls
        baseCourseApiUrl = f"{coreCanvasApiUrl}courses/sis_course_id:{targetCourseSisId}"
        # contentMigrationApiUrl = baseCourseApiUrl + "/content_migrations"
        
        # ## Make a content migration API call to find out what content has already been copied to the course
        # courseMigrationsObject, _ = makeApiCall(localSetup, p1_apiUrl=contentMigrationApiUrl)
        
        # ## If the API status code is anything other than 200 it is an error, so log it and skip
        # if (courseMigrationsObject.status_code != 200):
        #     localSetup.logger.error("\nCourse Error: " + str(courseMigrationsObject.status_code))
        #     localSetup.logger.error(contentMigrationApiUrl)
        #     localSetup.logger.error(courseMigrationsObject.url)
        #     return
        
        # ## If the API status code is 200, save the result as courseMigrations
        # courseMigrations = courseMigrationsObject.json()
        
        ## For each outcome in the targetCourseDataDict
        for outcome in outcomeKeys:
            
            ## If the outcome is empty skip it
            if pd.isna(targetCourseActiveOutcomeCourseDataDict[outcome]) or not targetCourseActiveOutcomeCourseDataDict[outcome] or not outcome or pd.isna(outcome):
                continue

            ## Get the outcome canvas data dict from the auxillary df dict
            outcomeCanvasData = auxillaryDFDict[
                "Outcome Canvas Data Dict"
                ][
                    targetCourseActiveOutcomeCourseDataDict[
                        outcome
                        ]
                    ]

            ## Define the API url to get the outcome groups of the course
            courseOutcomeGroupsApiUrl = f"{baseCourseApiUrl}/outcome_groups"

            ## Make the API call to get the outcome groups of the course
            courseOutcomeGroupsObject, _ = makeApiCall(localSetup, p1_apiUrl=courseOutcomeGroupsApiUrl)

            ## If the API status code is anything other than 200 it is an error, so log it and skip
            if (courseOutcomeGroupsObject.status_code != 200):
                localSetup.logger.error("\nCourse Error: " + str(courseOutcomeGroupsObject.status_code))
                localSetup.logger.error(courseOutcomeGroupsApiUrl)
                localSetup.logger.error(courseOutcomeGroupsObject.url)
                continue

            ## Define a variable to hold the whether the course already has the outcome group and another to hold its canvas id
            outcomeGroupAlreadyInCourse = False
            outcomeGroupCanvasIdInCourse = None

            ## Define a variable to hold the outcome group id of the course itself in case the outcome group needs to be added to the course
            courseOutcomeGroupCanvasId = None

            ## For each outcome group in the course outcome groups object
            for courseOutcomeGroup in courseOutcomeGroupsObject.json():
                
                ## If the title contains the target sis id 
                if targetCourseSisId in courseOutcomeGroup['title']:
                    ## Set the course outcome group canvas id to the id of the outcome group
                    courseOutcomeGroupCanvasId = courseOutcomeGroup['id']
                    if outcomeCanvasData["Outcome Group is Root Account"]:
                        outcomeGroupAlreadyInCourse = True
                        outcomeGroupCanvasIdInCourse = courseOutcomeGroup['id']


                ## Else if the the title is equal to the outcome group title from the outcome canvas data dict
                elif courseOutcomeGroup['title'] == outcomeCanvasData["Outcome Group Title"] or courseOutcomeGroup['title'] == outcomeCanvasData["Outcome Group Id"]:
                    ## Set the outcome group already in course variable to true
                    outcomeGroupAlreadyInCourse = True
                    outcomeGroupCanvasIdInCourse = courseOutcomeGroup['id']
                    ## Break out of the loop
                    break

            if courseOutcomeGroupCanvasId is None:
                rootOutcomeGroupApiUrl = f"{baseCourseApiUrl}/root_outcome_group"

                rootOutcomeGroupObject, _ = makeApiCall(localSetup, p1_apiUrl=rootOutcomeGroupApiUrl)

                if (rootOutcomeGroupObject.status_code != 200):
                    localSetup.logger.error("\nCourse Error: " + str(rootOutcomeGroupObject.status_code))
                    localSetup.logger.error(rootOutcomeGroupApiUrl)
                    localSetup.logger.error(rootOutcomeGroupObject.url)
                    continue

                courseOutcomeGroupCanvasId = rootOutcomeGroupObject.json()['id']

            ## If the outcome group is not already in the course
            if not outcomeGroupAlreadyInCourse:

                ## Define the API url to add the outcome group to the course using the course outcome group canvas id
                addOutcomeGroupToCourseApiUrl = f"{baseCourseApiUrl}/outcome_groups/{courseOutcomeGroupCanvasId}/import"

                ## Define the API payload to add the outcome group to the course
                addOutcomeGroupToCourseApiPayload = {
                    "source_outcome_group_id": outcomeCanvasData["Outcome Group Id"],
                    }

                ## Make the API call to add the outcome group to the course
                addOutcomeGroupToCourseObject, _ = makeApiCall(
                    localSetup, 
                    p1_apiUrl=addOutcomeGroupToCourseApiUrl, 
                    p1_payload=addOutcomeGroupToCourseApiPayload, 
                    p1_apiCallType="post"
                    )

                ## If the API status code is anything other than 200 it is an error, so log it and skip
                if (addOutcomeGroupToCourseObject.status_code != 200):
                    localSetup.logger.error("\nCourse Error: " + str(addOutcomeGroupToCourseObject.status_code))
                    localSetup.logger.error(addOutcomeGroupToCourseApiUrl)
                    localSetup.logger.error(addOutcomeGroupToCourseObject.url)
                    continue

                ## Log the fact that the outcome group has been added to the course
                localSetup.logger.info(f"\n {targetCourseSisId} has been added outcome group {outcomeCanvasData['Outcome Group Title']}")

                ## Retrieve the ooutcomeGroupCanvasIdInCourse from the API call response
                outcomeGroupCanvasIdInCourse = addOutcomeGroupToCourseObject.json()['id']

            ## Define the API url to add the outcome to the course outcome group
            addOutcomeToCourseApiUrl = f"{baseCourseApiUrl}/outcome_groups/{outcomeGroupCanvasIdInCourse}/outcomes/{outcomeCanvasData['Outcome Canvas Id']}"

            ## Make the API call to add the outcome to the course
            addOutcomeToCourseObject, _ = makeApiCall(localSetup, p1_apiUrl=addOutcomeToCourseApiUrl, p1_apiCallType="put")

            ## If the API status code is anything other than 200 it is an error, so log it and skip
            if (addOutcomeToCourseObject.status_code != 200):
                localSetup.logger.error("\nCourse Error: " + str(addOutcomeToCourseObject.status_code))
                localSetup.logger.error(addOutcomeToCourseApiUrl)
                localSetup.logger.error(addOutcomeToCourseObject.url)
                continue

            ## Log the fact that the outcome has been added to the course
            localSetup.logger.info(f"\n {targetCourseSisId} has had outcome {targetCourseActiveOutcomeCourseDataDict[outcome]} added")
            
            ## If a migration that has settings has the outcome name in the migration's setting's source course name and has a status of completed
            # if any([migration['settings']['source_course_id'] == outcomeCourseCanvasId and migration['workflow_state'] == 'completed' for migration in courseMigrations if 'settings' in migration.keys()]):

            #     ## Log the fact that the outcome has already been copied in
            #     localSetup.logger.info(f"\n {targetCourseSisId} already has {targetCourseActiveOutcomeCourseDataDict[outcome]}")

            #     ## Skip to the next outcome
            #     continue

            ## Create the API Payload from the outcome sis id
            #payload = {'migration_type': 'course_copy_importer', 'settings[source_course_id]': [outcomeCourseCanvasId], 'selective_import': True}
                
            ## Make the API call and save the result as course_object
            # courseCopyObject, _ = makeApiCall(localSetup, p1_apiUrl=contentMigrationApiUrl, p1_payload=payload, p1_apiCallType="post")
            
            # ## Turn the text of the API call into a json object
            # courseCopy = courseCopyObject.json()

            # ## Define the list items endpoint api url using the migration id
            # listSelectiveImportItemsApiUrl = f"{contentMigrationApiUrl}/{courseCopy['id']}/selective_data"

            # ## Make a get request to the list items endpoint
            # listSelectiveImportItemsObject, _ = makeApiCall(localSetup, p1_apiUrl=listSelectiveImportItemsApiUrl)

            # ## If the API status code is anything other than 200 it is an error, so log it and skip
            # if (listSelectiveImportItemsObject.status_code != 200):
            #     localSetup.logger.error("\nCourse Error: " + str(listSelectiveImportItemsObject.status_code))
            #     localSetup.logger.error(listSelectiveImportItemsApiUrl)
            #     localSetup.logger.error(listSelectiveImportItemsObject.url)
            #     continue
            
            # ## Turn the text of the API call into a json object
            # listSelectiveImportItems = listSelectiveImportItemsObject.json()

            # ## Find the list item that has the learning_outcomes as the value of the type key
            # learningOutcomesListItem = [item for item in listSelectiveImportItems if item['type'] == 'learning_outcomes'][0]

            # ## Save the value of the property key of the learning_outcomes list item as as the selected import item
            # selectedImportItem = learningOutcomesListItem['property']

            # ## Define a payload with the selected import item = 1
            # updateContentMigrationApiPayload = {selectedImportItem: 1}

            # ## Define the update content migration api url using the course copy id
            # updateContentMigrationApiUrl = f"{contentMigrationApiUrl}/{courseCopy['id']}"

            # ## Make a put request to the update content migration api url with the update content migration api payload
            # updateContentMigrationObject, _ = makeApiCall(localSetup, p1_apiUrl=updateContentMigrationApiUrl, p1_payload=updateContentMigrationApiPayload, p1_apiCallType="put")

            # ## If the API status code is anything other than 200 it is an error, so log it and skip
            # if (updateContentMigrationObject.status_code != 200):
            #     localSetup.logger.error("\nCourse Error: " + str(updateContentMigrationObject.status_code))
            #     localSetup.logger.error(updateContentMigrationApiUrl)
            #     localSetup.logger.error(updateContentMigrationObject.url)
            #     continue

            # ## Log the fact that the outcome has been copied in
            # localSetup.logger.info(f"\n {targetCourseSisId} has {targetCourseActiveOutcomeCourseDataDict[outcome]}")



    except Exception as Error:
        errorHandler.sendError (functionName, Error)

## This function removes any outcomes that don't have corresponding courses
def removeMissingOutcomes (p1_activeOutcomeCourseDf, p1_uniqueOutcomes, p1_outcomeCanvasDataDict):
    functionName = "Remove Missing Outcomes"

    try:

        ## Get a list of all unique outcomes that are not in the keys of the outcomeCanvasDataDict
        missingOutcomes = [outcome for outcome in p1_uniqueOutcomes if outcome not in p1_outcomeCanvasDataDict.keys()]
        
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
                        errorHandler.sendError (f"External Input Error: {functionName}", f"Outcome Missing Import Course: {row[outcome]}")
                        
                ## If all outcome values in the row are blank strings
                if all([pd.isna(row[outcome]) for outcome in outcomesColumns]):
                    
                    ## Drop the row
                    p1_activeOutcomeCourseDf.drop(index, inplace=True)

        ## Return the active outcome course df
        return p1_activeOutcomeCourseDf
    
    except Exception as Error:
        errorHandler.sendError (functionName, Error)

## This function returns a dict with the course id of the course named after each outcome
def getUniqueOutcomesAndOutcomeCoursesDict (p3_inputTerm, p1_activeOutcomeCourseDf, p4_targetDesignator):
    functionName = "Get Unique Outcomes And Outcome Courses Dict"
    
    try:

        ## Make a df with one collumn where all outcome columns that don't have area in the name are stacked
        targetOutcomesDF = p1_activeOutcomeCourseDf[[col for col in p1_activeOutcomeCourseDf.columns if "Outcome" in col and "Area" not in col]].stack().reset_index(drop=True)
        
        ## Make a list of the unique outcomes that are not blank
        uniqueTargetOutcomes = [outcome for outcome in targetOutcomesDF.unique() 
                          if (
                              pd.notna(outcome)
                              and str(outcome).strip() not in ("", "nan", "none", "NaN", "None")
                              )
                          ]

        ## Retrieve the Automated Outcome Tool Variables excel file as a df    
        automatedOutcomeToolVariablesDf = pd.read_excel(
            os.path.join(
                localSetup.getExternalResourcePath("TLC"), 
                "Automated Outcome Tool Variables.xlsx"
                )
        )

        ## Get the account name associated with the target designator
        targetAccountName = automatedOutcomeToolVariablesDf.loc[
            automatedOutcomeToolVariablesDf["Target Designator"] == p4_targetDesignator, 
            "Outcome Location Account Name"
            ].values[0]

        ## Open the p4_targetDesignator relevant outcome df
        targetDesignatorCanvasOutcomeDf = CanvasReport.getOutcomesDf(localSetup, p3_inputTerm, targetAccountName, p4_targetDesignator)

        ## Open the accounts df
        accountsDf = CanvasReport.getAccountsDf(localSetup)

        ## Get the target account id from the accounts df using the target account name
        targetCanvasAccountId = (
            1 if targetAccountName == "NNU"
            else accountsDf.loc[accountsDf["name"] == targetAccountName, "canvas_account_id"].values[0]
            )

        ## Define a dict to hold tail of the api url to add the outcome to a course
        uniqueOutcomesCanvasData = {}

        ## For each outcome in the unique target outcomes list
        for outcome in uniqueTargetOutcomes:

            ## Get the index of the outcome from the title column of the targetDesignatorCanvasOutcomeDf
            outcomeIndexSearch = targetDesignatorCanvasOutcomeDf[targetDesignatorCanvasOutcomeDf['title'] == outcome].index

            ## If the outcomeIndexs is empty
            if outcomeIndexSearch.empty:
                
                ## Log the fact that the outcome was not found
                localSetup.logger.error(f"\nOutcome not found: {outcome}")
                    
                ## Email the fact that the outcome was not found
                errorHandler.sendError (f"External Input Error: {functionName}", f"Outcome not found: {outcome}")
                ## Skip to the next outcome
                continue

            ## Use the outcome index to get the vendor_guid from the outcome with the outcome as the title
            outcomeParentGuid = targetDesignatorCanvasOutcomeDf.loc[outcomeIndexSearch[0], 'parent_guids']

            ## Define the API url to add the outcome to the course using the target canvas account id 
            ## and the outcome parent guid and outcome vendor guid
            outcomeGroupsApiUrl = f"{coreCanvasApiUrl}accounts/{targetCanvasAccountId}/outcome_groups"

            ## Make an API call to get the outcome groups in the target account
            outcomeGroupsObject, outcomeGroupsObjectList = makeApiCall(localSetup, p1_apiUrl=outcomeGroupsApiUrl)

            ## If the API status code is anything other than 200 it is an error, so log it and skip`
            if (outcomeGroupsObject.status_code != 200):
                localSetup.logger.error("\nCourse Error: " + str(outcomeGroupsObject.status_code))
                localSetup.logger.error(outcomeGroupsApiUrl)
                localSetup.logger.error(outcomeGroupsObject.url)
                continue

            ## If the the api response was paginated a list of the responses will have been returned
            outcomeGroupsJsonList = []
            if outcomeGroupsObjectList:
                ## Paginated: flatten all pages
                outcomeGroupsJsonList = flattenApiObjectToJsonList(
                    localSetup,
                    outcomeGroupsObjectList,
                    outcomeGroupsApiUrl
                )
            else:
                ## Non-paginated: just use the single response's json
                singlePageData = outcomeGroupsObject.json()
                if isinstance(singlePageData, list):
                    outcomeGroupsJsonList = singlePageData
                else:
                    outcomeGroupsJsonList = [singlePageData]


            ## Define a variable to hold the outcome group Canvas id
            outcomeGroupCanvasId = None

            ## For each outcome group in the outcome groups json list
            for outcomeGroup in outcomeGroupsJsonList:
                
                ## If the outcomeParentGuid is nan
                if (
                    pd.isna(outcomeParentGuid) 
                    or not outcomeParentGuid 
                    or str(outcomeParentGuid).strip() in ("", "nan", "none", "NaN", "None")
                    ):
                    ## The outcome is in the root outcome group, so test if the title is equal to the target account name
                    if outcomeGroup['title'] == targetAccountName:
                        ## Set the outcome group canvas id to the id of the outcome group
                        outcomeGroupCanvasId = outcomeGroup['id']
                        ## Break out of the loop
                        break
                
                ## If the outcome group's vendor guid is equal to the outcome parent guid
                if outcomeGroup['vendor_guid'] == outcomeParentGuid:
                    ## Set the outcome group canvas id to the id of the outcome group
                    outcomeGroupCanvasId = outcomeGroup['id']
                    ## Break out of the loop
                    break

            ## Define an outcome api url by tagging on the outcome group canvas id 
            ## and /outcomes to the end of the outcome groups api url
            outcomesApiUrl = f"{outcomeGroupsApiUrl}/{outcomeGroupCanvasId}/outcomes"

            ## Make an API call to the outcomes api url to get the outcomes in the outcome group
            outcomesObjects, _ = makeApiCall(localSetup, p1_apiUrl=outcomesApiUrl)

            ## If the API status code is anything other than 200 it is an error, so log it and skip
            if (outcomesObjects.status_code != 200):
                localSetup.logger.error("\nCourse Error: " + str(outcomesObjects.status_code))
                localSetup.logger.error(outcomesApiUrl)
                localSetup.logger.error(outcomesObjects.url)
                continue

            ## Use the outcome index to get the vendor_guid from the outcome with the outcome as the title
            outcomeVendorGuid = targetDesignatorCanvasOutcomeDf.loc[outcomeIndexSearch[0], 'vendor_guid']

            ## Define a variable to hold the outcome canvas id
            outcomeCanvasId = None

            ## For each outcome in the outcomes object
            for outcomeData in [
                outcomeObject["outcome"] 
                for outcomeObject in outcomesObjects.json() 
                if "outcome" in outcomeObject
                ]:
                ## If the outcome's vendor guid is equal to the outcome vendor guid
                ## Or if the outcomeData id is equal to the outcome vendor guid when split by ':'
                if (
                    outcomeData['vendor_guid'] == outcomeVendorGuid 
                    or str(outcomeData["id"]) == str(outcomeVendorGuid.split(':')[1])
                    ):
                    ## Set the outcome canvas id to the id of the outcome
                    outcomeCanvasId = outcomeData['id']
                    ## Break out of the loop
                    break

            ## If the outcome canvas id is not found
            if outcomeCanvasId is None:
                ## Log the fact that the outcome was not found in the outcome group
                localSetup.logger.error(f"\nOutcome not found in outcome group: {outcome}")
                    
                ## Email the fact that the outcome was not found in the outcome group
                errorHandler.sendError (f"External Input Error: {functionName}", f"Outcome not found in outcome group: {outcome}")
                ## Skip to the next outcome
                continue

            ## Use the outcome Parent guide to find the index of the outcome group 
            ## with the same parent guid in the outcome groups object for the target account
            outcomeGroupIndexSearch = (
                targetDesignatorCanvasOutcomeDf[targetDesignatorCanvasOutcomeDf['vendor_guid'] == outcomeParentGuid].index
                )

            ## Use the outcome group index to get the outcome group title 
            ## from the outcome group column in the targetDesignatorCanvasOutcomeDf
            outcomeGroupTitle = (
                targetAccountName if str(outcomeParentGuid).strip() == "nan" 
                else targetDesignatorCanvasOutcomeDf.loc[outcomeGroupIndexSearch[0], 'title']
                )

            ## Make a dict for the outcome with the outcome group title and outcome canvas id
            uniqueOutcomesCanvasData[outcome] = {
                "Outcome Group Title": outcomeGroupTitle,
                "Outcome Canvas Id": outcomeCanvasId,
                "Outcome Group Id": outcomeGroupCanvasId,
                "Outcome Group is Root Account" : True if outcomeGroupTitle == targetAccountName else False
            }


        return uniqueTargetOutcomes, uniqueOutcomesCanvasData

        # ## Make a dict to hold the course id of the course named after each outcome
        # outcomeCanvasDataDict = {}
        
        # ## For each outcome in the unique outcomes list
        # for outcome in uniqueOutcomes:
            
        #     ## Define a variable to hold the courseIndex
        #     courseIndex = None

        #     try: ## Irregular try clause, do not comment out in testing

        #         ## Make a list of the indexes where the long_name column is equal to the outcome (there should only be 1)
        #         courseIndexSearch = allCanvasCoursesDF[allCanvasCoursesDF['long_name'] == outcome].index

        #         ## If the courseIndexs is not empty
        #         if not courseIndexSearch.empty:

        #             ## Get the first index from the courseIndexSearch
        #             courseIndex = courseIndexSearch[0]

            
        #         ## Set the courseIndex to the index of the course with the outcome as the long name
        #         courseIndex = allCanvasCoursesDF[allCanvasCoursesDF['long_name'] == outcome].index[0]
            
        #     ## If no course is found with the outcome as the long name
        #     except: ## Irregular except clause, do not comment out in testing
                
        #         ## Make a list of the indexes where the short_name column is equal to the outcome (there should only be 1)
        #         courseIndexeSearch = allCanvasCoursesDF[allCanvasCoursesDF['short_name'] == outcome].index

        #         ## If the courseIndexs is not empty
        #         if not courseIndexeSearch.empty:

        #             ## Get the first index from the courseIndexSearch
        #             courseIndex = courseIndexeSearch[0]
                
        #     ## Finally
        #     finally:

        #         ## If there is still no course index
        #         if courseIndex is None:

        #             ## Log the fact that the course was not found
        #             localSetup.logger.error(f"\nOutcome not found: {outcome}")
                    
        #             ## Email the fact that the course was not found
        #             errorHandler.sendError (f"External Input Error: {functionName}", f"Outcome course not found: {outcome}")

        #             ## Skip to the next outcome
        #             continue
                
        #     ## Use the course index to get the canvas course id from the course with the outcome as the name
        #     courseCanvasId = allCanvasCoursesDF.loc[courseIndex, 'canvas_course_id']
                
        #     ## Add the course id to the outcomeCanvasDataDict
        #     outcomeCanvasDataDict[outcome] = courseCanvasId

        # ## Return the outcomeCanvasDataDict
        # return uniqueOutcomes, outcomeCanvasDataDict    
    
    except Exception as Error:
        errorHandler.sendError (functionName, Error)

# This function checks whether a term's outcome courses have their associated outcomes and adds them if they don't
def termOutcomeExporter(p1_inputTerm, p1_targetDesignator):
    functionName = "outcome_exporter"

    try:    

        ## Make a list to hold the active add outcome threads
        activeThreads = []

        ## Retrieve the data for determining and sending out relevant communication
        completeActiveCanvasCoursesDF, auxillaryDFDict = (
            retrieveDataForRelevantCommunication(
                p2_inputTerm = p1_inputTerm
                , p3_targetDesignator = p1_targetDesignator
                )
            )

        ## If the complete active canvas courses df is empty
        if completeActiveCanvasCoursesDF.empty:

            ## Log the fact that there are no active courses
            localSetup.logger.info(f"\nNo {p1_targetDesignator} active courses within {p1_inputTerm}")

            ## Return
            return

        ## For each row in the active outcome course df
        for index, row in completeActiveCanvasCoursesDF.iterrows():

            ## If the course is in the auxillaryDFDict active
            
            ## Create an add outcome to course thread
            addOutcomeThread = threading.Thread(target=addOutcomeToCourse
                                                , args=(row
                                                        , auxillaryDFDict
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
     
    except Exception as Error:
        errorHandler.sendError (functionName, Error)

if __name__ == "__main__":

    ## Start and download the Canvas report
    termOutcomeExporter (p1_inputTerm = input("Enter the desired term in four character format (FA20, SU20, SP20): ")
        , p1_targetDesignator = input("Enter the desired target designator (GE, I-EDUC, U-ENGR): ")
        )

    input("Press enter to exit")
