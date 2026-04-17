## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

## Import Generic Moduels
from datetime import datetime
from math import e
import os, sys, threading, time
import pandas as pd

## Set working directory
os.chdir(os.path.dirname(__file__))


## Add the resource modules path
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

## Import local modules
from ResourceModules.Local_Setup import LocalSetup
from ResourceModules.Canvas_Report import CanvasReport
from ResourceModules.Error_Email import errorEmail
from ReportModules.Incoming_Student_Report import termGetIncomingStudentsInfo
from ReportModules.Outcome_Attachment_Report import termOutcomeAttachmentReport
from ReportModules.Nighthawk_360_Canvas_Report import Nighthawk360CanvasReport
from ReportModules.Outcome_Results_Report import termProcessOutcomeResults
from ActionModules.Enroll_TUG_Students_In_SGA import enrollTugStudentsInSga
from ActionModules.Enroll_GPS_Students_In_Grad_Hub import enrollGPSStudentsInGrad_Hub
from ActionModules.Course_Date_Related_Actions import termDetermineAndPerformRelevantActions
from ActionModules.Change_Syllabus_Tab import updateCourseSyllabusTab
from ActionModules.Send_Catalog_To_Simple_Syllabus import processCatalogCoursesAndUploadToSimpleSyllabus
from ActionModules.Send_Course_Editors_To_Simple_Syllabus import processCourseEditorsAndUploadToSimpleSyllabus
from ActionModules.Remove_Orphaned_SIS_Items import removeOrphanedSisItems

## Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = os.path.basename(__file__).replace(".py", "")

scriptPurpose = r"""
Run all IDT Canvas related scripts. Including but not limited to Get_Terms, Get_TUG_Students, Get_Courses, Get_Enrollments, Get_Outcome_Results, Retrieve_University_Syllabi, Check_Syllabi_For_Syllabus_Addendum, List_Gathered_Syllabi, Create_Active_GE_Course_List, Outcome_Exporter, Check_GE_Outcome_Attachment, and GE_Data_SSIS.
"""
externalRequirements = r"""
The full contents of the repository found at https://github.com/NNU-IDT-Scripts/NNU-Canvas-Scripts.
"""

## Initialize the local setup object and error handler
localSetup = LocalSetup(datetime.now(), __file__)
errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)
 
from Common_Configs import undgTermsCodesToWordsDict

## Define time variables
currentHour = localSetup.dateDict["hour"]
currentDay = localSetup.dateDict["day"]
currentWeekDay = localSetup.dateDict["weekDay"]  ## 0=Monday, 4=Friday
currentMonth = localSetup.dateDict["month"]
decade = localSetup.dateDict["decade"]


## Testing variables
## currentDay = 1 ## First week of the month testing value make sure to comment out the target terms variable
## currentWeekDay = 2 ## Day of the week testing value 
## currentHour = 1 ## First run of the day testing value
## currentHour = 16 ## Last run of the day testing value

        
## Run Outcome Related Scripts
def outcomeReportsAndActions (p1_relaventTerm):
    
    functionName = "Outcome Reports and Actions"
    
    try:
    
        ## Retrieve the Automated Outcome Tool Variables excel file as a df    
        automatedOutcomeToolVariablesDf = pd.read_excel(os.path.join(localSetup.getExternalResourcePath("SIS"), "Internal Tool Files\\Automated Outcome Tool Variables.xlsx"))
        
        ## Define the output threading objects
        ongoingOutcomeOutput1Threads = []

        ## Define a variable to track whether the term is a grad term
        gradTerm = False

        ## If the term is a grad term
        if p1_relaventTerm[:2].upper() in ["GS", "SG", "GF"]:
            gradTerm = True
        
        ## For each Target Designator and Course Level in the Automated Outcome Tool Variables
        for targetDesignator, courseLevel in zip(
            automatedOutcomeToolVariablesDf["Target Designator"],
            automatedOutcomeToolVariablesDf["Course Level"]
        ):


            ## If the courseLevel is Undergraduate
            if courseLevel == "Undergraduate":

                ## If the term is a grad term
                if gradTerm:

                    ## Continue
                    continue

            ## Otherwise if the courseLevel is Graduate
            elif courseLevel == "Graduate":

                ## If the term is not a grad term
                if not gradTerm:

                    ## Continue
                    continue

            ## Define the outcome attachment and outcome result report threads
            threadOutcomeAttachment = threading.Thread(target=termOutcomeAttachmentReport, args=(p1_relaventTerm, targetDesignator))
            
            ## Start the threads
            threadOutcomeAttachment.start()
            
            ## Add the threads to the ongoingOutcomeOutputThreads list
            ongoingOutcomeOutput1Threads.append(threadOutcomeAttachment)
    
        ## Wait until all ongoingOutcomeOutputThreads threads have completed
        for thread in ongoingOutcomeOutput1Threads:
            thread.join()

        ## Define the output threading objects
        ongoingOutcomeOutput2Threads = []

        ## For each Target Designator in the Automated Outcome Tool Variables
        for targetDesignator, courseLevel in zip(
            automatedOutcomeToolVariablesDf["Target Designator"],
            automatedOutcomeToolVariablesDf["Course Level"]
        ):

            ## If the courseLevel is Undergraduate
            if courseLevel == "Undergraduate":

                ## If the term is a grad term
                if gradTerm:

                    ## Continue
                    continue

            ## Otherwise if the courseLevel is Graduate
            elif courseLevel == "Graduate":

                ## If the term is not a grad term
                if not gradTerm:

                    ## Continue
                    continue

            ## Define the outcome attachment and outcome result report threads
            threadOutcomeResult = threading.Thread(target=termProcessOutcomeResults, args=(p1_relaventTerm, targetDesignator))
            
            ## Start the threads
            threadOutcomeResult.start()
            
            ## Add the threads to the ongoingOutcomeOutputThreads list
            ongoingOutcomeOutput2Threads.append(threadOutcomeResult)
    
        ## Wait until all ongoingOutcomeOutputThreads threads have completed
        for thread in ongoingOutcomeOutput2Threads:
            thread.join()
            
        ## Define the action threading objects
        ongoingOutcomeActionThreads = []
        
        ## For each Target Designator in the Automated Outcome Tool Variables
        for targetDesignator, courseLevel in zip(
            automatedOutcomeToolVariablesDf["Target Designator"],
            automatedOutcomeToolVariablesDf["Course Level"]
        ):

            ## If the courseLevel is Undergraduate
            if courseLevel == "Undergraduate":

                ## If the term is a grad term
                if gradTerm:

                    ## Continue
                    continue

            ## Otherwise if the courseLevel is Graduate
            elif courseLevel == "Graduate":

                ## If the term is not a grad term
                if not gradTerm:

                    ## Continue
                    continue
            
            ## Define the outcome action threads
            threadOutcomeAction = threading.Thread(target=termDetermineAndPerformRelevantActions, args=(p1_relaventTerm, targetDesignator))
            
            ## Start the threads
            threadOutcomeAction.start()
            
            ## Add the threads to the ongoingOutcomeActionThreads list
            ongoingOutcomeActionThreads.append(threadOutcomeAction)
            
        ## Wait until all ongoingOutcomeActionThreads threads have completed
        for thread in ongoingOutcomeActionThreads:
            thread.join()
 
    except Exception as Error:
        errorHandler.sendError (functionName, Error)
        

## This function creates course and enrollment CSVs for the given term
def createPartialCanvasInputs_Threaded (p3_RelaventTerm):
    functionName = "Create Partial Canvas Inputs"

    try:
    
        ## Create a list for ongoing threads
        activeThreads = []

        ## Define the primary threading objects and add them to the ongoingThreads list
        activeThreads.append(threading.Thread(target=CanvasReport.getCoursesDf, args=(localSetup, p3_RelaventTerm)))
        activeThreads.append(threading.Thread(target=CanvasReport.getSectionsDf, args=(localSetup, p3_RelaventTerm)))
        activeThreads.append(threading.Thread(target=CanvasReport.getEnrollmentsDf, args=(localSetup, p3_RelaventTerm)))
        activeThreads.append(threading.Thread(target=CanvasReport.getUnpublishedCoursesDf, args=(localSetup, p3_RelaventTerm)))

        
        ## Start threading objects 
        for thread in activeThreads:
            thread.start()
            time.sleep(1)

        ## Wait for the threading to complete
        for thread in activeThreads:
            thread.join()

        ## Make sure the users file is recent
        CanvasReport.getUsersDf(localSetup)
    
    except Exception as Error:
        errorHandler.sendError (functionName, Error)



## This function creates term, user, course, and account CSVs for the given term
## Term and User CSVs are not term specific and so only need to be created once
def createCompleteCanvasInputs_Threaded(p2_RelaventTerm):
    ## Define a list for ongoing threads
    activeThreads = []

    ## Define Canvas instance wholistic threading objects
    activeThreads.append(threading.Thread(target=CanvasReport.getCoursesDf, args=(localSetup, "All")))
    activeThreads.append(threading.Thread(target=CanvasReport.getSectionsDf, args=(localSetup, "All")))
    activeThreads.append(threading.Thread(target=CanvasReport.getTermsDf, args=(localSetup,)))
    activeThreads.append(threading.Thread(target=CanvasReport.getUsersDf, args=(localSetup,)))
    activeThreads.append(threading.Thread(target=CanvasReport.getAccountsDf, args=(localSetup,)))
    activeThreads.append(threading.Thread(target=CanvasReport.getCanvasUserLastAccessDf, args=(localSetup,)))

    ## Define the term specific threading object
    activeThreads.append(threading.Thread(target=createPartialCanvasInputs_Threaded, args=(p2_RelaventTerm,)))

    ## Start threading objects 
    for thread in activeThreads:
        thread.start()
        time.sleep(1)

    ## Wait for the threading to complete
    for thread in activeThreads:
        thread.join()

    ## Retrieve the Automated Outcome Tool Variables excel file as a df    
    automatedOutcomeToolVariablesDf = pd.read_excel(os.path.join(localSetup.getExternalResourcePath("SIS"), "Internal Tool Files\\Automated Outcome Tool Variables.xlsx"))
    
    ## For each Target Designator in the Automated Outcome Tool Variables
    for targetDesignator in automatedOutcomeToolVariablesDf["Target Designator"]:
        ## Retrieve the lists of active outcome courses for the given term
        CanvasReport.getActiveOutcomeCoursesDf(localSetup, p2_RelaventTerm, targetDesignator)

## This function determines the target term(s) using TLC_Common helper
def determineTargetTerms():
    functionName = "Determine Target Terms"

    try:

        ## Get the current term codes
        targetTermSet = localSetup.getCurrentTermCodes()
        
        ## If today is Friday
        if currentWeekDay == 4:
            ## Add current school year, most recent term, and next term
            targetTermSet.update(localSetup.getCurrentSchoolYearTermCodes())
            targetTermSet.update(localSetup.getMostRecentCompletedTermCodes())
            targetTermSet.update(localSetup.getNextTermCodes())

            ## If this is the first or third Friday of the month
            if (1 <= currentDay <= 7) or (15 <= currentDay <= 21):
                ## Also add previous school year terms
                targetTermSet.update(localSetup.getPreviousSchoolYearTermCodes())

        ## Convert set to list for return
        targetTermList = list(targetTermSet)

        localSetup.logger.info(f"\nRelevant Terms set as {targetTermList}.")
        return targetTermList

    except Exception as Error:
        errorHandler.sendError(functionName, p1_errorInfo=Error)
        return localSetup.getCurrentTermCodes()

## This function retrives the data neccessary to run the four times daily processes and runs them
def fourTimesDaily (p1_relaventTerm):
    functionName = "Four Times Daily"

    try:

        ## get and open the courses.csv from the external input path
        sisImportFilesList = os.listdir(localSetup.getExternalResourcePath("SIS"))
        if "canvas_course.csv" in sisImportFilesList:
            sisCoursesDf = pd.read_csv(f"{localSetup.getExternalResourcePath('SIS')}canvas_course.csv", dtype=str)
            sisCoursesDf.fillna("", inplace=True)

            ## Filter out any courses that are not in the current or next term
            sisCoursesDf = sisCoursesDf[sisCoursesDf["term_id"].isin(localSetup.getCurrentTermCodes().union(localSetup.getNextTermCodes()))]

            ## Filter out any deleted courses
            sisCoursesDf = sisCoursesDf[sisCoursesDf["status"] != "deleted"]
            
            ## For each row in the sis courses df
            ongoingSyllabusUpdateThreads = []
            for index, row in sisCoursesDf.iterrows():
                ## Get the sis id
                sisCourseId = row["course_id"]

                ## IF the row isn't status active, skip it
                if row["status"] != "active":
                    continue

                ## Define and start thread to update the syllabus tab for the course
                thread = threading.Thread(target=updateCourseSyllabusTab, args=(sisCourseId,))
                thread.start()
                ongoingSyllabusUpdateThreads.append(thread)

                ## Small gap between thread starts to avoid hammering APIs
                time.sleep(1)

            ## Wait until all syllabus update threads have completed
            for thread in ongoingSyllabusUpdateThreads:
                thread.join()
        
        ## Get the primary term data
        createCompleteCanvasInputs_Threaded (p1_relaventTerm)

        ## If relavent term is a summer term
        if p1_relaventTerm[:2].upper() == "SU":

            ## Get the fall future partial canvas inputs
            createPartialCanvasInputs_Threaded(p1_relaventTerm.replace("SU", "FA"))
            
            ## Run the get incoming student info for the future undg fall term
            termGetIncomingStudentsInfo(p1_relaventTerm.replace("SU", "FA"))

            ## If it is july
            if currentMonth == 7:

                ## Run the partial on the upcoming undg and grad school year terms
                createPartialCanvasInputs_Threaded(p1_relaventTerm.replace("SU", "GF"))

                ## Run the incoming student info for the future grad school year
                termGetIncomingStudentsInfo(p1_relaventTerm.replace("SU", "GF"))

            ## Otherwise
            else:

                ## Run get incoming student infor for the current grad school year
                termGetIncomingStudentsInfo(p1_relaventTerm.replace("SU", "SG"))
            
        ## Otherwise
        else:

            ## If it is december
            if currentMonth == 12:
                
                ## Run the partial on the upcoming undg and grad spring tgerms
                createPartialCanvasInputs_Threaded(f"SP{int(decade) + 1}")
                createPartialCanvasInputs_Threaded(f"GS{int(decade) + 1}")

                ## Run the get incoming student info for the future spring terms
                termGetIncomingStudentsInfo(f"SP{int(decade) + 1}")
                termGetIncomingStudentsInfo(f"GS{int(decade) + 1}")
                
            ## Otherwise
            else:    
            
                ## Run the get incoming student info for the current terms
                termGetIncomingStudentsInfo(p1_relaventTerm)
                termGetIncomingStudentsInfo(p1_relaventTerm.replace("SP", "GS").replace("FA", "GF"))

        ## Remove any orphaned SIS-created courses and enrollments
        removeOrphanedSisItems()

    except Exception as Error:
        errorHandler.sendError (functionName, Error)

## This function runs the one time daily processes
def oneTimeDaily (p1_currentTerm, p1_relaventTerms):
    functionName = "One Time Daily"

    try:

        ## Define the neccessary thread lists
        ongoingInputThreads = []
        ongoingOutcomeThreads = []
        ongoingReportThreads = []
        
        ## Define input threading objects for every term but the first 
        ## (The first term was already run through the process by the four times daily script)
        for term in p1_relaventTerms:

            ## For the first term run the four times daily function as it is run for the first time in the same
            ## time frame as the once daily function
            if term == p1_currentTerm:
                
                ## Define the term related Canvas input thread to get the complete
                ## Canvas input and run the four times a day script for the first time of the day
                inputThread = threading.Thread(target=fourTimesDaily, args=(p1_currentTerm,))

            ## Otherwise just get the partial canvas account input for each additional term
            else:

                ## Define the term related Canvas input thread
                inputThread = threading.Thread(target=createPartialCanvasInputs_Threaded, args=(term,))

            ## Start the term related input thread
            inputThread.start()

            ## Add the term related input thread to the list of ongoing threads 
            ongoingInputThreads.append(inputThread)
            time.sleep(1)
            
        ## Check if all ongoing input threads have completed
        for thread in ongoingInputThreads:
            thread.join()

        ## Get the current, most recent completed, and next term codes
        currentTermCodes = localSetup.getCurrentTermCodes()
        mostRecentCompletedTermCodes = localSetup.getMostRecentCompletedTermCodes()
        nextTermCodes = localSetup.getNextTermCodes()

        ## Define a variable to hold the outcome target terms
        outcomeTargetTermCodesList = list()

        ## If it is the first or third Friday of the month, define a list of all the current, most recent completed, and next term codes
        if (1 <= currentDay <= 7) or (15 <= currentDay <= 21):
            outcomeTargetTermCodesList = list(
                currentTermCodes.union(mostRecentCompletedTermCodes).union(nextTermCodes)
                )
        ## Otherwise, define the list as just the current term codes
        else:
            outcomeTargetTermCodesList = list(currentTermCodes)
            
        ## For each term in the relavent terms
        for term in outcomeTargetTermCodesList:

            ## Define a outcome reports and actions thread
            outcomeReportsAndActionsThread = threading.Thread(target=outcomeReportsAndActions, args=(term,))
                
            ## Start the outcome reports and actions thread
            outcomeReportsAndActionsThread.start()
                
            ## Add the outcome reports and actions thread to the list of ongoing threads
            ongoingOutcomeThreads.append(outcomeReportsAndActionsThread)
                
            ## Wait a second to ensure there is a gap before the next thread
            time.sleep(1)
        
        ## Check if all ongoing outcome threads have completed
        for thread in ongoingOutcomeThreads:
            thread.join()

        ########   Required Canvas Data Retrieved   ########

        ## Ensure the current course catalog is processed and uploaded to Simple Syllabus for the current term
        processCatalogCoursesAndUploadToSimpleSyllabus()

        ## Process and upload Course Editor file to Simple Syllabus (depends on Course Extract from catalog above)
        processCourseEditorsAndUploadToSimpleSyllabus()

        ## If the term is the summer term, change it to fall for the purpose of enrolling TUG students in SGA
        enrollTugStudentsInSgaThread = threading.Thread(target=enrollTugStudentsInSga, args=(p1_currentTerm.replace("SU","FA"),))

        ## Start the term related enrollTugStudentsInSga thread
        enrollTugStudentsInSgaThread.start()

        ## Add the term related enrollTugStudentsInSga thread to the list of ongoing report threads
        ongoingReportThreads.append(enrollTugStudentsInSgaThread)

        ## Define a term related enrollGPSStudentsInGrad_Hub thread
        ## If the term is the summer term, change it to fall for the purpose of enrolling TUG students in SGA
        enrollGPSStudentsInGrad_HubThread = threading.Thread(target=enrollGPSStudentsInGrad_Hub, args=(p1_currentTerm.replace("SG", "GF").replace("FA", "GF").replace("SP", "GS"),))

        ## Start the term related enrollGPSStudentsInGrad_Hub thread
        enrollGPSStudentsInGrad_HubThread.start()

        ## Add the term related enrollGPSStudentsInGrad_Hub thread to the list of ongoing report threads
        ongoingReportThreads.append(enrollGPSStudentsInGrad_HubThread)       

        ## Wait until all ongoing threads have completed
        for thread in ongoingReportThreads:
            thread.join()

    except Exception as Error:
        errorHandler.sendError (functionName, Error)

def main ():
    functionName = "Main"

    try:
        
        ## Determine the target terms
        targetTerms = determineTargetTerms()
        ## Find which ever term code is in the undgTermsCodesToWordsDict
        currentTermCodes = localSetup.getCurrentTermCodes()
        currentTerm = None
        for term in currentTermCodes:
            if term[:2].upper() in undgTermsCodesToWordsDict.keys():
                currentTerm = term
                break
        
        ## If it is the first run of the day
        if currentHour < 6:

            ## Run the onetime daily function with the list of target terms
            ## This function includes the four times daily function
            oneTimeDaily(currentTerm, targetTerms)
        
        ## If it is the last run of the day (determined by being after 3:00 pm)
        elif currentHour > 15:

            ## Run the fourTimesDaily script with the 1st (and should be only) target term
            fourTimesDaily (p1_relaventTerm = currentTerm)

            ## Fetch the current Nighthawk 360 data
            Nighthawk360CanvasReport()

        ## If it is the second or third run of the day
        else:

            ## Run the fourTimesDaily script with the 1st (and should be only) target term
            fourTimesDaily (p1_relaventTerm = currentTerm)

    except Exception as Error:
        errorHandler.sendError (functionName, Error)

        

if __name__ == "__main__":
    main()
    #input("Press enter to exit")