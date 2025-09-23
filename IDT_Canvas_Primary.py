# Author: Bryce Miller - brycezmiller@nnu.edu
# Last Updated by: Bryce Miller

## Import Generic Moduels
from datetime import datetime
import traceback, os, sys, logging, threading, time, calendar, json
import pandas as pd

## Set working directory
os.chdir(os.path.dirname(__file__))

## Add Script repository to syspath
sys.path.append(f"{os.getcwd()}\ResourceModules")

## Import local modules
from ResourceModules.Error_Email_API import errorEmailApi
from ResourceModules.Get_Terms import termGetTerms
from ResourceModules.Get_TUG_Students import getUsers
from ResourceModules.Get_Accounts import termGetAccounts
from ResourceModules.Get_Courses import termGetCourses
from ResourceModules.Get_Sections import termGetSections
from ResourceModules.Get_Enrollments import termGetEnrollments
from ResourceModules.Get_Active_Outcome_Courses import termGetActiveOutcomeCourses
from ResourceModules.Get_TUG_Students import termGetTugStudents
# from ResourceModules.Get_Slate_Info import getSlateInfo
# from ResourceModules.Get_Outcomes import createOutcomeCSV
# from ResourceModules.Get_Outcome_Results import termGetOutcomeResults
from ResourceModules.Get_Unpublished_Courses import termGetUnpublishedCourses
from ResourceModules.Get_Canvas_User_Last_Access import termGetCanvasUserLastAccess
from ReportModules.Syllabi_Report import termSyllabiReport
from ReportModules.Syllabi_Report import clearRelaventSyllabiFolders
from ReportModules.Syllabus_Addendum_Report import termAddendumReport
from ReportModules.Incoming_Student_Report import termGetIncomingStudentsInfo
from ReportModules.Outcome_Attachment_Report import termOutcomeAttachmentReport
from ReportModules.Nighthawk_360_Canvas_Report import Nighthawk360CanvasReport
from ReportModules.Outcome_Results_Report import termProcessOutcomeResults
from ActionModules.Enroll_TUG_Students_In_SGA import enrollTugStudentsInSga
from ActionModules.Enroll_GPS_Students_In_Grad_Hub import enrollGPSStudentsInGrad_Hub
from ActionModules.Course_Date_Related_Actions import termDetermineAndPerformRelevantActions
from ActionModules.CX_Data_Sync import importCXData

# Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = "IDT_Canvas_Primary"

scriptPurpose = r"""
Run all IDT Canvas related scripts. Including but not limited to Get_Terms, Get_TUG_Students, Get_Courses, Get_Enrollments, Get_Outcome_Results, Retrieve_University_Syllabi, Check_Syllabi_For_Syllabus_Addendum, List_Gathered_Syllabi, Create_Active_GE_Course_List, Outcome_Exporter, Check_GE_Outcome_Attachment, and GE_Data_SSIS.
"""
externalRequirements = r"""
The full contents of the repository found at https://github.com/NNU-IDT-Scripts/NNU-Canvas-Scripts.
"""

## Date Variables
currentDateTime = datetime.now()
currentHour = currentDateTime.hour
currentDay = currentDateTime.day
currentWeekDay = currentDateTime.weekday()
currentMonth = currentDateTime.month
currentYear = currentDateTime.year
century = str(currentYear)[:2]
decade = str(currentYear)[2:]
lastDayOfCurrentMonth = calendar.monthrange(currentYear, currentMonth)[1]

## Testing variables
# currentDay = 1 ## First week of the month testing value
# currentWeekDay = 0 ## Monday testing value 
# currentWeekDay = 0 ## Monday testing value 
# currentHour = 1 ## First run of the day testing value
# currentHour = 16 ## Last run of the day testing value

## Set working directory
os.chdir(os.path.dirname(__file__))

## Relative Path (this changes depending on the working directory of the main script)
PFRelativePath = r".\\"

## If the Canvas directory is not in the folder the relative path points to
## find the Canvas directory and set the relative path to its parent folder
while "Scripts TLC" not in os.listdir(PFRelativePath):

    PFRelativePath = f"..\\{PFRelativePath}"

## Change the relative path to an absolute path
PFAbsolutePath = f"{os.path.abspath(PFRelativePath)}\\"

## Local Path Variables
baseLogPath = f"{PFAbsolutePath}Logs\\{scriptName}\\"
rawOutputPath = f"{PFAbsolutePath}Canvas Resources\\"
rawOutputPathWithYear = f"{rawOutputPath}{str(currentYear)}-{int(decade)+1}\\"
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
    functionName = "error_handler"

    ## Log the error
    logger.error (f"     \nA script error occured while running {p1_ErrorLocation}. "
                     f"Error: {str(p1_ErrorInfo)}")

    ## If the function with the error has not already been processed send an email alert
    if (p1_ErrorLocation not in setOfFunctionsWithErrors):
        errorEmailApi.sendEmailError(p2_ScriptName = scriptName, p2_ScriptPurpose = scriptPurpose, 
                                     p2_ExternalRequirements = externalRequirements, 
                                     p2_ErrorLocation = p1_ErrorLocation, p2_ErrorInfo = f"{p1_ErrorInfo}: \n\n {traceback.format_exc()}")
        
        ## Add the function name to the set of functions with errors
        setOfFunctionsWithErrors.add(p1_ErrorLocation)
        
        ## Note that an error email was sent%
        logger.error (f"     \nError Email Sent")
    
    ## Otherwise log the fact that an error email as already been sent
    else:
        logger.error (f"     \nError email already sent")
        
## Run Outcome Related Scripts
def outcomeReportsAndActions (p1_relaventTerm):
    
    functionName = "Outcome Reports and Actions"
    
    try:
    
        ## Retrieve the Automated Outcome Tool Variables excel file as a df    
        automatedOutcomeToolVariablesDf = pd.read_excel(f"{baseExternalInputPath}Internal Tool Files\\Automated Outcome Tool Variables.xlsx")
        
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
 
    except Exception as error:
        error_handler (functionName, p1_ErrorInfo = error)
        

## This function creates course and enrollment CSVs for the given term
def createPartialCanvasInputs_Threaded (p3_RelaventTerm):
    
    ## Create a list for ongoing threads
    activeThreads = []
    
    ## If the term is not a grad term
    if p3_RelaventTerm[:2].upper() not in ["GS", "SG", "GF"]:
    
        ## Define the relavent grad term
        relaventGradTerm = p3_RelaventTerm.replace("SP", "GS").replace("FA", "GF").replace("SU", "SG")    
        
        threadGradCreatePartialCanvasInputs_Threaded = threading.Thread(target=createPartialCanvasInputs_Threaded, args=(relaventGradTerm,))
        
        ## Add the thread to the ongoingThreads list
        activeThreads.append(threadGradCreatePartialCanvasInputs_Threaded)

    ## Define the primary threading objects and add them to the ongoingThreads list
    activeThreads.append(threading.Thread(target=termGetCourses, args=(p3_RelaventTerm,)))    
    activeThreads.append(threading.Thread(target=termGetSections, args=(p3_RelaventTerm,)))
    activeThreads.append(threading.Thread(target=termGetCourses, args=(p3_RelaventTerm,)))
    activeThreads.append(threading.Thread(target=termGetEnrollments, args=(p3_RelaventTerm,)))
    activeThreads.append(threading.Thread(target=termGetUnpublishedCourses, args=(p3_RelaventTerm,)))

        
    ## Start threading objects 
    for thread in activeThreads:
        thread.start()
        time.sleep(1)
    


    ## Wait for the threading to complete
    for thread in activeThreads:
        thread.join()    

    ## Retrieve the Automated Outcome Tool Variables excel file as a df    
    automatedOutcomeToolVariablesDf = pd.read_excel(f"{baseExternalInputPath}Internal Tool Files\\Automated Outcome Tool Variables.xlsx")

    ## For each Target Designator in the Automated Outcome Tool Variables
    for targetDesignator in automatedOutcomeToolVariablesDf["Target Designator"]:
        
        ## Retrieve the lists of active outcome courses for the given term
        termGetActiveOutcomeCourses(p3_RelaventTerm, targetDesignator)

    userFileUpdated = False

    ## Make sure to wait until the canvas user's CSV exists
    while not userFileUpdated:

        ## If the user file exists
        if os.path.exists(f"{rawOutputPath}Canvas_Users.csv"):
            
            ## Get its last moddifed timestamp
            userFileTimestamp = os.path.getmtime(f"{rawOutputPath}Canvas_Users.csv")

            ## Convert the timestamp to datetime
            userFileDateTime = datetime.fromtimestamp(userFileTimestamp)

            ## Subtract the file's datetime from the current datetime
            userFileHoursOld = int((currentDateTime - userFileDateTime).total_seconds() // 3600)

            ## If it has been an hour or more since the userfile was updated
            if userFileHoursOld >= 3.5:

                ## Wait 5 seconds
                time.sleep(5)
                logger.info ("The Canvas Users file hasn't been updated yet, wait 5 seconds")

            ## Otherwise
            else:

                ## Update the userFileUpdated value to true
                userFileUpdated = True
    



## This function creates term, user, course, and account CSVs for the given term
## Term and User CSVs are not term specific and so only need to be created once
def createCompleteCanvasInputs_Threaded (p2_RelaventTerm):

    ## Define a list for ongoing threads
    activeThreads = []

    ## Define Canvas instance wholistic threading objects
    activeThreads.append(threading.Thread(target=termGetCourses, args=("All",)))
    activeThreads.append(threading.Thread(target=termGetSections, args=("All",)))
    activeThreads.append(threading.Thread(target=termGetCourses, args=(p2_RelaventTerm,)))
    activeThreads.append(threading.Thread(target=termGetTerms, args=(p2_RelaventTerm,)))
    activeThreads.append(threading.Thread(target=getUsers, args=(p2_RelaventTerm,)))
    activeThreads.append(threading.Thread(target=termGetAccounts, args=(p2_RelaventTerm,)))
    activeThreads.append(threading.Thread(target=termGetCanvasUserLastAccess))
    # activeThreads.append(threading.Thread(target=getSlateInfo, args=(p2_RelaventTerm,)))

    ## Define the term specific threading object
    activeThreads.append(threading.Thread(target=createPartialCanvasInputs_Threaded, args=(p2_RelaventTerm,)))
        
    ## Start threading objects 
    for thread in activeThreads:
        thread.start()
        time.sleep(1)
    
    ## Wait for the threading to complete
    for thread in activeThreads:
        thread.join()

## This function adds the terms from the previous school year
def addPrevYearTerms (p2_targetTermList):
    functionName = "Add Previous School Year Terms"
    
    try:

        lastYearsTerms = []

        ## Determine the last school year's terms by subracting one year from each of the current year's terms
        for term in p2_targetTermList:
            oldTermYear = str(int(term[2:]) - 1)
            oldTerm = f"{term[:2]}{oldTermYear}"
            lastYearsTerms.append (oldTerm)
    
        ## Add the last year's terms to the rel
        p2_targetTermList.extend(lastYearsTerms)

    except Exception as error:
        error_handler (functionName, p1_ErrorInfo = error)

## This function adds the other terms for the current school year as well 
## as the terms for the next semester if they are not part of the current school year
def addAdditionalRelatedTerms (p2_targetTerm, p2_targetTermList):
    functionName = "Add Additional Related Terms"

    try:
        ## If targetting Spring
        if p2_targetTerm == "SP":
            ## Add the previous Fall terms
            p2_targetTermList.append (f"FA{str(currentYear - 1)[2:]}")
            ## Add the comming Summer terms
            p2_targetTermList.append (f"SU{str(currentYear)[2:]}")
            ## Add the comming Fall terms
            p2_targetTermList.append (f"FA{str(currentYear)[2:]}")

        ## If targetting Summer
        elif p2_targetTerm == "SU":
            ## Add the previous Fall terms
            p2_targetTermList.append (f"FA{str(currentYear - 1)[2:]}")
            ## Add the previous Spring terms
            p2_targetTermList.append (f"SP{str(currentYear)[2:]}")
            ## Add the comming Fall terms
            p2_targetTermList.append (f"FA{str(currentYear)[2:]}")
        
        ## Otherwise assume targetting Fall
        else:
            ## Add the previous Summer terms
            p2_targetTermList.append (f"SU{str(currentYear)[2:]}")
            ## Add the comming Spring terms
            p2_targetTermList.append (f"SP{str(currentYear + 1)[2:]}")
            ## Add the comming Summer terms
            p2_targetTermList.append (f"SU{str(currentYear + 1)[2:]}")


    except Exception as error:
        error_handler (functionName, p1_ErrorInfo = error)

## This function adds the other current terms and, depending on the time and date, the additional related terms and previous year's terms
def addRelatedTerms (p1_targetTermList, p1_targetTerm):
    functionName = "Add Related Terms"

    try:
        ## If Spring
        if p1_targetTerm == "SP":

            ## Add the previous and coming terms if it is Monday
            if currentWeekDay == 0:
                addAdditionalRelatedTerms(p2_targetTerm = p1_targetTerm, p2_targetTermList = p1_targetTermList)

                ## If the current day is also the first or third Monday of the month,
                ## add the previous school year's terms as well
                if (currentDay >= 1 and currentDay <= 7) or (currentDay >= lastDayOfCurrentMonth - 6 and currentDay <= lastDayOfCurrentMonth):
                    addPrevYearTerms(p1_targetTermList)

        ## If Summer
        elif p1_targetTerm == "SU":

            ## Add the previous and coming terms if it is Monday
            if currentWeekDay == 0:
                addAdditionalRelatedTerms(p2_targetTerm = p1_targetTerm, p2_targetTermList = p1_targetTermList)

                ## If the current day is also the first or third Monday of the month,
                ## add the previous school year's terms as well
                if (currentDay >= 1 and currentDay <= 7) or (currentDay >= lastDayOfCurrentMonth - 6 and currentDay <= lastDayOfCurrentMonth):
                    addPrevYearTerms(p1_targetTermList)

        ## September through December (the rest of the months) is the Fall Term
        else:

            ## Add the previous and coming terms if it is Monday
            if currentWeekDay == 0:
                addAdditionalRelatedTerms(p2_targetTerm = p1_targetTerm, p2_targetTermList = p1_targetTermList)

                ## If the current day is also the first or third Monday of the month,
                ## add the previous school year's terms as well
                if (currentDay >= 1 and currentDay <= 7) or (currentDay >= 15 and currentDay <= 21):
                    addPrevYearTerms(p1_targetTermList)

        logger.info(f"\n     Relavent Terms set as {p1_targetTermList}.")

    except Exception as error:
        error_handler (scriptName, p1_ErrorInfo = error)

    #return p1_targetTermList

## This function determines the target term/s based on the current date
def determineTargetTermOrTerms (targetMonth = currentMonth):
    functionName = "Determine Related Terms"

    try:

        ## Define the list to hold the relavent terms
        targetTermList = []

        ## Determine the target term based off the target month
        targetTerm = ""

        ## January through May makes the target the target the Spring Term
        if targetMonth >= 1 and targetMonth <= 5:
            targetTerm = "SP"

        ## June through August makes the target the target the Summer Term
        elif targetMonth >= 6 and targetMonth <= 8:
            targetTerm = "SU"

        ## September through December (the rest of the months) makes the target the Fall Term
        else:
            targetTerm = "FA"

        targetTermList.append (f"{targetTerm}{str(currentYear)[2:]}")

        ## If the current hour indicates that it is 3:00 A.M. 
        ## Add the additional related terms as 3:00 is when the full script is run. 
        ## Otherwise it is a partial run that only requires 1 term
        if currentHour < 6:
            addRelatedTerms(p1_targetTermList = targetTermList, p1_targetTerm = targetTerm)

        ## Return the target term list
        return targetTermList

    except Exception as error:
        error_handler (scriptName, p1_ErrorInfo = error)

## This function retrives the data neccessary to run the four times daily processes and runs them
def fourTimesDaily (p1_relaventTerm):
    functionName = "Four Times Daily"

    try:

        ## Run the cx data sync
        CXDataSyncStatus = importCXData()

        ## If the cx data sync was successful
        if CXDataSyncStatus:
            ## Log the successful cx data sync
            logger.info("CX Data Sync Successful")

        # Otherwise
        else:

            ## Log the failed cx data sync
            logger.error("CX Data Sync Failed")

            ## Send an error email
            error_handler (functionName, p1_ErrorInfo = "The CX Data Sync Failed. Please check the messages at https://nnu.instructure.com/accounts/1/sis_import for more information.")
        
        ## Get the primary term data
        createCompleteCanvasInputs_Threaded(p1_relaventTerm)

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
                
                ## Run the partial on the upcoming undg and grad spring terms
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

    except Exception as error:
        error_handler (functionName, p1_ErrorInfo = error)

## This function runs the one time daily processes
def oneTimeDaily (p1_relaventTerms):
    functionName = "One Time Daily"

    try:

        ## Define the neccessary thread lists
        ongoingInputThreads = []
        ongoingOutcomeThreads = []
        ongoingClearSyllabiThreads = []
        ongoingReportThreads = []

        #########   Start retrieving required Canvas data and run the four-times-daily reports  #########
        
        ## Define input threading objects for every term but the first 
        ## (The first term was already run through the process by the four times daily script)
        for term in p1_relaventTerms:

            ## For the first term run the four times daily function as it is run for the first time in the same
            ## time frame as the once daily function
            if term == p1_relaventTerms[0]:
                
                ## Define the term related Canvas input thread to get the complete
                ## Canvas input and run the four times a day script for the first time of the day
                inputThread = threading.Thread(target=fourTimesDaily, args=(term,))

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

        ## Define outcomesTermsList as a list of the relevant term as well as the previous and next terms, starting with the current term
        outcomesTermsList = [p1_relaventTerms[0]]

        ## Otherwise if the first term is a spring term
        if p1_relaventTerms[0][:2].upper() == "SP":
            ## Add the next summer term
            outcomesTermsList.append(p1_relaventTerms[0].replace("SP", "SU"))

        ## If the first term is a summer term
        elif p1_relaventTerms[0][:2].upper() == "SU":
            ## Add the next fall term
            outcomesTermsList.append(p1_relaventTerms[0].replace("SU", "FA"))
            
        ## Otherwise if the first term is a fall term
        elif p1_relaventTerms[0][:2].upper() == "FA":
            ## Add the next spring term, adding 1 to the decade
            outcomesTermsList.append(f"SP{int(p1_relaventTerms[0][2:]) + 1}")

        ## Add the graduate versions of the terms
        for term in outcomesTermsList.copy():
            if term[:2].upper() == "SP":
                outcomesTermsList.append(term.replace("SP", "GS"))
            elif term[:2].upper() == "SU":
                outcomesTermsList.append(term.replace("SU", "SG"))
            elif term[:2].upper() == "FA":
                outcomesTermsList.append(term.replace("FA", "GF"))
            
        ## For each term in the relavent terms
        for term in outcomesTermsList:
            
            ## for each of the first three terms
            # if term == "GF25":

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

        #########   Required Canvas Data Retrieved   #########

        ############################################################################################

        #########   Start report preparation   #########

        ## Clear the current term's syllabi folders to ensure that we only have one syllabi
        ## for each course. Only clear the current terms (the first three terms)
        for term in p1_relaventTerms[:3]:

            ## If the term is the first term
            if term == p1_relaventTerms[0]:

                ## Define a term related termGetTugStudents thread
                termGetTugStudentsThread = threading.Thread(target=termGetTugStudents, args=(term,))

                ## Start the term related termGetTugStudents thread
                termGetTugStudentsThread.start()

                ## Add the term related termGetTugStudents thread to the list of ongoing threads
                ongoingInputThreads.append(termGetTugStudentsThread)
        
            ## Define the term related syallbi report thread
            ClearSyllabiThread = threading.Thread(target=clearRelaventSyllabiFolders, args=(term,))
        
            ## Start the term related syallbi report thread
            ClearSyllabiThread.start()
        
            ## Add the term related syallbi report thread to the list of ongoing threads
            ongoingClearSyllabiThreads.append(ClearSyllabiThread)
        
            ## Wait a second to ensure there is a gap before the next thread
            time.sleep(1)

        ## Check if all ongoing input threads have completed
        for thread in ongoingClearSyllabiThreads:
            thread.join()
            
        #########   Report preparation completed   #########

        ############################################################################################

        #########   Start Reporting   #########

        ## Define a list of the undergrad terms
        undergradTerms = ["FA", "SP", "SU"]
            
        ## Define threading objects
        for term in p1_relaventTerms:

            ## If the term is the first term
            if term == p1_relaventTerms[0]:

                ## Define a term related enrollTugStudentsInSga thread
                ## If the term is the summer term, change it to fall for the purpose of enrolling TUG students in SGA
                enrollTugStudentsInSgaThread = threading.Thread(target=enrollTugStudentsInSga, args=(term.replace("SU","FA"),))

                ## Start the term related enrollTugStudentsInSga thread
                enrollTugStudentsInSgaThread.start()

                ## Add the term related enrollTugStudentsInSga thread to the list of ongoing report threads
                ongoingReportThreads.append(enrollTugStudentsInSgaThread)

                ## Define a term related enrollGPSStudentsInGrad_Hub thread
                ## If the term is the summer term, change it to fall for the purpose of enrolling TUG students in SGA
                enrollGPSStudentsInGrad_HubThread = threading.Thread(target=enrollGPSStudentsInGrad_Hub, args=(term.replace("SG|FA","GF").replace("SP","GS"),))

                ## Start the term related enrollGPSStudentsInGrad_Hub thread
                enrollGPSStudentsInGrad_HubThread.start()

                ## Add the term related enrollGPSStudentsInGrad_Hub thread to the list of ongoing report threads
                ongoingReportThreads.append(enrollGPSStudentsInGrad_HubThread)
        
            # ## Define the term related syallbi report thread
            termSyallabiThread = threading.Thread(target=termSyllabiReport, args=(term,))

            ## Start the term related syallbi report thread
            termSyallabiThread.start()

            ## Add the term related syallbi report thread to the list of ongoing report threads
            ongoingReportThreads.append(termSyallabiThread)

            # Wait three seconds to ensure there is a gap before the next thread
            time.sleep(3)

            ## Define the term related syallbi report thread
            termAddendumThread = threading.Thread(target=termAddendumReport, args=(term,))
                
            ## Start the term related syallbi report thread
            termAddendumThread.start()
                
            ## Add the term related syallbi report thread to the list of ongoing report threads
            ongoingReportThreads.append(termAddendumThread)

            ## Wait three seconds to ensure there is a gap before the next thread
            time.sleep(3)            

        ## Wait until all ongoing threads have completed
        for thread in ongoingReportThreads:
            thread.join()

        #########   Reporting completed   #########

        ############################################################################################



    except Exception as error:
        error_handler (functionName, p1_ErrorInfo = error)

def main ():
    functionName = "Main"

    try:
        
        ## Determine the target terms
        targetTerms = determineTargetTermOrTerms()
        
        ## If it is the first run of the day
        if currentHour < 6:

            ## Run the onetime daily function with the list of target terms
            ## This function includes the four times daily function
            oneTimeDaily(targetTerms)
        
        ## If it is the last run of the day (determined by being after 3:00 pm)
        elif currentHour > 15:

            ## Run the fourTimesDaily script with the 1st (and should be only) target term
            fourTimesDaily (p1_relaventTerm = targetTerms[0])

            ## Fetch the current Nighthawk 360 data
            Nighthawk360CanvasReport()

        ## If it is the second or third run of the day
        else:

            ## Run the fourTimesDaily script with the 1st (and should be only) target term
            fourTimesDaily (p1_relaventTerm = targetTerms[0])

    except Exception as error:
        error_handler (functionName, p1_ErrorInfo = error)

        

if __name__ == "__main__":
    main()
    
    # terms = ["FA", "SP", "SU"]

    # Define the start and end years
    # start_year = 14
    # end_year = 25

    # Initialize an empty list to store the term codes
    # term_codes = []
    
    # targetDesignatorList = ["GE"]

    # Generate the term codes
    # for year in range(start_year, end_year + 1):
    #     for term in terms:
    #         term_codes.append(f"{term}{year:02}")
            
    # ## For each target designator
    # for targetDesignator in targetDesignatorList:
            
    #     threads = []
            
    #     for term in term_codes:
        
    #         ## create partial thread
    #         thread = threading.Thread(target=termGetActiveOutcomeCourses, args=(term,targetDesignator))
        
    #         ## Start the thread
    #         thread.start()
        
    #         ## Add the thread to the threads list
    #         threads.append(thread)
            
    #     ## Wait until all threads have completed
    #     for thread in threads:
    #         thread.join()    
        
    #     threads2 = []
    
    #     for term in term_codes:
    #         ## create partial thread
    #             thread = threading.Thread(target=outcomeReportsAndActions, args=(term, targetDesignator))
            
    #             ## Start the thread
    #             thread.start()       
        
    
    # currentDay = 1 ## First week of the month testing value
    # currentWeekDay = 4 ## Monday testing value 
    # currentHour = 3 ## First run of the day testing value
    
    # main()

    # currentHour = 18 ## Last run of the day testing value
    
    # main()
    
    # subprocess.run(['python', r'C:\NNU Code\Python Scripts\Scripts TLC\IDT_Canvas_Primary.py'])

    ## Pause the terminal before closing
    #input("Press enter to exit")