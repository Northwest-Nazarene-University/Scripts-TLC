# Author: Bryce Miller - brycezmiller@nnu.edu
# Last Updated by: Bryce Miller

## Import Generic Moduels
import traceback, os, sys, logging, threading, csv, requests, json, pdfkit, re, os, os.path
from datetime import date
import pandas as pd

## Set working directory
os.chdir(os.path.dirname(__file__))

## Add Script repository to syspath
sys.path.append(f"{os.getcwd()}\ResourceModules")

## Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = "Non Official Instructors Report"

## Script file identifier
scriptRequirementMissingFolderIdentifier = "Courses_Without_Required_Outcome_Attached"

scriptPurpose = r"""
This script takes in the current term's enrollments and the SIS enrollment feed to create an excel file of the non instructors of record for each course.
"""
externalRequirements = r"""
To function properly, this script requires access to the ..\Canvas Resources folder and the shared drive Instructional Design and Technology\Canvas_Load_Files folder in the 
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

## Define the internal paths
rawInternalInputPath = f"{PFAbsolutePath}Canvas Resources\\"
internalOutputPath = f"{PFAbsolutePath}Canvas Resources\\"

## Add Input Modules to the sys path
sys.path.append(f"{PFAbsolutePath}Scripts TLC\\ResourceModules")

## Import local modules
from Error_Email_API import errorEmailApi
from Download_File import downloadFile

from Create_Sub_Account_Save_Path import determineDepartmentSavePath
from Download_File import downloadFile

## Local Path Variables
baseLogPath = f"{PFAbsolutePath}Logs\\{scriptName}\\"
configPath = f"{PFAbsolutePath}\\Configs TLC\\"
baseLocalInputPath = f"{PFAbsolutePath}Canvas Resources\\"  ## This is only the base path as the real path requires the requested term
baseLocalOutputPath = f"{PFAbsolutePath}Canvas Resources\\" ## This is only the base path as the real path requires the requested term

## External Path Variables

## Define a variable to hold the base external input path which is where the sis input files are stored
baseExternalOutputPath = None ## Where the syllabus repository will be created and relavent reports stored
## Open Base_External_Paths.json from the config path and get the baseExternalInputPath value
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

## This function takes in a term and returns the instructor information for non-official instructors 
def termNonOfficialInstructorsReport(p1_inputTerm = ""):
    functionName = "termNonOfficialInstructorsReport"

    try:

        # Determine and save the term's school year
        schoolYear = None
        if re.search("AF|FA|GF", p1_inputTerm):
            ## Fall terms are the first terms of a new school year so FA20 is part of the 2020-21 school year.
            schoolYear = (century + p1_inputTerm[2:] + "-" + str(int(p1_inputTerm[2:]) + 1))
        elif re.search("SP|GS|AS|SG|SA|SU", p1_inputTerm):
            ## Spring and Summer terms belong in the same school year as the fall terms before them, so SP21 is part of the same 2020-21 school year as FA20.
            schoolYear = (century + str(int(p1_inputTerm[2:]) - 1) + "-" + p1_inputTerm[2:])

        ## Determine the relevant grad term
        gradTerm = None
        if "FA" in p1_inputTerm:
            gradTerm = f"GF{p1_inputTerm[2:]}"

        elif "SP" in p1_inputTerm:
            gradTerm = f"GS{p1_inputTerm[2:]}"

        elif "SU" in p1_inputTerm:
            gradTerm = f"SG{p1_inputTerm[2:]}"
        
        ## Create the school year relavent input and output paths
        schoolYearInputPath = f"{rawInternalInputPath}\\{schoolYear}\\"
        schoolYearOutputPath = f"{internalOutputPath}\\{schoolYear}\\"
        
        ## Define the undergrad term specific input path
        undgTermInputPath = f"{schoolYearInputPath}{p1_inputTerm}\\"

        ## Define the grad term specific input path
        gradTermInputPath = f"{schoolYearInputPath}{gradTerm}\\"
    
        ## Retrieve the Undergraduate enrollments file as a df, filter it to only contain the rows that have "Instructor" in the role value
        rawUndgTermInstructorEnrollmentsDF = pd.read_csv(f"{undgTermInputPath}{p1_inputTerm}_Complete_Canvas_Enrollments.csv")

        ## Filter the Undg Enrollments to only have rows that have "Instructor" in the role value
        firstFilteredUndgTermInstructorEnrollmentsDF = rawUndgTermInstructorEnrollmentsDF[rawUndgTermInstructorEnrollmentsDF['role'] == 'teacher']

        ## Filter the Undg Enrollments to only have rows that have "active" in the status value
        secondFilteredUndgTermInstructorEnrollmentDF = firstFilteredUndgTermInstructorEnrollmentsDF[firstFilteredUndgTermInstructorEnrollmentsDF['status'] == 'active']
        
        ## Retrieve the Graduate enrollments file as a df, filter it to only contain the rows that have "Instructor" in the role value and active in the status value
        rawGradTermInstructorEnrollmentsDF = pd.read_csv(f"{gradTermInputPath}{gradTerm}_Complete_Canvas_Enrollments.csv")

        ## Filter the Grad Enrollments to only have rows that have "Instructor" in the role value
        firstFilteredGradTermInstructorEnrollmentsDF = rawGradTermInstructorEnrollmentsDF[rawGradTermInstructorEnrollmentsDF['role'] == 'teacher']

        ## Filter the Grad Enrollments to only have rows that have "active" in the status value
        secondFilteredGradTermInstructorEnrollmentsDF = firstFilteredGradTermInstructorEnrollmentsDF[firstFilteredGradTermInstructorEnrollmentsDF['status'] == 'active']

        ## Retrieve the SIS feed enrollments file as a df, and filter it to only contain the rows that have "Instructor" in the role value
        rawSisEnrollmentsDF = pd.read_csv(f"{baseExternalInputPath}canvas_enroll.csv")
        
        ## Filter the SIS Enrollments to only have rows that have "Instructor" in the role value
        firstFilteredSisEnrollmentsDF = rawSisEnrollmentsDF[rawSisEnrollmentsDF['role'] == 'teacher']

        ## Filter the SIS Enrollments to only have rows that have "active" in the status value
        secondFilteredSisEnrollmentsDF = firstFilteredSisEnrollmentsDF[firstFilteredSisEnrollmentsDF['status'] == 'active']

        ## Add a column that combines the course_id and user_id to create a unique identifier for each row
        secondFilteredSisEnrollmentsDF['course_plus_user_id'] = secondFilteredSisEnrollmentsDF['course_id'] + secondFilteredSisEnrollmentsDF['user_id'].astype(str)
        
        ## Create a non official instructors df by concatenating the undergrad and grad dfs
        rawCombinedTermInstructorEnrollmentDF = pd.concat([secondFilteredUndgTermInstructorEnrollmentDF, secondFilteredGradTermInstructorEnrollmentsDF])

        ## Retrieve the grad and undergrad sections dfs
        rawGradSectionsDF = pd.read_csv(f"{gradTermInputPath}{gradTerm}_Canvas_Sections.csv")
        rawUndgSectionsDF = pd.read_csv(f"{undgTermInputPath}{p1_inputTerm}_Canvas_Sections.csv")

        ## Combine the grad and undergrad sections dfs
        rawCombinedSectionsDF = pd.concat([rawGradSectionsDF, rawUndgSectionsDF])

        ## Remove any duplicate rows from the combined sections df
        rawCombinedSectionsDF.drop_duplicates(inplace = True)

        # Create a dictionary from rawCombinedSectionsDF for quick lookup
        rawCombinedSectionsDF['name'] = rawCombinedSectionsDF['name'].astype(str)
        canvasSectionIdNameDict = rawCombinedSectionsDF.set_index('canvas_section_id')['name'].apply(lambda x: x.split(' ')[-1]).to_dict()

        # Map the canvas_section_id to the corresponding name in the enrollment file
        rawCombinedTermInstructorEnrollmentDF['section_id'] = rawCombinedTermInstructorEnrollmentDF['canvas_section_id'].map(canvasSectionIdNameDict).fillna('Unknown')
        
        ## Add a column that combines the course_id and user_id to create a unique identifier for each row
        rawCombinedTermInstructorEnrollmentDF['course_plus_user_id'] = rawCombinedTermInstructorEnrollmentDF['section_id'] + rawCombinedTermInstructorEnrollmentDF['user_id'].astype(str)

        ## Filter out any rows that are in the SIS feed
        filteredCombinedTermInstructorEnrollmentDF = rawCombinedTermInstructorEnrollmentDF[~rawCombinedTermInstructorEnrollmentDF['course_plus_user_id'].isin(secondFilteredSisEnrollmentsDF['course_plus_user_id'])]

        ## Delete the course_plus_user_id column
        del filteredCombinedTermInstructorEnrollmentDF['course_plus_user_id']

        ## Set the type of the course_id column to int
        filteredCombinedTermInstructorEnrollmentDF['user_id'] = filteredCombinedTermInstructorEnrollmentDF['user_id'].fillna(0).astype(int)

        ## Get the user file from the raw input path
        userFileDf = pd.read_csv(f"{rawInternalInputPath}Canvas_Users.csv")

        ## Create a Instructor Name column by matching the user_id in the user file to the user_id in the filtered combined term instructor enrollment df
        filteredCombinedTermInstructorEnrollmentDF['full_name'] = filteredCombinedTermInstructorEnrollmentDF['canvas_user_id'].apply(
            lambda x: userFileDf.loc[userFileDf['canvas_user_id'] == x, 'full_name'].values[0] if len(userFileDf.loc[userFileDf['canvas_user_id'] == x, 'full_name'].values) > 0 else 'Unknown'
        )
        
        ## Create a non official instructors df by taking the course_id, course_name, user_id, and full_name columns
        rawNonOfficialInstructorDf = filteredCombinedTermInstructorEnrollmentDF[['course_id', 'user_id', 'full_name', "created_by_sis"]]

        ## Drop any rows where the course_id is null or ""
        rawNonOfficialInstructorDf = rawNonOfficialInstructorDf[~rawNonOfficialInstructorDf['course_id'].isnull()]

        ## Change the user_id column to instructor_id and the full name column to instructor_name
        rawNonOfficialInstructorDf.rename(columns = {'user_id' : 'instructor_id', 'full_name' : 'instructor_name'}, inplace = True)
        
        ## Filter out any rows with 63232 as the user_id
        filteredNonOfficialInstructorDf = rawNonOfficialInstructorDf[
            (rawNonOfficialInstructorDf['instructor_id'] != 63232) & 
            (rawNonOfficialInstructorDf['created_by_sis'] == False)
        ]

        ## Drop the created_by_sis column
        filteredNonOfficialInstructorDf.drop(columns = ['created_by_sis'], inplace = True)

        ## Save the raw and filtered non official instructors dfs to the output path as excel files
        rawNonOfficialInstructorDf.to_excel(f"{schoolYearOutputPath}Raw_Non_Official_Instructors.xlsx", index = False)
        filteredNonOfficialInstructorDf.to_excel(f"{schoolYearOutputPath}Non_Official_Instructors.xlsx", index = False)
        
        
    
    except Exception as error:
        error_handler (functionName, error)


## This function takes in a input term or creates one to run the term Non Official Instructors Report
def createNonOfficialInstructorsReport (inputTerm = ""):
    functionName = "Run OutcomeAttachment Report"
    
    try:

        currentTerm = ""

        ## If a term is not given, determine it off of the current year
        if not inputTerm:
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

            ## Set the input term as current term
            inputTerm = currentTerm

        termNonOfficialInstructorsReport (p1_inputTerm = inputTerm)
     
    except Exception as error:
        error_handler (functionName, error)

if __name__ == "__main__":

    ## Define the API Call header using the retreived Canvas Token
    ##header = {'Authorization' : f"Bearer {canvasAccessToken}"}

    ## Start and download the Canvas report
    createNonOfficialInstructorsReport (inputTerm = input("Enter the desired term in \
four character format (FA20, SU20, SP20): "))

    input("Press enter to exit")