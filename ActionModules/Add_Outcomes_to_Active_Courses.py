# Author: Bryce Miller - brycezmiller@nnu.edu
# Last Updated by: Bryce Miller

## Import Generic Moduels
import os, sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
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
    from Error_Email import errorEmail
    from TLC_Action import (
        retrieveDataForRelevantCommunication,
        addOutcomeToCourse,
    )
    from TLC_Common import isMissing

except ImportError:
    from ResourceModules.Local_Setup import LocalSetup
    from ResourceModules.Error_Email import errorEmail
    from ResourceModules.TLC_Action import (
        retrieveDataForRelevantCommunication,
        addOutcomeToCourse,
    )
    from ResourceModules.TLC_Common import isMissing

# Create LocalSetup and localSetup.logger
localSetup = LocalSetup(datetime.now(), __file__)

## Setup error handler
errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)

# This function checks whether a term's outcome courses have their associated outcomes and adds them if they don't
def termOutcomeExporter(p1_inputTerm, p1_targetDesignator):
    functionName = "outcome_exporter"

    try:    

        ## Retrieve the data for determining and sending out relevant communication
        completeActiveCanvasCoursesDF, auxillaryDFDict = (
            retrieveDataForRelevantCommunication(
                p1_localSetup = localSetup
                , p1_errorHandler = errorHandler
                , p2_inputTerm = p1_inputTerm
                , p3_targetDesignator = p1_targetDesignator
                )
            )

        ## If the complete active canvas courses df is empty
        if isMissing(completeActiveCanvasCoursesDF):

            ## Log the fact that there are no active courses
            localSetup.logger.info(f"\nNo {p1_targetDesignator} active courses within {p1_inputTerm}")

            ## Return
            return

        ## Process each course in a thread pool
        MAX_WORKERS = 25
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for index, row in completeActiveCanvasCoursesDF.iterrows():

                ## Submit the add outcome task to the thread pool
                executor.submit(addOutcomeToCourse, localSetup, errorHandler, row, auxillaryDFDict)

    except Exception as Error:
        errorHandler.sendError (functionName, Error)

if __name__ == "__main__":

    ## Start and download the Canvas report
    termOutcomeExporter (p1_inputTerm = input("Enter the desired term in four character format (FA20, SU20, SP20): ")
        , p1_targetDesignator = input("Enter the desired target designator (GE, I-EDUC, U-ENGR): ")
        )

    input("Press enter to exit")
