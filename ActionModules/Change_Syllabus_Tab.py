## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

## Import Generic Moduels
from __future__ import print_function
import traceback, os, sys, logging, requests, re, os, os.path, threading, math, json
from datetime import datetime, date, timedelta
from dateutil import parser
import pandas as pd

## Import necessary functions from local modules
## Add Script repository to syspath
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

## New resource modules
try:
    
    from Local_Setup import LocalSetup
    from TLC_Common import makeApiCall, isFileRecent
    from Canvas_Report import CanvasReport
    from Error_Email import errorEmail
except ImportError:
    from ResourceModules.Local_Setup import LocalSetup
    from ResourceModules.TLC_Common import makeApiCall, isFileRecent
    from ResourceModules.Canvas_Report import CanvasReport
    from ResourceModules.Error_Email import errorEmail

## Create the localsetup variable
localSetup = LocalSetup(datetime.now(), __file__)  ## sets cwd, paths, logs, date parts

## Import configs
from Common_Configs import coreCanvasApiUrl, canvasAccessToken, gradTermsWordsToCodesDict

## Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = os.path.basename(__file__).replace(".py", "")

## Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = "Change Syllabus Tab"

scriptPurpose = r"""
Ensure Simple Syllabus is second in course navigation and hide the Canvas Syllabus tab if visible.
"""
externalRequirements = r"""
To function properly this script requires a spreadsheet of the most recent outcomes and the courses they are assigned to.
"""
## Setup the error handler
errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)

## This function retrieves the navigation tabs for a given course sis id
def getNavigationTabs (p1_targetCourseSisId):
    functionName = "Get Navigation Tabs"
    try:
        ## Create the base and specific course API urls
        baseCourseApiUrl = coreCanvasApiUrl + "courses/sis_course_id:" + p1_targetCourseSisId
        navigationApiUrl = baseCourseApiUrl + "/tabs"
        ## Make the API call and save the result as navigationObject
        navigationObject, _ = makeApiCall (localSetup, p1_apiUrl = navigationApiUrl)
        ## If the API status code is anything other than 200 it is an error, so log it and skip
        if (navigationObject.status_code != 200):
            localSetup.logErrorThreadSafe("\nNavigation Error: " + str(navigationObject.status_code))
            localSetup.logErrorThreadSafe(navigationApiUrl)
            localSetup.logErrorThreadSafe(navigationObject.url)
            return None
        ## If the API status code is 200, save the result as navigationTabs
        navigationTabs = navigationObject.json()
        ## Return the navigation tabs
        return navigationTabs

    except Exception as e:
        errorHandler.sendError (functionName, e)
        return None

def updateCourseSyllabusTab (p1_targetCourseSisId):
    functionName = "Update Course Syllabus Tab"
    try:
        ## Get the navigation tabs for the target course sis id
        navigationTabs = getNavigationTabs (p1_targetCourseSisId)

        ## If the navigation tabs is None, return
        if navigationTabs is None:
            return

        ## Define variables to hold the syllabus tab and simple syllabus tab
        syllabusTab = None
        simpleSyllabusTab = None
        
        ## Get the tab with the id of 'syllabus' and the tab with the id of 'external_tool_4856' if it exists
        for tab in navigationTabs:
            ## If the tab is the syllabus tab
            if tab['id'] == 'syllabus':
                syllabusTab = tab
            ## If the tab is the simple syllabus tab
            if tab['id'] == 'context_external_tool_4856':
                simpleSyllabusTab = tab

        ## If there is no syllabus tab, skip this course
        if syllabusTab is None:
            localSetup.logWarningThreadSafe(f"No syllabus tab found for course {p1_targetCourseSisId}")
            return

        ## If the syllabus tab's visibility is public, or if its position is not equal to the length of the navigation tabs, hide the syllabus tab and move it to the end
        if syllabusTab['visibility'] == 'public' or syllabusTab['position'] != (len(navigationTabs) - 2):
            ## Create the base and specific course API urls
            baseCourseApiUrl = coreCanvasApiUrl + "courses/sis_course_id:" + p1_targetCourseSisId
            updateTabApiUrl = baseCourseApiUrl + f"/tabs/{syllabusTab['id']}"
            ## Define the payload to hide the tab
            payload = {"hidden": True, "position": (len(navigationTabs) - 2)}
            ## Make the API call and save the result as updateTabObject
            updateTabObject, _ = makeApiCall (localSetup, p1_apiUrl = updateTabApiUrl, p1_payload = payload, p1_apiCallType = "put")
            ## If the API status code is anything other than 200 it is an error, so log it and skip
            if (updateTabObject.status_code != 200):
                localSetup.logErrorThreadSafe("\nUpdate Tab Error: " + str(updateTabObject.status_code))
                localSetup.logErrorThreadSafe(updateTabApiUrl)
                localSetup.logErrorThreadSafe(updateTabObject.url)
                return
            ## Log the fact that the tab was updated
            localSetup.logInfoThreadSafe(f"\nSyllabus tab hidden for course {p1_targetCourseSisId}")

        ## If there isn't a simple syllabus tab in the list or if it exists but it is not in position 2 tab is the simple syllabus tab
        if not simpleSyllabusTab or (simpleSyllabusTab.get('position') != 2 or simpleSyllabusTab.get('hidden') == True):
            ## Create the base and specific course API urls
            baseCourseApiUrl = coreCanvasApiUrl + "courses/sis_course_id:" + p1_targetCourseSisId
            updateTabApiUrl = baseCourseApiUrl + f"/tabs/{simpleSyllabusTab['id']}"
            ## Define the payload to move the tab to position 2
            payload = {'visibility' : 'public', 'position': 2, 'hidden': False}
            ## Make the API call and save the result as updateTabObject
            updateTabObject, _ = makeApiCall (localSetup, p1_apiUrl = updateTabApiUrl, p1_payload = payload, p1_apiCallType = "put")
            ## If the API status code is anything other than 200 it is an error, so log it and skip
            if (updateTabObject.status_code != 200):
                localSetup.logErrorThreadSafe("\nUpdate Tab Error: " + str(updateTabObject.status_code))
                localSetup.logErrorThreadSafe(updateTabApiUrl)
                localSetup.logErrorThreadSafe(updateTabObject.url)
                return
            ## Log the fact that the tab was updated
            localSetup.logger.info(f"\nSimple Syllabus tab moved to position 2 for course {p1_targetCourseSisId}")


    except Exception as e:
        errorHandler.sendError (functionName, e)
        ##return

if __name__ == "__main__":

    ## Target the course Sandbox_Bryce_Miller for testing
    targetCourseSisId = "GS2026_ACCT6016_1L"
    ## Update the course syllabus tab
    updateCourseSyllabusTab (targetCourseSisId)

    input("Press enter to exit")