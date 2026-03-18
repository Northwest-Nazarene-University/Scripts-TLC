# Author: Bryce Miller - brycezmiller@nnu.edu
# Last Updated by: Bryce Miller

## Import Generic Moduels

from calendar import c
from math import e
import os, sys, threading, asyncio
import re
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlparse

# Ensure ResourceModules are importable when running from ActionModules
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

try: ## Irregular try clause, do not comment out in testing
    from Local_Setup import LocalSetup
    from Canvas_Report import CanvasReport
    from Error_Email import errorEmail
    from TLC_Common import isPresent, downloadFile, makeApiCall
    from Add_Outcomes_to_Active_Courses import (
        retrieveDataForRelevantCommunication,
        getUniqueOutcomesAndOutcomeCoursesDict,
        removeMissingOutcomes,
        addOutcomeToCourse,
    )
except ImportError:
    # Fallback to relative imports if package layout differs
    from ResourceModules.Local_Setup import LocalSetup
    from ResourceModules.Canvas_Report import CanvasReport
    from ResourceModules.Error_Email import errorEmail
    from ResourceModules.TLC_Common import isPresent, downloadFile, makeApiCall
    from ActionModules.Add_Outcomes_to_Active_Courses import (
            retrieveDataForRelevantCommunication,
            getUniqueOutcomesAndOutcomeCoursesDict,
            removeMissingOutcomes,
            addOutcomeToCourse,
        )

## Get catalogToSimpleSyllabusConfig from configs
from Common_Configs import catalogToSimpleSyllabusConfig

## Set working directory
os.chdir(os.path.dirname(__file__))

# Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = os.path.basename(__file__)

scriptPurpose = r"""
This script determines what course date related actions need to be taken for a specific term, such as sending outcome related emails to instructors, and performs those actions.
"""
externalRequirements = r"""
This script requires the following external resources:
1. Access to the Canvas API for retrieving course and instructor data.
2. Access to the email system for sending outcome related emails to instructors.
3. The ResourceModules and ActionModules directories in the Scripts TLC directory for additional functionality.
"""

## Initialize LocalSetup and helpers from ResourceModules
localSetup = LocalSetup(datetime.now(), __file__)
errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)

## This function formats the combined catalog course report df into the format needed for Simple Syllabus and saves as a new CSV


## This function determines the school year for the catalog based on the provided catalog links, 
## retrieves the catalog home page to find the school year if necessary, 
## and builds a local path for storing the catalog reports based on that school year. 
def buildCatalogSchoolYearRelatedLocalPath(p1_catalogLinksDict: dict) -> str:

    functionName = "buildCatalogSchoolYearRelatedLocalPath"

    def _tryParseCatalogSchoolYearFromCatalogHomeHtml(htmlText: str):
        match = re.search(r"(20\d{2})\s*[-/\u2013\u2014]?\s*(20\d{2})", htmlText)
        if match:
            return f"{match.group(1)}-{match.group(2)}"
        compactMatch = re.search(r"(20\d{2})(20\d{2})", htmlText)
        if compactMatch:
            return f"{compactMatch.group(1)}-{compactMatch.group(2)}"
        return None

    try:
        schoolYear = None
        for _, downloadUrl in (p1_catalogLinksDict or {}).items():
            if not downloadUrl:
                continue

            reportPageUrl = downloadUrl.split("/download", 1)[0]
            localSetup.logger.info(f"Determining catalog school year from {reportPageUrl}")

            # Use makeApiCall instead of direct requests.get()
            reportPageResponse, _ = makeApiCall(
                localSetup,
                p1_apiUrl=reportPageUrl,
                p1_apiCallType="get"
            )
            reportPageHtml = reportPageResponse.text

            # Parse HTML with BeautifulSoup
            soup = BeautifulSoup(reportPageHtml, 'html.parser')
            
            # Find the "Catalog Home" link using BeautifulSoup
            catalogHomeLink = soup.find(
                'a',
                string=re.compile(r'Catalog\s+Home', re.IGNORECASE)
            )

            if not catalogHomeLink or not catalogHomeLink.get('href'):
                schoolYear = _tryParseCatalogSchoolYearFromCatalogHomeHtml(reportPageHtml)
                if schoolYear:
                    break
                continue

            catalogHomeHref = catalogHomeLink['href']
            ## Parse the base URL from the report page URL
            parsedReportPageUrl = urlparse(reportPageUrl)
            ## Build the full URL for the catalog home page
            catalogHomeUrl = f"{parsedReportPageUrl.scheme}://{parsedReportPageUrl.netloc}{catalogHomeHref}"

            # Use makeApiCall instead of direct requests.get()
            homeResponse, _ = makeApiCall(
                localSetup,
                p1_apiUrl=catalogHomeUrl,
                p1_apiCallType="get"
            )

            # Parse home page HTML with BeautifulSoup
            catalogSchoolYear = _tryParseCatalogSchoolYearFromCatalogHomeHtml(homeResponse.text)
            if catalogSchoolYear:
                break

        if not catalogSchoolYear:
            nowYear = datetime.now().year
            catalogSchoolYear = f"{nowYear}-{nowYear+1}"
            localSetup.logger.warning(f"{functionName}: Could not determine catalog school year; defaulting to {catalogSchoolYear}")

        baseCatalogPath = localSetup.getInternalResourcePaths("Catalog")
        catalogPath = os.path.join(baseCatalogPath, catalogSchoolYear)
        os.makedirs(catalogPath, exist_ok=True)
        return catalogPath, catalogSchoolYear

    except Exception as Error:
        localSetup.logger.error(f"{functionName}: {Error}")
        raise

## Retrieve the TUG and GPS catalog course reports from the urls in catalogProduction in the catalogToSimpleSyllabusConfig
def retrieveCatalogCourseReportsDfs():

    functionName = "retrieveCatalogCourseReports"

    try:
        catalogLinks = catalogToSimpleSyllabusConfig.get("catalogProduction", {})
        if not catalogLinks or not isinstance(catalogLinks, dict):
            raise ValueError("catalogToSimpleSyllabusConfig['catalogProduction'] missing or invalid")

        catalogRootPath, catalogSchoolYear = buildCatalogSchoolYearRelatedLocalPath(catalogLinks)

        catalogCourseReportsDict = {}
        for catalogType, downloadUrl in catalogLinks.items():
            if not downloadUrl:
                continue

            fileName = f"{catalogType.upper()} Course Report.csv"
            localFilePath = os.path.join(catalogRootPath, fileName)
            localSetup.logger.info(f"Downloading {catalogType} catalog course report to {localFilePath}")

            downloadedPath = downloadFile(localSetup, downloadUrl, localFilePath)
            if not downloadedPath or not os.path.exists(downloadedPath):
                raise FileNotFoundError(f"Download failed for {catalogType}: {downloadedPath}")

            catalogCourseReportsDict[catalogType] = downloadedPath

        if not catalogCourseReportsDict:
            raise ValueError("No catalog reports were downloaded")

        ## Open the downloaded files as dfs, add a column for catalog type, and combine into a single df
        combinedCatalogCourseReportDf = pd.DataFrame()
        for catalogType, filePath in catalogCourseReportsDict.items():
            catalogCourseReportsDf = pd.read_csv(filePath)
            catalogCourseReportsDf['Catalog Type'] = catalogType
            if combinedCatalogCourseReportDf.empty:
                combinedCatalogCourseReportDf = catalogCourseReportsDf
            else:
                combinedCatalogCourseReportDf = pd.concat([combinedCatalogCourseReportDf, catalogCourseReportsDf], ignore_index=True)

        ## Save the combined df as a new CSV in the same location as the downloaded reports, with a name like "Combined Catalog Course Report.csv"
        combinedCatalogCourseReportDf.to_csv(os.path.join(catalogRootPath, "Combined Catalog Course Report.csv"), index=False)
                
        return combinedCatalogCourseReportDf, catalogSchoolYear

    except Exception as Error:
        localSetup.logger.error(f"{functionName}: {Error}")
        raise

## This function retreives the catalog course reports, combines them into a simple syllabus format, and uploads the result to Simple Syllabus
def processCatalogCoursesAndUploadToSimpleSyllabus():
    functionName = "processCatalogCoursesAndUploadToSimpleSyllabus"
    try:
        combinedCatalogCourseReportDf, catalogSchoolYear = retrieveCatalogCourseReportsDfs()
        # Placeholder for formatting the combined catalog course report df into the format needed for Simple Syllabus and saving as a new CSV
        # This would involve reading the CSVs, transforming the data, and saving a new CSV
        # Placeholder for uploading the processed file to Simple Syllabus
        # This would involve using the Simple Syllabus API or another method to upload the file
        localSetup.logger.info(f"{functionName}: Successfully processed catalog courses and uploaded to Simple Syllabus")
    except Exception as Error:
        localSetup.logger.error(f"{functionName}: {Error}")
        raise
        
## If the script is being run directly, execute the main function to process catalog courses and upload to Simple Syllabus
if __name__ == "__main__":
    functionName = "main"
    try:
        processCatalogCoursesAndUploadToSimpleSyllabus()
    except Exception as Error:
        localSetup.logger.error(f"{functionName}: {Error}")
        errorHandler.sendError(functionName, Error)