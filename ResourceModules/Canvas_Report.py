## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

## Import Generic Modules
import os, time, json, pandas as pd, io, csv, zipfile
from datetime import datetime
from pandas.errors import EmptyDataError

## If the module is run directly 
try: ## Irregular try clause, do not comment out in testing
    ## Import local modules and variables
    from Local_Setup import LocalSetup
    from TLC_Common import (downloadFile, makeApiCall, isFileRecent)
    from Core_Microsoft_Api import downloadSharedMicrosoftFile
except ImportError: ## Otherwise as a relative import if the module is imported
    from .Local_Setup import LocalSetup
    from .TLC_Common import (downloadFile, makeApiCall, isFileRecent)
    from .Core_Microsoft_Api import downloadSharedMicrosoftFile

## Import neccessary config variables
from Common_Configs import coreCanvasApiUrl, canvasAccessToken, undgTermsCodesToWordsDict, gradTermsCodesToWordsDict, gradTermsWordsToCodesDict, serviceEmailAccount

## Define the script name, purpose, and external requirements for logging and error reporting purposes
__scriptName = os.path.basename(__file__).replace(".py", "")
scriptPurpose = r"""
This module (Canvas_Report) provides the CanvasReport class for interacting with the Canvas Reports API.
It allows users to request reports, check their status, download them, and load them into pandas DataFrames.
It requires a valid Canvas API token and access to the specified report endpoints.
    """
externalRequirements = r"""
To function properly this module requires:
- A valid Canvas API token with permissions to access the specified report endpoints.
    """

class CanvasReport:
    def __init__(self, localSetup : LocalSetup, reportType, apiUrl = None, header = None, termCode=None, accountName="NNU",
                 outputRoot=None, includeDeleted=False, filename=None, payload=None, 
                 endpoint="provisioning_csv", accountCanvasID=None):
        ## Receive LocalSetup from caller
        self.localSetup = localSetup
        self.localSetup.logger = self.localSetup.logger  # Use LocalSetup's localSetup.logger
        ## Use LocalSetup's internal resource paths for Canvas unless overridden
        self.outputRoot = outputRoot or self.localSetup.getInternalResourcePaths("Canvas")
        ## Initialize the CanvasReport object with configuration parameters
        self.reportType = reportType
        self.apiUrl = apiUrl or coreCanvasApiUrl
        self.header = header or {'Authorization' : f"Bearer {canvasAccessToken}"}
        self.termCode = termCode
        self.accountName = accountName
        self.includeDeleted = includeDeleted
        self.filename = filename or self._generateFilename()  # Generate filename if not provided
        self.payload = payload or self._buildDefaultPayload()  # Use provided payload or build default
        self.endpoint = endpoint
        self.accountsDf = pd.DataFrame() if self.reportType == "accounts" else self.getAccountsDf(self.localSetup)
        self.accountCanvasID = accountCanvasID if accountCanvasID else self._resolveAccountId()
        self.outputPath = self._buildOutputPath()  # Build directory path for output
        self.filePath = os.path.join(self.outputPath, self.filename)  # Full path to the output file
        self.statusUrl = None
        os.makedirs(self.outputPath, exist_ok=True)  # Ensure output directory exists
        ## Log initialization
        self.localSetup.logger.info(f"Initialized CanvasReport for reportType={self.reportType}, outputPath={self.outputPath}")


    def _buildDefaultPayload(self):
        # Construct default payload for the API call
        payload = {f'parameters[{self.reportType}]': 'true'}
        if self.termCode and self.termCode != "All":
            payload['parameters[enrollment_term_id]'] = f"sis_term_id:{self.termCode}"
        if self.includeDeleted:
            payload['parameters[include_deleted]'] = 'true'
        return payload

    ## Generate a filename for the report
    def _generateFilename(self):
        suffix = self.reportType.replace(" ", "_").capitalize()
        if self.includeDeleted:
            suffix += "_including_deleted"
        return f"{self.termCode or 'All'}_{suffix}.csv"

    ## Build the output directory path based on term code and current year
    def _buildOutputPath(self):
        if self.termCode and self.termCode not in ("All", "Default Term"):
            # Extract year and term prefix
            termYear = int(str(self.localSetup.dateDict['century']) + self.termCode[2:])
            termPrefix = self.termCode[:2]

            # Determine term name (Fall, Spring, Summer)
            termName = undgTermsCodesToWordsDict.get(termPrefix) or gradTermsCodesToWordsDict.get(termPrefix)

            # Determine course level based on prefix
            courseLevel = "Graduate" if termPrefix in gradTermsCodesToWordsDict.keys() else "Undergraduate"

            # Use LocalSetup's public method to get or create the course-level path
            return self.localSetup.getCourseLevelPath(courseLevel, termName, termYear)

        # Default to Canvas root if termCode is "All"
        return self.outputRoot



    ## Resolve the Canvas account ID based on the account name
    def _resolveAccountId(self):
        if self.accountName == "NNU":
            return 1
        if self.accountsDf is not None and self.accountName:
            match = self.accountsDf.loc[self.accountsDf["name"] == self.accountName, "canvas_account_id"]
            if not match.empty:
                return match.values[0]
        return None  # fallback if not resolvable


    ## Check if the existing report file is current based on max age in hours
    def isCurrent(self, maxAgeHours=3.5):
        return isFileRecent(self.localSetup, self.filePath, maxAgeHours)

    ## Request a new report from Canvas and download it
    def getOrCreateReport(self, attempt=0, maxAttempts=3):
        ## Define the API endpoint, adjusting for account if needed
        apiUrl = f"{coreCanvasApiUrl}accounts/{self.accountCanvasID}/reports/{self.endpoint}" if self.accountCanvasID else self.apiUrl

        ## Get the first page of the index of reports to check for an already running report
        # indexResponse, _ = makeApiCall(self.localSetup, p1_header=self.header, p1_apiUrl=apiUrl, p1_apiCallType="get", firstPageOnly=True)
        # indexData = indexResponse.json()
        # activeReport = next((r for r in indexData if r.get('status') in ['running', 'pending']), None)

        ## Define report ID and status URL variables
        # reportID = None
        # self.statusUrl = None

        ## IF there is an active report
        # if activeReport:
        #     ## Set the report ID and status URL
        #     reportID = activeReport['id']
        #     self.statusUrl = f"{apiUrl}/{reportID}"
        #     self.localSetup.logger.info(f"Found active report (ID: {reportID}). Monitoring progress...")
        # else:
            ## No active report, create a new one
        response, _ = makeApiCall(self.localSetup, p1_header=self.header, p1_apiUrl=apiUrl, p1_payload=self.payload, p1_apiCallType="post")
        if response.status_code != 200:
            self.localSetup.logger.error(f"Failed to create new report. Status: {response.status_code}")
            return None
        reportId = json.loads(response.text)["id"]
        self.statusUrl = f"{apiUrl}/{reportId}"

        # Poll the report status until it's ready
        while True:
            statusResponse, _ = makeApiCall(self.localSetup, p1_header=self.header, p1_apiUrl=self.statusUrl, p1_apiCallType="get")
            statusData = json.loads(statusResponse.text)
            #if statusResponse.status_code != 200:
                #raise Exception(f"Failed to get report status. HTTP {statusResponse.status_code}")
            if statusData.get("progress") == 100:
                break
            time.sleep(10)

        # Retry if the report is not properly generated
        if "attachment" not in statusData or "url" not in statusData["attachment"]:
            if attempt < maxAttempts:
                time.sleep(5)
                return self.getOrCreateReport(attempt + 1, maxAttempts)
            #raise Exception("Canvas report failed after multiple attempts.")

        # Download the report file
        downloadUrl = statusData["attachment"]["url"]
        downloadFile(self.localSetup, downloadUrl, self.filePath, "w")
        ## If the filepath doesn't exist or is empty, wait
        if not os.path.exists(self.filePath) or os.path.getsize(self.filePath) == 0:
            if attempt < maxAttempts:
                time.sleep(5)
                return self.getOrCreateReport(attempt + 1, maxAttempts)
            #raise Exception("Downloaded Canvas report file is empty after multiple attempts.")
        return self.filePath
    
    ## Get or create the report, monitoring its status until completion
    # def getOrCreateReport(self, pollInterval=10):
    #     """
    #     Checks if a report of this type is already running or completed.
    #     Monitors it until completion or creates a new one if none exist.
    #     Downloads the file when ready and returns the file path.
    #     """
    #     # Build index URL for this report type
    #     indexUrl = f"{coreCanvasApiUrl}accounts/{self.accountCanvasID}/reports/{self.endpoint}"
    #     self.localSetup.logger.info(f"Checking for existing {self.endpoint} reports...")
    #     response, _ = makeApiCall(self.localSetup, p1_header=self.header, p1_apiUrl=indexUrl, p1_apiCallType="get", firstPageOnly=True)

    #     if response.status_code != 200:
    #         self.localSetup.logger.warning(f"Failed to retrieve report index for {self.endpoint}. Status: {response.status_code}")
    #         return None

    #     reportInstances = response.json()
    #     activeReport = next((r for r in reportInstances if r.get('status') in ['running', 'pending']), None)

    #     # If active report exists, monitor it
    #     if activeReport:
    #         reportId = activeReport['id']
    #         statusUrl = f"{indexUrl}/{reportId}"
    #         self.localSetup.logger.info(f"Found active report (ID: {reportId}). Monitoring progress...")
    #         while True:
    #             statusResponse, _ = makeApiCall(self.localSetup, p1_header=self.header, p1_apiUrl=statusUrl, p1_apiCallType="get")
    #             statusData = statusResponse.json()
    #             progress = statusData.get('progress', 0)
    #             self.localSetup.logger.info(f"Report progress: {progress}%")
    #             if statusData.get('status') == 'complete':
    #                 downloadUrl = statusData.get('file_url') or statusData.get('attachment', {}).get('url')
    #                 if downloadUrl:
    #                     downloadFile(self.localSetup, downloadUrl, self.filePath, "w")
    #                     return self.filePath
    #             time.sleep(pollInterval)

    #     # No active report, create a new one
    #     createUrl = indexUrl
    #     self.localSetup.logger.info(f"No active report found. Creating new {self.endpoint} report...")
    #     createResponse, _ = makeApiCall(self.localSetup, p1_header=self.header, p1_apiUrl=createUrl, p1_payload=self.payload, p1_apiCallType="post")

    #     if createResponse.status_code != 200:
    #         self.localSetup.logger.error(f"Failed to create new report. Status: {createResponse.status_code}")
    #         return None

    #     newReport = createResponse.json()
    #     reportId = newReport['id']
    #     statusUrl = f"{indexUrl}/{reportId}"
    #     self.localSetup.logger.info(f"New report created (ID: {reportId}). Monitoring progress...")

    #     while True:
    #         statusResponse, _ = makeApiCall(self.localSetup, p1_header=self.header, p1_apiUrl=statusUrl, p1_apiCallType="get")
    #         statusData = statusResponse.json()
    #         progress = statusData.get('progress', 0)
    #         self.localSetup.logger.info(f"Report progress: {progress}%")
    #         if statusData.get('status') == 'complete':
    #             downloadUrl = statusData.get('file_url') or statusData.get('attachment', {}).get('url')
    #             if downloadUrl:
    #                 downloadFile(self.localSetup, downloadUrl, self.filePath, "w")
    #                 return self.filePath
    #         time.sleep(pollInterval)

    ## Get the current report if it's fresh, otherwise request a new one
    def getCurrentReport(self, maxAgeHours=3.5):
        # Return the current report if it's fresh, otherwise get a new one
        if self.isCurrent(maxAgeHours):
            return self.filePath
        return self.getOrCreateReport()

    ## Get the current report if it's fresh, otherwise request a new one and load it into a pandas DataFrame
    def getCurrentDataFrame(self, maxAgeHours=3.5):
        self.getCurrentReport(maxAgeHours)
        targetDf = None
        attempt = 0
        while targetDf is None and attempt < 10:
            try: ## Irregular try clause, do not comment out in testing
                targetDf = pd.read_csv(self.filePath)
            except EmptyDataError:
                time.sleep(3)
            attempt += 1
        return targetDf


    
    @classmethod
    def getAccountsDf(cls, localSetup):    
        """
    Retrieve the Canvas Accounts report as a pandas DataFrame.
    
    Args:
        localSetup (LocalSetup): The LocalSetup instance for logging and path management.
    
    Returns:
        pd.DataFrame: DataFrame containing account details from Canvas.
        """
        methodName = "getAccountsDf"
        localSetup.logger.info(f"Starting {methodName}...")
        try:
            report = cls(localSetup=localSetup, reportType="accounts")
            df = report.getCurrentDataFrame()
            localSetup.logger.info(f"Completed {methodName}")
            return df
        except Exception as Error:
            localSetup.logger.error(f"Error in {methodName}: {Error}")
            #raise


    @classmethod
    def getTermsDf(cls, localSetup):    
        """
    Retrieve the Canvas Terms report as a pandas DataFrame.
    
    Args:
        localSetup (LocalSetup): The LocalSetup instance for logging and path management.
    
    Returns:
        pd.DataFrame: DataFrame containing term details from Canvas.
        """
        methodName = "getTermsDf"
        localSetup.logger.info(f"Starting {methodName}...")
        try:
            report = cls(localSetup=localSetup, reportType="terms")
            df = report.getCurrentDataFrame()
            localSetup.logger.info(f"Completed {methodName}")
            return df
        except Exception as Error:
            localSetup.logger.error(f"Error in {methodName}: {Error}")
            #raise


    @classmethod
    def getUsersDf(cls, localSetup):  
        """
    Retrieve the Canvas Users report as a pandas DataFrame.
    Includes deleted users by default.
    
    Args:
        localSetup (LocalSetup): The LocalSetup instance for logging and path management.
    
    Returns:
        pd.DataFrame: DataFrame containing user details from Canvas.
        """
        methodName = "getUsersDf"
        localSetup.logger.info(f"Starting {methodName}...")
        try:
            report = cls(localSetup=localSetup, reportType="users", includeDeleted=True)
            df = report.getCurrentDataFrame()
            localSetup.logger.info(f"Completed {methodName}")
            return df
        except Exception as Error:
            localSetup.logger.error(f"Error in {methodName}: {Error}")
            #raise


    @classmethod
    def getEnrollmentsDf(cls, localSetup, term, includeDeleted=False):
        """
    Retrieve the Canvas Enrollments report for a given term as a pandas DataFrame.
    
    Args:
        localSetup (LocalSetup): The LocalSetup instance for logging and path management.
        term (str): The SIS term code (e.g., "FA25").
        includeDeleted (bool): Whether to include deleted enrollments.
    
    Returns:
        pd.DataFrame: DataFrame containing enrollment details for the specified term.
        """
        methodName = "getEnrollmentsDf"
        localSetup.logger.info(f"Starting {methodName} for term={term}...")
        try:
            report = cls(localSetup=localSetup, reportType="enrollments", termCode=term, includeDeleted=includeDeleted)
            df = report.getCurrentDataFrame()
            localSetup.logger.info(f"Completed {methodName} for term={term}")
            return df
        except Exception as Error:
            localSetup.logger.error(f"Error in {methodName} for term={term}: {Error}")
            #raise


    @classmethod
    def getCoursesDf(cls, localSetup, term):    
        """
    Retrieve the Canvas Courses report for a given term as a pandas DataFrame.
    
    Args:
        localSetup (LocalSetup): The LocalSetup instance for logging and path management.
        term (str): The SIS term code (e.g., "FA25").
    
    Returns:
        pd.DataFrame: DataFrame containing course details for the specified term.
        """
        methodName = "getCoursesDf"
        localSetup.logger.info(f"Starting {methodName} for term={term}...")
        try:
            report = cls(localSetup=localSetup, reportType="courses", termCode=term)
            df = report.getCurrentDataFrame()
            localSetup.logger.info(f"Completed {methodName} for term={term}")
            return df
        except Exception as Error:
            localSetup.logger.error(f"Error in {methodName} for term={term}: {Error}")
            #raise


    @classmethod
    def getSectionsDf(cls, localSetup, term="All"):
        """
    Retrieve the Canvas Sections report for a given term as a pandas DataFrame.
    
    Args:
        localSetup (LocalSetup): The LocalSetup instance for logging and path management.
        term (str): The SIS term code or "All" for all terms.
    
    Returns:
        pd.DataFrame: DataFrame containing section details for the specified term.
        """
        methodName = "getSectionsDf"
        localSetup.logger.info(f"Starting {methodName} for term={term}...")
        try:
            report = cls(localSetup=localSetup, reportType="sections", termCode=term)
            df = report.getCurrentDataFrame()
            localSetup.logger.info(f"Completed {methodName} for term={term}")
            return df
        except Exception as Error:
            localSetup.logger.error(f"Error in {methodName} for term={term}: {Error}")
            #raise

    @classmethod
    def getOutcomesDf(cls, localSetup, term, account, targetDesignator=""):
        """
    Retrieve the Canvas Outcomes report for a given term and account as a pandas DataFrame.
    
    Args:
        localSetup (LocalSetup): The LocalSetup instance for logging and path management.
        term (str): The SIS term code.
        account (str): The Canvas account name.
        targetDesignator (str): Optional designator for filtering outcomes.
    
    Returns:
        pd.DataFrame: DataFrame containing outcome details for the specified term and account.
        """
        methodName = "getOutcomesDf"
        localSetup.logger.info(f"Starting {methodName} for term={term}, account={account}, targetDesignator={targetDesignator}...")
        try:
            filename = (
                f"{term}_{targetDesignator}_Canvas_Outcomes.csv"
                if targetDesignator else f"{term}_{account}_Canvas_Outcomes.csv"
            )
            payload = {"parameters[enrollment_term_id]": f"sis_term_id:{term}"}
            report = cls(
                localSetup=localSetup,
                reportType="outcome_export",
                apiUrl=f"{coreCanvasApiUrl}accounts/{account if account != 'NNU' else 1}/reports/outcome_export_csv",
                termCode=term,
                accountName=account,
                filename=filename,
                payload=payload,
                endpoint="outcome_export_csv"
            )

            ## Download the file and get the path
            targetDestination = report.getCurrentReport()

            # Wait until file has content
            downloadedFileLines = []
            while not downloadedFileLines:
                with open(targetDestination, 'r', encoding='utf-8') as file:
                    downloadedFileLines = file.readlines()
                if not downloadedFileLines:
                    time.sleep(5)

            # Fix header if needed
            downloadedFileFirstLine = downloadedFileLines[0]
            maxCommas = max(line.count(',') for line in downloadedFileLines)
            if maxCommas > 12:
                firstLineParts = downloadedFileFirstLine.strip().split(',')
                firstLineParts[-1] = 'rating 1 points'
                newFirstLine = ','.join(firstLineParts) + ','
                for i in range(1, maxCommas - 11):
                    if i == maxCommas - 11:
                        newFirstLine += f'rating {i} description'
                    else:
                        newFirstLine += f'rating {i} description,rating {i+1} points,'
                downloadedFileLines[0] = newFirstLine + '\n'

            # Join lines and clean unwanted characters
            downloadedFileAsSingleString = ''.join(downloadedFileLines).replace(r'​', '')
            downloadedFileDf = pd.DataFrame()
            try:
                downloadedFileDf = pd.read_csv(io.StringIO(downloadedFileAsSingleString), quoting=csv.QUOTE_MINIMAL, encoding='utf-8')
            except:
                localSetup.logger.warning(f"Initial read_csv failed for {methodName} for term={term}, account={account}. Retrying with 'latin-1' encoding.")

            ## IF the downloadedFileDf isn't empty
            if not downloadedFileDf.empty:
            
                ## Drop empty columns beyond 12th
                downloadedFileDf = downloadedFileDf.dropna(axis=1, how='all')

                ## Clean title column
                if 'title' in downloadedFileDf.columns:
                    if downloadedFileDf['title'].str.contains(r'â€‹').any():
                        downloadedFileDf['title'] = downloadedFileDf['title'].str.replace(r'â€‹', '')
                    if downloadedFileDf['title'].str.contains(r'â€“').any():
                        downloadedFileDf['title'] = downloadedFileDf['title'].str.replace('â€“', '\u2013')

                ## Save cleaned file
                downloadedFileDf.to_csv(targetDestination, index=False, encoding='utf-8')
                localSetup.logger.info(f"Completed {methodName} for term={term}, account={account}")
            else:
                localSetup.logger.warning(f"{methodName} for term={term}, account={account} resulted in an empty DataFrame.")

            return downloadedFileDf

        except Exception as Error:
            localSetup.logger.error(f"Error in {methodName} for term={term}, account={account}: {Error}")
            #raise

    @classmethod
    def getOutcomeResultsDf(cls, localSetup, term, account, targetDesignator=""):
        """
    Retrieve the Canvas Outcomes report for a given term and account as a pandas DataFrame.
    
    Args:
        localSetup (LocalSetup): The LocalSetup instance for logging and path management.
        term (str): The SIS term code.
        account (str): The Canvas account name.
        targetDesignator (str): Optional designator for filtering outcomes.
    
    Returns:
        pd.DataFrame: DataFrame containing outcome details for the specified term and account.
        """
        methodName = "getOutcomeResultsDf"
        localSetup.logger.info(f"Starting {methodName} for term={term}, account={account}, targetDesignator={targetDesignator}...")
        try:
            filename = (
                f"{term}_{targetDesignator}_Canvas_Outcomes_Results.csv"
                if targetDesignator else f"{term}_{account}_Canvas_Outcomes_Results.csv"
            )
            payload = {
                "parameters[enrollment_term_id]": f"sis_term_id:{term}",
                "parameters[order]": "courses"
            }
            report = cls(
                localSetup=localSetup,
                reportType="outcome_results",
                apiUrl=f"{coreCanvasApiUrl}accounts/1/reports/outcome_results_csv",
                termCode=term,
                accountName=account,
                filename=filename,
                payload=payload,
                endpoint="outcome_results_csv"
            )
            df = report.getCurrentDataFrame()
            localSetup.logger.info(f"Completed {methodName} for term={term}, account={account}")
            return df
        except Exception as Error:
            localSetup.logger.error(f"Error in {methodName} for term={term}, account={account}: {Error}")
            #raise
    
    @classmethod
    def getUnpublishedCoursesDf(cls, localSetup, term):

        """
    Retrieve the Canvas Unpublished Courses report for a given term as a pandas DataFrame.
    
    Args:
        localSetup (LocalSetup): The LocalSetup instance for logging and path management.
        term (str): The SIS term code.
    
    Returns:
        pd.DataFrame: DataFrame containing unpublished course details for the specified term.
        """
        methodName = "getUnpublishedCoursesDf"
        localSetup.logger.info(f"Starting {methodName} for term={term}...")
        try:
            filename = f"{term}_Canvas_Unpublished_Courses.csv"
            payload = {"parameters[enrollment_term_id]": f"sis_term_id:{term}"}
            report = cls(
                localSetup=localSetup,
                reportType="unpublished_courses",
                apiUrl=f"{coreCanvasApiUrl}accounts/1/reports/unpublished_courses_csv",
                termCode=term,
                filename=filename,
                payload=payload,
                endpoint="unpublished_courses_csv"
            )
            df = report.getCurrentDataFrame()
            localSetup.logger.info(f"Completed {methodName} for term={term}")
            return df
        except Exception as Error:
            localSetup.logger.error(f"Error in {methodName} for term={term}: {Error}")
            #raise
 
    @classmethod
    def getCanvasUserLastAccessDf(cls, localSetup):  
        """
    Retrieve the Canvas Last User Access report as a pandas DataFrame.
    
    Args:
        localSetup (LocalSetup): The LocalSetup instance for logging and path management.
    
    Returns:
        pd.DataFrame: DataFrame containing last user access details.
        """
        methodName = "getCanvasUserLastAccessDf"
        localSetup.logger.info(f"Starting {methodName}...")
        try:
            report = cls(
                localSetup=localSetup,
                reportType="last_user_access_csv",
                apiUrl=f"{coreCanvasApiUrl}accounts/1/reports/last_user_access_csv",
                endpoint="last_user_access_csv"
            )
            df = report.getCurrentDataFrame()
            localSetup.logger.info(f"Completed {methodName}")
            return df
        except Exception as Error:
            localSetup.logger.error(f"Error in {methodName}: {Error}")
            #raise

    @classmethod
    def getGpsStudentsDf(cls, localSetup, term):    
        """
    Generate a GPS Students DataFrame for a given term.
    Combines courses, enrollments, and users data filtered for GPS accounts.
    
    Args:
        localSetup (LocalSetup): The LocalSetup instance for logging and path management.
        term (str): The SIS term code.
    
    Returns:
        pd.DataFrame: DataFrame containing GPS student details for the specified term.
        """
        methodName = "getGpsStudentsDf"
        localSetup.logger.info(f"Starting {methodName} for term={term}...")
        try:
            # Get courses, enrollments, and users as DataFrames
            coursesDf = cls.getCoursesDf(localSetup, term)
            gpsCourses = coursesDf[
                coursesDf["account_id"].str.contains("G_") &
                (coursesDf["created_by_sis"] == True)
            ]

            enrollmentsDf = cls.getEnrollmentsDf(localSetup, term)
            gpsEnrollments = enrollmentsDf[
                enrollmentsDf["canvas_course_id"].isin(gpsCourses["canvas_course_id"]) &
                (enrollmentsDf["created_by_sis"] == True) &
                (enrollmentsDf["role"] == "student")
            ]

            usersDf = cls.getUsersDf(localSetup)
            gpsUsers = usersDf[
                usersDf["canvas_user_id"].isin(gpsEnrollments["canvas_user_id"].unique())
            ]

            localSetup.logger.info(f"Completed {methodName} for term={term}")
            return gpsUsers
        except Exception as Error:
            localSetup.logger.error(f"Error in {methodName} for term={term}: {Error}")
            #raise


    @classmethod
    def getTugStudentsDf(cls, localSetup, term):
        """
    Generate a TUG Students DataFrame for a given term.
    Combines courses, enrollments, and users data filtered for TUG accounts.
    
    Args:
        localSetup (LocalSetup): The LocalSetup instance for logging and path management.
        term (str): The SIS term code.
    
    Returns:
        pd.DataFrame: DataFrame containing TUG student details for the specified term.
        """
        methodName = "getTugStudentsDf"
        localSetup.logger.info(f"Starting {methodName} for term={term}...")
        try:
            # Get courses, enrollments, and users as DataFrames
            coursesDf = cls.getCoursesDf(localSetup, term)
            coursesDf["account_id"] = coursesDf["account_id"].fillna("")
            tugCourses = coursesDf[
                ~coursesDf["account_id"].str.contains("G_") &
                (coursesDf["created_by_sis"] == True)
            ]

            enrollmentsDf = cls.getEnrollmentsDf(localSetup, term)
            tugEnrollments = enrollmentsDf[
                enrollmentsDf["canvas_course_id"].isin(tugCourses["canvas_course_id"]) &
                (enrollmentsDf["created_by_sis"] == True) &
                (enrollmentsDf["role"] == "student")
            ]

            usersDf = cls.getUsersDf(localSetup)
            tugUsers = usersDf[
                usersDf["canvas_user_id"].isin(tugEnrollments["canvas_user_id"].unique())
            ]

            localSetup.logger.info(f"Completed {methodName} for term={term}")
            return tugUsers
        except Exception as Error:
            localSetup.logger.error(f"Error in {methodName} for term={term}: {Error}")
            #raise

    @classmethod
    def getActiveOutcomeCoursesDf(cls, localSetup, term, targetDesignator):
        """
    Generate an Excel file of active Canvas courses that are published,
    have student enrollments, and are aligned with outcomes for a given term and designator.
    Includes both undergraduate and graduate terms.

    Args:
        localSetup (LocalSetup): The LocalSetup instance for logging and path management.
        term (str): SIS term code (e.g., "FA25").
        targetDesignator (str): Outcome designator (e.g., "GE", "I-EDUC").

    Returns:
        str: Path to the generated Excel file.
        """
        methodName = "getActiveOutcomeCoursesDf"
        localSetup.logger.info(f"Starting {methodName} for term={term}, targetDesignator={targetDesignator}...")

        try:
            # Determine graduate term
            gradTerm = cls.determineGradTerm(term)

            ## Load outcome tool configuration
            sisResourcePath = localSetup.getExternalResourcePath("SIS")
            outcomeToolConfigPath = os.path.join(sisResourcePath, "Internal Tool Files", "Automated Outcome Tool Variables.xlsx")
            outcomeToolConfigDf = pd.read_excel(outcomeToolConfigPath)

            ## Get the designator row to determine course level
            designatorRow = outcomeToolConfigDf[outcomeToolConfigDf["Target Designator"] == targetDesignator]
            courseLevel = designatorRow.iloc[0]["Course Level"]

            ## Initialize the term variables
            targetTerm = gradTerm if courseLevel == "Graduate" else term ## Use grad term for graduate level designators
            termYear = int(str(localSetup.dateDict['century']) + str(targetTerm[2:]))
            termName = localSetup._determineTermName(targetTerm[:2])

            ## Initialize active course dictionary
            outputFilePath = localSetup.getTargetDesignatedOutputPath(termName, termYear, targetDesignator)
            fileName = f"{targetTerm}_{targetDesignator}_Active_Courses.xlsx"
            targetDestination = os.path.join(outputFilePath, fileName)

            # If file is recent, return it
            if isFileRecent(localSetup, targetDestination):
                localSetup.logger.info(f"{targetDestination} is up to date.")
                return pd.read_excel(targetDestination)

            # Retrieve Canvas data for both terms
            coursesDf = pd.concat([
                cls.getCoursesDf(localSetup, term), 
                cls.getCoursesDf(localSetup, gradTerm)
                ], ignore_index=True)
            sectionsDf = pd.concat([
                cls.getSectionsDf(localSetup, term), 
                cls.getSectionsDf(localSetup, gradTerm)
                ], ignore_index=True)
            enrollmentsDf = pd.concat([
                cls.getEnrollmentsDf(localSetup, term, includeDeleted=True),
                cls.getEnrollmentsDf(localSetup, gradTerm, includeDeleted=True)
                ], ignore_index=True)
            usersDf = cls.getUsersDf(localSetup)

            ## Define the output path for outcome course data
            outputPath = localSetup.getTargetDesignatedOutputPath(termName, termYear, targetDesignator)

            # Build active course dictionary
            activeCourseDict = {
                "Term": [], "Outcome Area": [], "Course_sis_id": [], "Parent_Course_sis_id": [],
                "Section_id": [], "Course_name": [], "Canvas_Account_id": [], "Number_of_students": [],
                "Instructor_#1_ID": [], "Instructor_#1_name": [], "Instructor_#1_email": []
            }

            # Call the internal method to get outcome course DataFrame
            outcomeCourseDf = cls._getOutcomeAssociatedCourseCodesDf(
                localSetup=localSetup,
                outputPath=outputPath,
                inputTerm=term,
                targetDesignator=targetDesignator,
                p1_outcomeToolConfigDf=outcomeToolConfigDf,
            )
            
            if outcomeCourseDf.empty:
                pd.DataFrame().to_excel(outputFilePath, index=False)
                return pd.read_excel(targetDestination)

            ## Identify outcome columns
            outcomeColumns = [col for col in outcomeCourseDf.columns if "Outcome" in col and "Area" not in col]

            ## Initialize active course dictionary with outcome columns
            for outcomeColumn in outcomeColumns:
                activeCourseDict[outcomeColumn] = []

            # Filter and populate courses
            for _, course in coursesDf.iterrows():
                ## The course doesn't have a sis id, wasn't created by sis, doesn't have an underscore in sis id, or is already added
                if (not course.get("course_id") 
                    or course.get("created_by_sis") != True 
                    or "_" not in course["course_id"]
                    or course["course_id"] in activeCourseDict["Course_sis_id"]
                    ):
                    continue ## Skip this course

                ## Make a list to hold crosslisted course sis ids and section ids
                crosslistedCanvasCourseIdList = []  
                crosslistedCanvasSectionIdsList = []

                ## If the course code is in the outcomeCourseDf
                if course["course_id"].split('_')[1] in outcomeCourseDf["Course Code"].values:

                    ## For each row that it appears in in the courseOutcomeAssociationsDf
                    #for index in courseOutcomeAssociationsDf[courseOutcomeAssociationsDf["Course Code"] == row["course_id"].split('_')[1]].index:
                    for index in outcomeCourseDf[outcomeCourseDf["Course Code"] == course["course_id"].split('_')[1]].index:

                        ## Define a primarySectionIndex
                        primarySectionIndex = None
                        ## ## try to get the index where the name column of the sectionsDf contains the course_id
                        try: ## Irregular try clause, do not comment out in testing
                            primarySectionIndex = sectionsDf[
                                sectionsDf["name"].fillna("").str.contains(
                                    course["course_id"]
                                    )
                                ].index[0]
                        except: ## Irregular except clause, do not comment out in testing     
                            ## Grab the section id from the course name in case it is different by splitting the course name by " " and getting the last element
                            try: ## Irregular try clause, do not comment out in testing
                                primarySectionIndex = sectionsDf[
                                    sectionsDf["name"].fillna("").str.contains(
                                        course["long_name"].split(" ")[-1]
                                        )
                                    ].index[0]
                            except: ## Irregular except clause, do not comment out in testing      
                                ## Otherwise log a warning that no section was found
                                localSetup.logger.warning (f"     \nCould not find a section that matched the course sis id or course name for {course['course_id']}.")
                                ## Skip to the next course
                                continue
                            ## If the primarySectionIndex is not None
                            if primarySectionIndex is not None:
                                ## Log a warning that no section was found that matched the course sis id but one was found that matched the course name
                                localSetup.logger.warning (f"     \nFound a section that matched the course name but not the course sis id for {course['course_id']}.")

                        ## Add the course to the active Outcome Courses Dict
                        activeCourseDict["Term"].append(term)
                        activeCourseDict["Outcome Area"].append(targetDesignator)
                        activeCourseDict["Course_sis_id"].append(course["course_id"])
                        activeCourseDict["Course_name"].append(course["long_name"])
                        activeCourseDict["Canvas_Account_id"].append(course["canvas_account_id"])
                        activeCourseDict["Parent_Course_sis_id"].append("")
                        activeCourseDict["Number_of_students"].append(0)
                        activeCourseDict["Instructor_#1_ID"].append("")
                        activeCourseDict["Instructor_#1_name"].append("")
                        activeCourseDict["Instructor_#1_email"].append("")
                   
                        ## Find the sections for this course
                        courseSectionsDf = sectionsDf[sectionsDf["canvas_course_id"] == course["canvas_course_id"]]

                        ## Add the section id that matches the course name to the active Outcome Courses Dict
                        activeCourseDict["Section_id"].append(sectionsDf.loc[primarySectionIndex, "canvas_section_id"])

                        targetCourseSectionsDf = sectionsDf[sectionsDf["canvas_course_id"] == course["canvas_course_id"]]

                        ## If the targetCourseSectionsDf has more than one section
                        if len(targetCourseSectionsDf) > 1:

                            ## For each additional section in the targetCourseSectionsDf
                            for sectionIndex in targetCourseSectionsDf.index:

                                ## If the section name is not the same as the course name
                                if targetCourseSectionsDf.loc[sectionIndex, "name"] != course["long_name"]:

                                    ## Add the crosslisted section and canvas_course id to the crosslistedCanvasSectionIdsList and crosslistedCanvasCourseIdList
                                    crosslistedCanvasSectionIdsList.append(targetCourseSectionsDf.loc[sectionIndex, "canvas_section_id"])
                                    crosslistedCanvasCourseIdList.append(targetCourseSectionsDf.loc[sectionIndex, "canvas_course_id"])
                                
                        ## For each outcome column in the outcomeCourseDf
                        for outcomeColumn in outcomeColumns:
                                    
                            ## Add the outcome to the active Outcome Courses Dict
                            activeCourseDict[outcomeColumn].append(outcomeCourseDf.loc[index, outcomeColumn])                                

                ## If there are crosslisted courses
                if crosslistedCanvasSectionIdsList:

                    ## For each crosslisted course sis id and crosslisted course section id
                    for crosslistedCanvasSectionId, crosslistedCanvasCourseId in zip(crosslistedCanvasSectionIdsList, crosslistedCanvasCourseIdList):

                        ## Get the index of the crosslistedCanvasCourseId in from the canvasAllCoursesDf
                        crosslistedSectionIndex = sectionsDf[sectionsDf["canvas_section_id"] == crosslistedCanvasSectionId].index[0]

                        ## Get the long name of the crosslistedCanvasCourseId
                        crosslistedCourseName = sectionsDf.loc[crosslistedSectionIndex, "name"]

                        ## Define variables to hold the course code and sis id
                        crosslistedCourseCode = None
                        crosslistedCourseSisId = None

                        ## Attempt to isolate the crosslisted course Code as it would show up in the outcomeCourseDf
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

                        ## If the crosslisted course sis id appears in the outcomeCourseDf
                        if crosslistedCourseCode in outcomeCourseDf["Course Code"].values:

                            ## For each course that appears in in the outcomeCourseDf
                            for crosslistedIndex in outcomeCourseDf[outcomeCourseDf["Course Code"] == crosslistedCourseCode].index:

                                ## Add the course to the active Outcome Courses Dict
                                activeCourseDict["Term"].append(term)
                                activeCourseDict["Outcome Area"].append(outcomeCourseDf.loc[crosslistedIndex, "Outcome Area"])
                                activeCourseDict["Course_sis_id"].append(crosslistedCourseSisId)
                                activeCourseDict["Course_name"].append(crosslistedCourseName)
                                activeCourseDict["Canvas_Account_id"].append(course["canvas_account_id"])
                                activeCourseDict["Number_of_students"].append(0)
                                activeCourseDict["Instructor_#1_ID"].append("")
                                activeCourseDict["Instructor_#1_name"].append("")
                                activeCourseDict["Instructor_#1_email"].append("")
                                                
                                ## For each outcome column in the outcomeCourseDf
                                for outcomeColumn in outcomeColumns:
                                                    
                                    ## Add the outcome to the active Outcome Courses Dict
                                    activeCourseDict[outcomeColumn].append(outcomeCourseDf.loc[crosslistedIndex, outcomeColumn])

                                ## Add the parent course sis id to the active Outcome Courses Dict
                                activeCourseDict["Parent_Course_sis_id"].append(course["course_id"])
                                                
                                ## Get the index of the crosslisted course id in the crosslistedCourseNamesList
                                crosslistedCanvasCourseIdIndex = crosslistedCanvasCourseIdList.index(crosslistedCanvasCourseId)

                                ## Add the section id that matches the crosslisted course name + 1 to the active Outcome Courses Dict
                                activeCourseDict["Section_id"].append(crosslistedCanvasSectionIdsList[crosslistedCanvasCourseIdIndex])

            # Enrich with enrollments
            for _, row in enrollmentsDf.iterrows():
                if row["status"] not in ["active", "concluded"]:
                    continue
                if row["canvas_course_id"] not in coursesDf["canvas_course_id"].values:
                    continue
                targetIndex = activeCourseDict["Course_sis_id"].index(row["course_id"]) if row["course_id"] in activeCourseDict["Course_sis_id"] else None
                if targetIndex is None:
                    continue
                if row["base_role_type"] == "StudentEnrollment":
                    activeCourseDict["Number_of_students"][targetIndex] += 1
                elif (row["base_role_type"] == "TeacherEnrollment" 
                      and row["user_id"] not in ["63232.0", "63232"]
                      ):
                    if activeCourseDict["Instructor_#1_ID"][targetIndex] == "":
                        activeCourseDict["Instructor_#1_ID"][targetIndex] = row["user_id"]
                    else:
                        ## Make a variable to hold the key name that the instructor's id will be added to
                        targetInstructorIDKey = None
                                    
                        ## Make a list of the keys that have instructor and id in them
                        instructorIDKeys = [key for key in activeCourseDict.keys() if "Instructor" in key and "ID" in key]
                                    
                        ## Make a list of the processed instructor ids using the instructorIDKeys
                        processedInstructorIds = [activeCourseDict[key][targetIndex] for key in instructorIDKeys if activeCourseDict[key][targetIndex]]

                        ## If the user id is already in the processed instructor ids
                        if row["user_id"] in processedInstructorIds:
                                        
                            ## Skip it
                            continue
                                    
                        ## For each key in the instructorIDKeys
                        for key in instructorIDKeys:
                                        
                            ## If the key is not the first instructor id key
                            if key != "Instructor_#1_ID":
                                            
                                ## If the key's value at the index is empty
                                if activeCourseDict[key][targetIndex] == "":
                                                
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
                                        
                            ## Add the new keys to the activeCourseDict
                            activeCourseDict[newInstructorIDKey] = []
                            activeCourseDict[newInstructorNameKey] = []
                            activeCourseDict[newInstructorEmailKey] = []
                                        
                            ## For for the length of the Instructor_#1_ID list
                            for i in range(len(activeCourseDict["Instructor_#1_ID"])):
                                                
                                ## Add a blank value to the new lists so they are the same length as the Instructor_#1_ID list
                                activeCourseDict[newInstructorIDKey].append("")
                                activeCourseDict[newInstructorNameKey].append("")
                                activeCourseDict[newInstructorEmailKey].append("")
                                        
                            ## Set the targetInstructorIDColumn to the newInstructorIDColumn
                            targetInstructorIDColumn = newInstructorIDKey

                        ## Set the instructor's id to the targetInstructorIDColumn at the courseIndex
                        activeCourseDict[targetInstructorIDColumn][targetIndex] = row["user_id"]

            ## Keep only active courses with students
            indicesToRemove = [i for i, count in enumerate(activeCourseDict["Number_of_students"]) if count == 0]
            for key in activeCourseDict:
                activeCourseDict[key] = [v for i, v in enumerate(activeCourseDict[key]) if i not in indicesToRemove]

            ## Create a blank userMap dict
            userMap = {}

            # Enrich instructor details
            # Create a user map preferring rows where created_by_sis == True, then rows with an email, then fallback.
            if not usersDf.empty:
                tempUsersDf = usersDf.copy()
                # Build a numeric priority: created_by_sis (2) + has_email (1)
                tempUsersDf["_priority"] = tempUsersDf["created_by_sis"].fillna(False).astype(bool).astype(int) * 2 + tempUsersDf["email"].notna().astype(int)

                # Ensure deterministic ordering: higher priority first, then keep first occurrence per user_id
                tempUsersDf = tempUsersDf.sort_values(by=["user_id", "_priority"], ascending=[True, False])
                unduplicatedTempUsersDf = tempUsersDf.drop_duplicates(subset="user_id", keep="first")
                userMap = unduplicatedTempUsersDf.set_index("user_id")[["full_name", "email"]].to_dict("index")
            else:
                userMap = {}

            ## Make a list of the instructor id keys
            instructorIDKeys = [key for key in activeCourseDict.keys() if "Instructor" in key and "ID" in key]

            ## For each key in the instructorIDKeys
            for key in instructorIDKeys:
                
                ## For each user value in the key
                for user_id in activeCourseDict[key]:
                    ## Convert the user id to str
                    user_id = str(user_id)
                    
                    ## If the user id is not empty
                    if user_id:
                    
                        ## Make a list of all of the indexes where the user id appears in the key
                        userIndexes = [i for i, x in enumerate(activeCourseDict[key]) if x == user_id]

                        ## For each index in the userIndexes list
                        for userIndex in userIndexes:
                    
                            ## Replace the empty name and email values with the user's name and email
                            activeCourseDict[key.replace("ID", "name")][userIndex] = userMap[user_id]["full_name"]
                            activeCourseDict[key.replace("ID", "email")][userIndex] = userMap[user_id]["email"]

            
            # ## For each outcome column, append the value from outcomeCourseDf
            # for outcomeColumn in outcomeColumns:
            #     ## Ensure the course exists in outcomeCourseDf before accessing
            #     if courseCode in outcomeCourseDf["Course Code"].values:
            #         rowIndex = outcomeCourseDf[outcomeCourseDf["Course Code"] == courseCode].index[0]
            #         activeCourseDict[outcomeColumn].append(outcomeCourseDf.loc[rowIndex, outcomeColumn])
            #     else:
            #         activeCourseDict[outcomeColumn].append("")  # Blank if not found


            # Save to Excel, replacing all instances of \u200b with ""
            activeCourseDf = pd.DataFrame(activeCourseDict)
            activeCourseDf.replace("\u200b", "", regex=True, inplace=True)
            activeCourseDf.to_excel(targetDestination, index=False)
            localSetup.logger.info(f"Successfully created {fileName}")
            return activeCourseDf

        except Exception as Error:
            localSetup.logger.error(f"Error in {methodName}: {Error}")
            #raise

    @classmethod
    def determineDepartmentSavePath(cls, localSetup, courseAccountId):
        """
    Determine the save path for a given Canvas account ID by traversing the account hierarchy.

    Args:
        localSetup (LocalSetup): The LocalSetup instance for logging and path management.
        courseAccountId (int): The Canvas account ID.

    Returns:
        str: The constructed department save path.
        """
        methodName = "determineDepartmentSavePath"

        try:
            # If root account
            if courseAccountId == 1:
                return "NNU\\"

            # Read the accounts CSV into a DataFrame
            departmentSavePathsDF = cls.getAccountsDf(localSetup)

            # Determine which column to use for matching
            targetRow = "account_id"
            if courseAccountId not in departmentSavePathsDF[targetRow].tolist():
                targetRow = "canvas_account_id"

            # Locate the row for the given account ID
            accountRow = departmentSavePathsDF.index.get_loc(
                departmentSavePathsDF[departmentSavePathsDF[targetRow] == courseAccountId].index[0]
            )

            # Start building the path with the account name
            accountName = departmentSavePathsDF["name"][accountRow]
            accountSavePath = f"{accountName}\\"

            # Traverse parent accounts until root
            targetAccountParentID = departmentSavePathsDF["canvas_parent_id"][accountRow]
            while pd.notna(targetAccountParentID) and targetAccountParentID != 1:
                parentAccountRow = departmentSavePathsDF.index.get_loc(
                    departmentSavePathsDF[departmentSavePathsDF["canvas_account_id"] == targetAccountParentID].index[0]
                )
                targetAccountName = departmentSavePathsDF["name"][parentAccountRow]
                accountSavePath = f"{targetAccountName}\\{accountSavePath}"
                targetAccountParentID = departmentSavePathsDF["canvas_parent_id"][parentAccountRow]

            # Clean up path formatting for department/college names
            if len(accountSavePath.rsplit("\\")) > 3:
                departmentName = accountSavePath.rsplit("\\")[1]
                accountSavePath = accountSavePath.replace(f" {departmentName}", "").replace(
                    "College of Arts &\\", "College of Arts & Humanities\\"
                )
            else:
                collegeName = accountSavePath.rsplit("\\")[0].replace("College of ", "")
                accountSavePath = accountSavePath.replace(f"Undergraduate {collegeName}", "Undergraduate").replace(
                    f"Graduate {collegeName}", "Graduate"
                )

            # Handle underscores in names
            if "_" in accountSavePath and "Undergraduate_" not in accountSavePath and "Graduate_" not in accountSavePath:
                if "Graduate " in accountSavePath:
                    stringWithUnderscore = accountSavePath.split("Graduate ")[1].split("_")[0]
                else:
                    stringWithUnderscore = accountSavePath.split("Undergraduate ")[1].split("_")[0]

                accountSavePath = (
                    accountSavePath.replace(f"Undergraduate {stringWithUnderscore}_", "Undergraduate_")
                    .replace(f"Graduate {stringWithUnderscore}_", "Graduate_")
                    .replace(f"{stringWithUnderscore}_", "")
                )

            return accountSavePath

        except Exception as Error:
            localSetup.logger.error(f"Error in {methodName}: {Error}")
            #raise



    @staticmethod
    def determineGradTerm(term):
        """
        Determine the graduate term equivalent for a given undergraduate term.
        Example: FA25 -> GF25, SP25 -> GP25, SU25 -> GU25
        """
        # Extract the prefix (first two characters)
        prefix = term[:2].upper()
        termName = undgTermsCodesToWordsDict.get(prefix, "")
        gradPrefix = gradTermsWordsToCodesDict[termName] if termName in gradTermsWordsToCodesDict else ""
        return term.replace(prefix, gradPrefix) if gradPrefix else term

    ## Retrieve the outcome course code list from SharePoint and return as a DataFrame.
    @staticmethod
    def _getOutcomeAssociatedCourseCodesDf(localSetup, outputPath, inputTerm, targetDesignator, p1_outcomeToolConfigDf):
        """
    Retrieve the outcome course code list from SharePoint and return as a DataFrame.
    Saves a cleaned Excel file locally for reuse.

    Args:
        outputPath (str): Path for term and target-specific output.
        inputTerm (str): SIS term code (e.g., "FA25").
        targetDesignator (str): Outcome designator (e.g., "GE", "I-EDUC").
        p1_outcomeToolConfigDf (pd.DataFrame): DataFrame containing outcome tool configuration.
        localSetup.logger (Logger): Logger instance for logging.

    Returns:
        pd.DataFrame: DataFrame containing outcome course associations.
        """
        try:
            # Define expected filenames and paths
            outcomeFileName = f"{inputTerm}_{targetDesignator}_Active_Course_Outcome_Associations.xlsx"
            rawOutputFileName = f"{inputTerm}_Raw_{targetDesignator}_Active_Course_Outcome_Associations.xlsx"
            outputFilePath = os.path.join(outputPath, outcomeFileName)
            rawOutputFilePath = os.path.join(outputPath, rawOutputFileName)

            # If file exists and is recent, return it
            if isFileRecent(localSetup, filePath=outputFilePath):
                    localSetup.logger.info(f"Outcome file {outcomeFileName} is up to date.")
                    return pd.read_excel(outputFilePath)

            # Retrieve SharePoint URL and sheet name for target designator
            shareUrl = p1_outcomeToolConfigDf.loc[p1_outcomeToolConfigDf["Target Designator"] == targetDesignator,
                                               "Outcome Course Association List URL"].values[0]
            sheetName = p1_outcomeToolConfigDf.loc[p1_outcomeToolConfigDf["Target Designator"] == targetDesignator,
                                                "Outcome Course Association Target Sheet Name"].values[0]

            # Download file from SharePoint
            downloadedFilePath = downloadSharedMicrosoftFile(
                p1_microsoftUserName=serviceEmailAccount,
                p1_microsoftShareUrl=shareUrl,
                p1_downloadSavePath=outputPath,
                p1_fileName=rawOutputFileName
            )

            if not rawOutputFilePath or not os.path.exists(rawOutputFilePath):
                localSetup.logger.error(f"Outcome file not found: {rawOutputFilePath}")
                return None

            # Choose engine based on extension
            fileExt = os.path.splitext(rawOutputFilePath)[1].lower()
            pdEngine = "xlrd" if fileExt == ".xls" else "openpyxl"

            # Load Excel file
            if not zipfile.is_zipfile(rawOutputFilePath):
                localSetup.logger.warning(f"Downloaded file is not a valid Excel file. Attempting repair...")
                # Try reading as CSV and resave as proper Excel
                try:
                    fileDataframe = pd.read_csv(rawOutputFilePath)
                    with pd.ExcelWriter(rawOutputFilePath, engine="openpyxl") as writer:
                        fileDataframe.to_excel(writer, index=False)
                    localSetup.logger.info(f"File repaired")
                except Exception as e:
                    localSetup.logger.error(f"Repair failed: {e}")
                    raise
            excelFile = pd.ExcelFile(rawOutputFilePath, engine=pdEngine)
            rawoutcomeCourseDf = None
            if sheetName and sheetName in excelFile.sheet_names:
                rawoutcomeCourseDf = pd.read_excel(excelFile, sheet_name=sheetName, engine=pdEngine)
            elif "By Course" in excelFile.sheet_names:
                rawoutcomeCourseDf = pd.read_excel(excelFile, sheet_name="By Course", engine=pdEngine)
            else:
                rawoutcomeCourseDf = pd.read_excel(excelFile, engine=pdEngine)

            # Normalize column names
            outcomeCourseDf = rawoutcomeCourseDf.copy()
            outcomeCourseDf.columns = [str(col).strip().lower() for col in outcomeCourseDf.columns]
            if "course number" in outcomeCourseDf.columns:
                outcomeCourseDf.rename(columns={"course number": "number"}, inplace=True)
            outcomeCourseDf.columns = [col.title() for col in outcomeCourseDf.columns]

            # Fix header if needed
            if outcomeCourseDf.columns[0].lower() != "prefix":
                outcomeCourseDf.columns = outcomeCourseDf.iloc[0].astype(str).str.strip().tolist()
                outcomeCourseDf = outcomeCourseDf[1:].reset_index(drop=True)

            # Add outcome area and clean course codes
            outcomeCourseDf["Outcome Area"] = targetDesignator
            if targetDesignator != "GE" and outcomeCourseDf["Prefix"].str.contains(targetDesignator).any():
                outcomeCourseDf["Prefix"] = outcomeCourseDf["Prefix"].str[2:]

            # Clean numeric columns
            for column in outcomeCourseDf.columns:
                if any(keyword in column.lower() for keyword in ["number", "course code", "course number"]):
                    outcomeCourseDf[column] = outcomeCourseDf[column].astype(str).str.replace(r'\.(0\d)$', '', regex=True)

            # Drop rows missing required values
            outcomeColumns = [col for col in outcomeCourseDf.columns if "Outcome" in col and "Area" not in col]
            outcomeCourseDf.dropna(subset=["Prefix", "Number", outcomeColumns[0]], inplace=True)

            # Add Course Code column
            outcomeCourseDf.insert(1, "Course Code", outcomeCourseDf["Prefix"] + outcomeCourseDf["Number"].astype(str))

            ## Drop Prefix and Number columns
            outcomeCourseDf.drop(columns=["Prefix", "Number"], inplace=True)

            ## Clean any .0 from Course Code
            outcomeCourseDf["Course Code"] = outcomeCourseDf["Course Code"].str.replace(r'\.0$', '', regex=True)

            # Save cleaned file
            outcomeCourseDf.to_excel(outputFilePath, index=False)
            return outcomeCourseDf

        except Exception as Error:
            localSetup.logger.error(f"Error in _getOutcomeAssociatedCourseCodesDf: {Error}")
            return pd.dataframe()


if __name__ == "__main__":


    # Initialize LocalSetup
    localSetup = LocalSetup(datetime.now(), __file__)

    while True:
        print("\nCanvas Report Menu")
        print("1. Accounts")
        print("2. Terms")
        print("3. Users")
        print("4. Last User Access")
        print("5. Enrollments")
        print("6. Courses")
        print("7. Sections")
        print("8. Outcomes")
        print("9. Outcome Results")
        print("10. Unpublished Courses")
        print("11. GPS Students")
        print("12. TUG Students")
        print("13. Active Outcome Courses")
        print("14. Run All Reports")
        print("0. Exit")

        choice = input("\nEnter the number of the report to run (or '0' to exit): ").strip()

        if choice == '0':
            print("Exiting Canvas Report Menu.")
            break

        if choice not in [str(i) for i in range(1, 15)]:
            print("Invalid selection. Please enter a number between 1 and 14, or 0 to exit.")
            continue

        # Prompt for term if needed
        term = None
        if choice in ['5', '6', '7', '8', '9', '10', '11', '12', '13', '14']:
            term_input = input("Enter the term code (e.g., 'FA25', 'GF25') or 'All': ").strip()
            term = term_input

        # Prompt for account if needed
        account = None
        if choice in ['8', '9']:
            account = input("Enter the account name (e.g., 'College of Arts'): ").strip()

        # Prompt for target designator if Active Outcome Courses
        targetDesignator = None
        if choice == '13':
            targetDesignator = input("Enter the target designator (e.g., 'GE', 'I-EDUC'): ").strip()

        try:
            if choice == '1':
                print(CanvasReport.getAccountsDf(localSetup))
            elif choice == '2':
                print(CanvasReport.getTermsDf(localSetup))
            elif choice == '3':
                print(CanvasReport.getUsersDf(localSetup))
            elif choice == '4':
                print(CanvasReport.getCanvasUserLastAccessDf(localSetup))
            elif choice == '5':
                print(CanvasReport.getEnrollmentsDf(localSetup, term))
            elif choice == '6':
                print(CanvasReport.getCoursesDf(localSetup, term))
            elif choice == '7':
                print(CanvasReport.getSectionsDf(localSetup, term))
            elif choice == '8':
                print(CanvasReport.getOutcomesDf(localSetup, term, account))
            elif choice == '9':
                print(CanvasReport.getOutcomeResultsDf(localSetup, term, account))
            elif choice == '10':
                print(CanvasReport.getUnpublishedCoursesDf(localSetup, term))
            elif choice == '11':
                print(CanvasReport.getGpsStudentsDf(localSetup, term))
            elif choice == '12':
                print(CanvasReport.getTugStudentsDf(localSetup, term))
            elif choice == '13':
                print(CanvasReport.getActiveOutcomeCoursesDf(localSetup, term, targetDesignator))
            elif choice == '14':
                # Run standard reports
                print(CanvasReport.getAccountsDf(localSetup))
                print(CanvasReport.getTermsDf(localSetup))
                print(CanvasReport.getUsersDf(localSetup))
                print(CanvasReport.getCanvasUserLastAccessDf(localSetup))
                print(CanvasReport.getEnrollmentsDf(localSetup, term))
                print(CanvasReport.getCoursesDf(localSetup, term))
                print(CanvasReport.getSectionsDf(localSetup, term))
                print(CanvasReport.getUnpublishedCoursesDf(localSetup, term))
                print(CanvasReport.getGpsStudentsDf(localSetup, term))
                print(CanvasReport.getTugStudentsDf(localSetup, term))

                # Retrieve outcomeToolConfigDf to get all target designators
                sisResourcePath = localSetup.getExternalResourcePath("SIS")
                outcomeToolConfigPath = os.path.join(sisResourcePath, "Internal Tool Files", "Automated Outcome Tool Variables.xlsx")
                outcomeToolConfigDf = pd.read_excel(outcomeToolConfigPath)

                # Get all target designators
                targetDesignators = outcomeToolConfigDf["Target Designator"].dropna().unique()

                # Run reports for each target designator
                for designator in targetDesignators:
                    print(f"\nRunning reports for Target Designator: {designator}")
                    print(CanvasReport.getOutcomesDf(localSetup, term, designator))
                    print(CanvasReport.getOutcomeResultsDf(localSetup, term, designator))
                    print(CanvasReport.getActiveOutcomeCoursesDf(localSetup, term, designator))

        except Exception as Error:
            print(f"An error occurred: {Error}")