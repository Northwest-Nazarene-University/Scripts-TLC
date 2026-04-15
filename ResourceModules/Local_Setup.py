## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

## Import Generic Modules
import os, json, sys, logging, calendar, re, requests
from datetime import datetime
from requests.adapters import HTTPAdapter
## Add the config path
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "Configs"))

## Import local config variables
from Common_Configs import (
    scriptLibrary, 
    externalResourcePathsDict, 
    undgTermsWordsToCodesDict, 
    undgTermsCodesToWordsDict,
    gradTermsWordsToCodesDict,
    gradTermsCodesToWordsDict,
    termMonthRanges,
    termSchoolYearLogic
)

## Define the script name, purpose, and external requirements for logging and error reporting purposes
__scriptName = os.path.basename(__file__).replace(".py", "")

scriptPurpose = r"""
This module (TLC_Common) provides the LocalSetup class for setting up common paths,
logging, and resource management for scripts within the TLC script library.
It standardizes the environment for scripts by establishing directory structures,
logging mechanisms, and resource access methods.
"""

externalRequirements = r"""
To function properly this module requires:
- Access to the filesystem to create and manage directories for logs and resources.
- Permissions to write log files in the designated log directory.
"""

class LocalSetup:
    def __init__(self, dateTime: datetime, __scriptPath: str, scriptLibrary: str = scriptLibrary):

        ## Private Variables ##

        ## Local Script Variables
        self.__scriptPath = __scriptPath
        self.__scriptName = os.path.basename(__scriptPath).replace(".py", "")
        self.__scriptLibraryName = scriptLibrary
        ## Term Dictionaries
        self.__undgTermsDict = undgTermsWordsToCodesDict
        self.__gradTermsWordsToCodesDict = gradTermsWordsToCodesDict

        ## Public Variables ##

        ## Date and Time Variables
        self.initialDateTime = dateTime
        self.dateDict = {
            "hour" : dateTime.hour,
            "day" : dateTime.day,
            "weekDay" : dateTime.weekday(),
            "month" : dateTime.month,
            "year" : dateTime.year,
            "century" : dateTime.year // 100, ## integer division to get century ## e.g. 2024 // 100 = 20
            "decade" : dateTime.year % 100, ## modulus to get decade ## e.g. 2024 % 100 = 24
            "lastDayOfCurrentMonth" : calendar.monthrange(dateTime.year, dateTime.month)[1]
        }
        ## Setup paths and logging
        self.absolutePath, self.baseLogPath, self.configPath = self._setupCommonPaths()
        self.logger = self._setupLogger()
        ## Path Storage
        self.internalResourcePathDict = {}
        self.externalResourcePaths = externalResourcePathsDict
        self.termPaths = {}
        self.internalDepartmentOutputPaths = {}

        ## Canvas API Session (persistent connection pool for all makeApiCall usage)
        self.canvasSession = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=10,   ## Match _canvasMaxConcurrentRequests
            pool_maxsize=10,       ## Match _canvasMaxConcurrentRequests
            max_retries=0,         ## We handle retries ourselves in Api_Caller
        )
        self.canvasSession.mount("https://", adapter)
        self.canvasSession.mount("http://", adapter)

    ## Internal Methods

    ## Create common paths set working directory, and add module paths to sys.path
    def _setupCommonPaths(self): 
        fileDir = os.path.dirname(self.__scriptPath)
        os.chdir(fileDir)

        ## Traverse up until "Scripts_TLC" is found
        pfRelativePath = fileDir
        while True:
            if "Scripts_TLC" in os.listdir(pfRelativePath):
                break
            parent = os.path.abspath(os.path.join(pfRelativePath, ".."))
            if parent == pfRelativePath:
                raise FileNotFoundError("Scripts_TLC directory not found in parent hierarchy.")
            pfRelativePath = parent

        absolutePath = os.path.abspath(pfRelativePath)
        ## Add resource module paths to sys.path
        sys.path.append(os.path.join(absolutePath, f"Scripts_{self.__scriptLibraryName}", "ResourceModules"))
        sys.path.append(os.path.join(absolutePath, f"Scripts_{self.__scriptLibraryName}", "ReportModules"))
        sys.path.append(os.path.join(absolutePath, f"Scripts_{self.__scriptLibraryName}", "ActionModules"))
        sys.path.append(os.path.join(absolutePath, f"Scripts_{self.__scriptLibraryName}", "Configs"))
        ## Setup log and config paths using absolute path
        configPath = os.path.join(absolutePath, f"Scripts_{self.__scriptLibraryName}", "Configs")
        baseLogPath = os.path.join(absolutePath, "Logs", self.__scriptName)
        os.makedirs(baseLogPath, mode=0o777, exist_ok=True)

        return absolutePath, baseLogPath, configPath

    ## Create and configure a localSetup.logger for the script
    def _setupLogger(self):
        logger = logging.getLogger(self.__scriptName)
        FORMAT = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", encoding='utf-8', filemode="a", level=logging.INFO)

        ## Info Log
        infoLogFile = os.path.join(self.baseLogPath, "Info Log.txt")
        logInfo = logging.FileHandler(infoLogFile, mode='a')
        logInfo.setLevel(logging.INFO)
        logInfo.setFormatter(FORMAT)
        logger.addHandler(logInfo)

        ## Warning Log
        warningLogFile = os.path.join(self.baseLogPath, "Warning Log.txt")
        logWarning = logging.FileHandler(warningLogFile, mode='a')
        logWarning.setLevel(logging.WARNING)
        logWarning.setFormatter(FORMAT)
        logger.addHandler(logWarning)

        ## Error Log
        errorLogFile = os.path.join(self.baseLogPath, "Error Log.txt")
        logError = logging.FileHandler(errorLogFile, mode='a')
        logError.setLevel(logging.ERROR)
        logError.setFormatter(FORMAT)
        logger.addHandler(logError)

        return logger
    
    ## Validate term input
    def _validateTerm(self, term: str):
        if term not in termSchoolYearLogic:
            raise ValueError(f"Invalid term: {term}")

    ## Determine current term based on month
    def _determineCurrentTerm(self, currentMonth: int) -> str:
        for term, (start, end) in termMonthRanges.items():
            if start <= currentMonth <= end:
                return term

    ## Determine the previous term given the current term
    def _determinePreviousTerm(self, currentTerm: str) -> str:
        termOrder = list(termMonthRanges.keys())  ## ["Fall", "Spring", "Summer"]
        currentIndex = termOrder.index(currentTerm)
        previousTerm = termOrder[currentIndex - 1] if currentIndex > 0 else termOrder[-1]
        return previousTerm

    ## Determine the next term given the current term
    def _determineNextTerm(self, currentTerm: str) -> str:
        termOrder = list(termMonthRanges.keys())  ## ["Fall", "Spring", "Summer"]
        currentIndex = termOrder.index(currentTerm)
        nextTerm = termOrder[currentIndex + 1] if currentIndex < len(termOrder) - 1 else termOrder[0]
        return nextTerm

    ## Get school year range as tuple
    def _getSchoolYearRange(self, currentTerm: str, currentYear: int) -> tuple[int, int]:
        self._validateTerm(currentTerm)
        if termSchoolYearLogic[currentTerm] == "current-next":
            return currentYear, currentYear + 1
        else:
            return currentYear - 1, currentYear
    
    ## Determine School Year from term and year (wrapper)
    def _determineSchoolYear(self, term: str, year: int) -> str:
        startYear, endYear = self._getSchoolYearRange(term, year)
        return f"{startYear}-{endYear}"

    ## Take in a term and year and determine the term code
    def _determineTermCode(self, term: str, year: int, courseLevel: str) -> str:
        targetTerm = self.__gradTermsWordsToCodesDict.get(term) if courseLevel.lower() == "graduate" else self.__undgTermsDict.get(term)
        return targetTerm + str(year)[2:]

    ## Determine the term name
    def _determineTermName(self, rawTerm: str) -> str:
        refinedTerm = undgTermsCodesToWordsDict[rawTerm] if rawTerm in undgTermsCodesToWordsDict.keys() else gradTermsCodesToWordsDict[rawTerm] if rawTerm in gradTermsCodesToWordsDict.keys() else rawTerm
        return refinedTerm.capitalize()

    ## Get year for term
    def _getYearForTerm(self, term: str, startYear: int, endYear: int) -> int:
        self._validateTerm(term)
        return startYear if termSchoolYearLogic[term] == "current-next" else endYear

    ## Get decade for term
    def _getDecadeForTerm(self, term: str, startDecade: int, endDecade: int) -> int:
        self._validateTerm(term)
        return startDecade if termSchoolYearLogic[term] == "current-next" else endDecade

    ## Create term path for a given term and year
    def _createTermPath(self, term: str, year: int, courseLevel: str):
        ## Get the base Canvas resource path
        canvasOutputPath = self.getInternalResourcePaths("Canvas")

        termName = self._determineTermName(term)

        ## Determine school year using LocalSetup logic
        schoolYear = self._determineSchoolYear(termName, year)

        ## Build the full path for the term
        schoolYearPath = os.path.join(canvasOutputPath, schoolYear)
        termPath = os.path.join(schoolYearPath, termName)  ## Use full term string for folder name

        ## Create the term directory if it does not exist
        os.makedirs(termPath, exist_ok=True)

        ## Store the term path for quick lookup
        self.termPaths[term+str(year)] = termPath
    
    ## Create target designated output path for a given term and designator
    def _createCourseLevelPath(self, courseLevel: str, rawTerm: str, year: int):

        ## Determine term code using LocalSetup logic
        termCode = self._determineTermCode(term=rawTerm, year=year, courseLevel=courseLevel)
        termPathName = rawTerm+str(year)

        ## Ensure term path exists
        if termPathName not in self.termPaths:
            self._createTermPath(rawTerm, year, courseLevel)  ## Reuse updated _createTermPath logic

        ## Create course-level path
        courseLevelPath = os.path.join(self.termPaths[termPathName], courseLevel)
        os.makedirs(courseLevelPath, exist_ok=True)

        ## Store course-level path using termCode
        self.termPaths[termCode] = courseLevelPath


    ## Create target designated output path for a given term and designator
    def _createDepartmentPath(self, rawTerm: str, year: int, targetDesignator: str):

        ## Determine school year using LocalSetup logic 
        schoolYear = self._determineSchoolYear(rawTerm, year)

        ## Build paths
        canvasOutputPath = self.getInternalResourcePaths("Canvas")
        schoolYearPath = os.path.join(canvasOutputPath, schoolYear)
        termPath = os.path.join(schoolYearPath, rawTerm)  ## Use rawTerm for folder name
        os.makedirs(termPath, exist_ok=True)

        ## Create department-specific path
        departmentPath = os.path.join(termPath, "Departments", targetDesignator)
        os.makedirs(departmentPath, exist_ok=True)

        ## Store in internal dictionary
        self.termPaths[rawTerm] = termPath
        self.internalDepartmentOutputPaths[(rawTerm, targetDesignator)] = departmentPath

    ## Public Methods

    ## Return the internal resource path for the given resource type, creating it if necessary
    def getInternalResourcePaths(self, resourceType):
        """
        Public method to get or create the internal resource path for a given resource type.
        """
        if resourceType not in self.internalResourcePathDict:
            resourcePath = os.path.join(self.absolutePath, f"Resources_{resourceType.capitalize()}")
            os.makedirs(resourcePath, exist_ok=True)
            self.internalResourcePathDict[resourceType] = resourcePath
        return self.internalResourcePathDict[resourceType]

    
    ## Return the termPath for a given term code
    def getTermPath(self, term: str, year: int, courseLevel: str):
        """
        Public method to retrieve the term path for a given term and year.
        """
        termName = self._determineTermName(term)
        termPathName = rawTerm+str(year)
        if termPathName not in self.termPaths:
            self._createTermPath(termName, year, courseLevel)
        return self.termPaths[termPathName]

    ## Return the courselevel path for a given courseLevel, term, and year
    def getCourseLevelPath(self, courseLevel: str, term: str, year: int) -> str:
        """
        Public method to retrieve the course level path for a given courseLevel, term, and year.
        """
        termName = self._determineTermName(term)
        termCode = self._determineTermCode(term=term, year=year, courseLevel=courseLevel)
        if termCode not in self.termPaths:
            self._createCourseLevelPath(courseLevel, termName, year)
        return self.termPaths[termCode]

    ## Return or create the target designated output path for a given term and designator
    def getTargetDesignatedOutputPath(self, term: str, year: int, targetDesignator: str) -> str:
        """
        Public method to retrieve the target designated output path for a given term, year, and designator.
        """
        termName = self._determineTermName(term)
        if (termName, targetDesignator) not in self.internalDepartmentOutputPaths:
            self._createDepartmentPath(term, year, targetDesignator)
        return self.internalDepartmentOutputPaths[(termName, targetDesignator)]

    ## Return the external resource path for a given resource type
    def getExternalResourcePath(self, resourceType: str) -> str:
        """
        Public method to retrieve the external resource path for a given resource type.
        """
        return self.externalResourcePaths.get(resourceType)

    ## Return the school year for a given term and year`
    def getSchoolYear(self, term: str, year: int) -> str:
        """
        Return the school year string for the given term and year. ## e.g. "2023-2024"
        """
        return self._determineSchoolYear(term, year)

    ## Return the current school year based on the current date
    def getCurrentSchoolYear(self) -> str:
        """
        Return the current school year based on the current date using _determineSchoolYear and _determineCurrentTerm.
        """
        term = self._determineCurrentTerm(self.dateDict["month"])
        year = self.dateDict["year"]
        return self._determineSchoolYear(term, year)
    
    ## Common function to get terms (full year codes)
    def getTerms(self, month: int, year: int) -> set:
        """
        Return full-year codes for the given month/year.
        ## e.g. ["SU2024", "SG2024"]
        """
        terms = set()
        for term, (start, end) in termMonthRanges.items():
            if start <= month <= end:
                terms.update([f"{undgTermsWordsToCodesDict[term]}{year}", f"{gradTermsWordsToCodesDict[term]}{year}"])
        return terms

    ## Common function to get term codes (decade codes)
    def getTermCodes(self, month: int, decade: int) -> set:
        """
        Return decade codes for the given month/year.
        ## e.g. ["SU24", "SG24"]
        """
        termCodes = set()
        for term, (start, end) in termMonthRanges.items():
            if start <= month <= end:
                termCodes.update([f"{undgTermsWordsToCodesDict[term]}{decade}", f"{gradTermsWordsToCodesDict[term]}{decade}"])
        return termCodes
    
    ## Return the current terms based on the current date
    def getCurrentTerms(self) -> set:
        """
        Return the current terms based on the current date.
        ## e.g. ["SU2024", "SG2024"]
        """
        return self.getTerms(self.dateDict["month"], self.dateDict["year"])

    ## Return the current term codes based on the current date
    def getCurrentTermCodes(self) -> set:
        """
        Return the current term codes based on the current date.
        ## e.g. ["SU24", "SG24"]
        """
        return self.getTermCodes(self.dateDict["month"], self.dateDict["decade"])

    ## Return all terms for the current school year
    def getCurrentSchoolYearTerms(self) -> set:
        """
        Return all unique terms for the current school year using getTerms.
        ## e.g. {"FA2025", "GF2025", "SP2026", "GS2026", "SU2026", "SG2026"}
        """
        currentTerm = self._determineCurrentTerm(self.dateDict["month"])
        startYear, endYear = self._getSchoolYearRange(currentTerm, self.dateDict["year"])

        terms = set()
        for term, logic in termSchoolYearLogic.items():
            yearForTerm = startYear if logic == "current-next" else endYear
            startMonth = termMonthRanges[term][0]
            terms.update(self.getTerms(startMonth, yearForTerm))
        return terms

    ## Return all decade codes for the current school year
    def getCurrentSchoolYearTermCodes(self) -> set:
        """
        Return all decade codes for the current school year using getTermCodes and termSchoolYearLogic.
        ## e.g. {"FA25", "GF25", "SP26", "GS26", "SU26", "SG26"}
        """
        currentTerm = self._determineCurrentTerm(self.dateDict["month"])
        startYear, endYear = self._getSchoolYearRange(currentTerm, self.dateDict["year"])
        startDecade, endDecade = startYear % 100, endYear % 100

        termCodes = set()
        for term, logic in termSchoolYearLogic.items():
            decadeForTerm = startDecade if logic == "current-next" else endDecade
            startMonth = termMonthRanges[term][0]
            termCodes.update(self.getTermCodes(startMonth, decadeForTerm))
        return termCodes

    ## Return the most recent completed terms
    def getMostRecentCompletedTerms(self) -> set:
        """
        Return the most recently completed full-year term codes based on the current date.
        ## e.g. ["SU2024", "SG2024"]
        """
        currentMonth = self.dateDict["month"]
        currentYear = self.dateDict["year"]

        currentTerm = self._determineCurrentTerm(currentMonth)
        previousTerm = self._determinePreviousTerm(currentTerm)

        ## Determine the correct year for the previous term
        previousTermYear = currentYear - 1 if termSchoolYearLogic[previousTerm] == 'current-next' else currentYear

        startYear, endYear = self._getSchoolYearRange(previousTerm, previousTermYear)
        yearForPreviousTerm = startYear if termSchoolYearLogic[previousTerm] == "current-next" else endYear

        return self.getTerms(termMonthRanges[previousTerm][0], yearForPreviousTerm)

    ## Return the most recent completed term codes
    def getMostRecentCompletedTermCodes(self) -> set:
        """
        Return the most recently completed decade term codes based on the current date.
        ## e.g. ["SU24", "SG24"]
        """
        currentMonth = self.dateDict["month"]
        currentYear = self.dateDict["year"]
        currentTerm = self._determineCurrentTerm(currentMonth)
        previousTerm = self._determinePreviousTerm(currentTerm)
        ## Determine the correct year for the previous term
        previousTermYear = currentYear - 1 if termSchoolYearLogic[previousTerm] == 'current-next' else currentYear
        startYear, endYear = self._getSchoolYearRange(previousTerm, previousTermYear)
        decadeForPreviousTerm = (startYear % 100) if termSchoolYearLogic[previousTerm] == "current-next" else (endYear % 100)
        return self.getTermCodes(termMonthRanges[previousTerm][0], decadeForPreviousTerm)

    ## Return the most recent completed decade term codes        
    def getPreviousSchoolYearTerms(self) -> set:
        """
        Return all full-year term codes for the previous school year using dynamic logic.
        ## e.g. {"FA2024", "GF2024", "SP2025", "GS2025", "SU2025", "SG2025"}
        """    
        currentTerm = self._determineCurrentTerm(self.dateDict["month"])
        previousTerm = self._determinePreviousTerm(currentTerm)
        currentYear = self.dateDict["year"]

        ## Determine correct year for previous term
        previousTermYear = currentYear - 1 if termSchoolYearLogic[previousTerm] == 'current-next' else currentYear

        startYear, endYear = self._getSchoolYearRange(previousTerm, previousTermYear)

        terms = set()
        for term, logic in termSchoolYearLogic.items():
            yearForTerm = startYear if logic == "current-next" else endYear
            startMonth = termMonthRanges[term][0]
            terms.update(self.getTerms(startMonth, yearForTerm))
        return terms

    ## Return all terms for the previous school year
    def getPreviousSchoolYearTermCodes(self) -> set:
        """
        Return all decade codes for the previous school year using _determineSchoolYear and termSchoolYearLogic.
        ## e.g. {"FA24", "GF24", "SP25", "GS25", "SU25", "SG25"}
        """
        currentTerm = self._determineCurrentTerm(self.dateDict["month"])
        previousTerm = self._determinePreviousTerm(currentTerm)
        currentYear = self.dateDict["year"]

        previousTermYear = currentYear - 1 if termSchoolYearLogic[previousTerm] == 'current-next' else currentYear

        startYear, endYear = self._getSchoolYearRange(previousTerm, previousTermYear)
        startDecade, endDecade = startYear % 100, endYear % 100

        termCodes = set()
        for term, logic in termSchoolYearLogic.items():
            decadeForTerm = startDecade if logic == "current-next" else endDecade
            startMonth = termMonthRanges[term][0]
            termCodes.update(self.getTermCodes(startMonth, decadeForTerm))
        return termCodes

    ## Return the next term
    def getNextTerms(self) -> set:
        """
        Return the next full-year term codes based on the current date.
        ## e.g. ["FA2024", "GF2024"]
        """
        currentMonth = self.dateDict["month"]
        currentYear = self.dateDict["year"]
        currentTerm = self._determineCurrentTerm(currentMonth)
        nextTerm = self._determineNextTerm(currentTerm)
        ## Determine the correct year for the next term
        nextTermYear = currentYear if termSchoolYearLogic[nextTerm] == 'current-next' else currentYear + 1
        return self.getTerms(termMonthRanges[nextTerm][0], nextTermYear)

    ## Return the next term codes
    def getNextTermCodes(self) -> set:
        """
        Return the next decade term codes based on the current date.
        ## e.g. ["FA24", "GF24"]
        """
        currentMonth = self.dateDict["month"]
        currentYear = self.dateDict["year"]
        currentTerm = self._determineCurrentTerm(currentMonth)
        nextTerm = self._determineNextTerm(currentTerm)
        ## Determine the correct year for the next term
        nextTermYear = currentYear if termSchoolYearLogic[nextTerm] == 'current-next' else currentYear + 1
        nextTermDecade = nextTermYear % 100
        return self.getTermCodes(termMonthRanges[nextTerm][0], nextTermDecade)


    ## Return all terms for the next school year
    def getNextSchoolYearTerms(self) -> set:
        """
        Return all full-year term codes for the next school year using dynamic logic.
        ## e.g. {"FA2026", "GF2026", "SP2027", "GS2027", "SU2027", "SG2027"}
        """
        currentTerm = self._determineCurrentTerm(self.dateDict["month"])
        currentYear = self.dateDict["year"]

        startYear, endYear = self._getSchoolYearRange(currentTerm, currentYear + 1)

        terms = set()
        for term, logic in termSchoolYearLogic.items():
            yearForTerm = startYear if logic == "current-next" else endYear
            startMonth = termMonthRanges[term][0]
            terms.update(self.getTerms(startMonth, yearForTerm))
        return terms

    ## Return all terms for the previous school year
    def getNextSchoolYearTermCodes(self) -> set:
        """
        Return all decade codes for the next school year using _determineSchoolYear and termSchoolYearLogic.
        ## e.g. {"FA26", "GF26", "SP27", "GS27", "SU27", "SG27"}
        """
        currentTerm = self._determineCurrentTerm(self.dateDict["month"])
        currentYear = self.dateDict["year"]

        startYear, endYear = self._getSchoolYearRange(currentTerm, currentYear + 1)

        ## Determine the next school year decades
        startDecade, endDecade = startYear % 100, endYear % 100

        termCodes = set()
        for term, logic in termSchoolYearLogic.items():
            decadeForTerm = startDecade if logic == "current-next" else endDecade
            startMonth = termMonthRanges[term][0]
            termCodes.update(self.getTermCodes(startMonth, decadeForTerm))
        return termCodes