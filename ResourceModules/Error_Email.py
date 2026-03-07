## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

## Import Generic Modules
import traceback, logging, os, sys

try: ## If the module is run directly
    from Core_Microsoft_Api import sendOutlookEmail
except ImportError: ## Otherwise as a relative import if the module is imported
    from .Core_Microsoft_Api import sendOutlookEmail

## Import Config Variables
from Common_Configs import scriptLibrary, serviceEmailAccount, authorContactInformation


## Define the script name, purpose, and external requirements for logging and error reporting purposes
__scriptName = os.path.basename(__file__).replace(".py", "")
scriptPurpose = r"""
This class contains the methods and variables to send error emails \
with the Microsoft API utilizing the local Core Microsoft Api module
"""
externalRequirements = r"""
See https://learn.microsoft.com/en-us/graph/overview?context=graph%2Fapi%2F1.0&view=graph-rest-1.0 \
for the outlook api documentation and setup requirements.
"""

class errorEmail:
    """Used to send function-specific error emails"""

    ## Initializer / Instance Attributes
    def __init__(self, scriptName, scriptPurpose, externalRequirements, p1_localSetup):
        self.scriptName = scriptName
        self.scriptPurpose = scriptPurpose
        self.externalRequirements = externalRequirements
        self.localSetup = p1_localSetup
        self.sentErrors = set()

    ## This class method creates a formatted error email
    def _createErrorEmailBody(self, p1_functionName, p1_errorInfo):
        functionName = "_createErrorEmailBody"

        return f"""To the LMS Admin or the department chair of {scriptLibrary},

An error has occurred in the {self.scriptName} script while running the function "{p1_functionName}"

Details on the purpose and function of the script are below.

{authorContactInformation}

Details regarding this script
Purpose:
{self.scriptPurpose}

Requirements to work properly:
{self.externalRequirements}

Error Description/Code: {p1_errorInfo}
"""

    ## This method sends an error email for a specific function
    def sendError(self, p1_functionName, p1_errorInfo):
        functionName = "Send Error"
        
        ## Log the error
        self.localSetup.logger.error(f"\nA script error occurred while running {p1_functionName}. Error: {str(p1_errorInfo)}")

        ## If the function has already triggered an error email, skip sending again
        if p1_functionName in self.sentErrors:
            self.localSetup.logger.error(f"\nError email already sent for {p1_functionName}")
            return

        
        # Try to get the actual exception object
        exc = p1_errorInfo if isinstance(p1_errorInfo, BaseException) else None

        if exc is None:
            # Fall back to the currently handled exception (if any)
            excType, excValue, excTb = sys.exc_info()
            if excValue is not None:
                exc = excValue

        if exc is not None:
            # Build traceback with locals captured in each frame
            tbExc = traceback.TracebackException.from_exception(exc, capture_locals=True)
            traceWithLocals = ''.join(tbExc.format())
        else:
            # Fallback: no specific exception object, just use standard traceback
            traceWithLocals = traceback.format_exc()


        ## Format the full error info with traceback
        fullErrorInfo = f"{p1_errorInfo}: \n\n{traceWithLocals}"

        ## Log the full error info
        self.localSetup.logger.error(f"\nFull Error Info:\n{fullErrorInfo}")

        ## Create the formatted email body
        emailBody = self._createErrorEmailBody(p1_functionName, fullErrorInfo)

        ## Send the error alert email
        sendOutlookEmail(
            p1_microsoftUserName=serviceEmailAccount,
            p1_subject=f"{self.scriptName}: Error in \"{p1_functionName}\"",
            p1_body=emailBody,
            p1_recipientEmailList=f"{scriptLibrary}@nnu.edu",
            p1_shared_mailbox=f"{scriptLibrary}@nnu.edu"
        )

        ## Track that an error email has been sent for this function
        self.sentErrors.add(p1_functionName)
        self.localSetup.logger.error(f"\nError Email Sent for {p1_functionName}")