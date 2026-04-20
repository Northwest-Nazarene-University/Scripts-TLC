## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

## Import Generic Modules
import traceback, logging, os, sys, threading

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
        self._sendErrorLock = threading.RLock()

    def _logError(self, message):
        if hasattr(self.localSetup, "logErrorThreadSafe"):
            self.localSetup.logErrorThreadSafe(message)
        else:
            self.localSetup.logger.error(message)

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
        with self._sendErrorLock:
            ## Log the error
            self._logError(f"\nA script error occurred while running {p1_functionName}. Error: {str(p1_errorInfo)}")

            ## If the function has already triggered an error email, skip sending again
            if p1_functionName in self.sentErrors:
                self._logError(f"\nError email already sent for {p1_functionName}")
                return

            ## ---- Sensitive-keyword list (lowercase) ----
            _SENSITIVE_KEYS = {"password", "passwd", "secret", "token", "api_key",
                            "apikey", "access_token", "refresh_token", "credential",
                            "client_secret", "authorization", "auth"}

            
            # Try to get the actual exception object
            exc = p1_errorInfo if isinstance(p1_errorInfo, BaseException) else None

            if exc is None:
                # Fall back to the currently handled exception (if any)
                excType, excValue, excTb = sys.exc_info()
                if excValue is not None:
                    exc = excValue

            if exc is not None:
                # Build traceback WITH locals for the full log file
                tbExcFull = traceback.TracebackException.from_exception(exc, capture_locals=True)
                traceWithLocals = ''.join(tbExcFull.format())

                # Build a sanitized version for the email (no locals)
                tbExcSafe = traceback.TracebackException.from_exception(exc, capture_locals=False)
                traceSafe = ''.join(tbExcSafe.format())
            else:
                # Fallback: no specific exception object
                traceWithLocals = traceback.format_exc()
                traceSafe = traceWithLocals


            ## ---- Redact sensitive values from the safe trace ----
            for line in traceSafe.splitlines():
                for key in _SENSITIVE_KEYS:
                    if key in line.lower():
                        traceSafe = traceSafe.replace(line, f"  {key}=<REDACTED>")
                        break

            ## Full info for the LOCAL log only (includes locals for debugging)
            fullErrorInfo = f"{p1_errorInfo}: \n\n{traceWithLocals}"

            ## Log the full (unsanitized) error info locally
            self._logError(f"\nFull Error Info:\n{fullErrorInfo}")

            ## Save the sanitized error info for the email (no locals, sensitive info redacted)
            safeErrorInfo = f"{p1_errorInfo}: \n\n{traceSafe}"

            ## Create the formatted email body
            emailBody = self._createErrorEmailBody(p1_functionName, safeErrorInfo)

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
            self._logError(f"\nError Email Sent for {p1_functionName}")
