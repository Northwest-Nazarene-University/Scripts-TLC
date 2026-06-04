## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

## Import Generic Modules
import os, sys
from datetime import datetime

## Add the resource modules path
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

## Try direct imports if run as main, else relative for package usage
try:
    from Local_Setup import LocalSetup
    from Error_Email import errorEmail
    from TLC_Common import readTargetCsv, runThreadedRows, makeApiCall
    from Common_Configs import coreCanvasApiUrl
except ImportError:  ## When imported as a package/module
    from .Local_Setup import LocalSetup
    from .Error_Email import errorEmail
    from .TLC_Common import readTargetCsv, runThreadedRows, makeApiCall
    from .Common_Configs import coreCanvasApiUrl

## Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = os.path.basename(__file__).replace(".py", "")
scriptPurpose = r"""
This script reads a CSV file containing Canvas user IDs and makes API calls to delete each user.
"""
externalRequirements = r"""
To function properly, this script requires:
- Valid Canvas API configuration in Common_Configs (coreCanvasApiUrl).
- A CSV file named "Target_Canvas_User_Ids.csv" located in the Canvas internal
  resources directory (LocalSetup.getInternalResourcePaths("Canvas")) with column:
    - canvas_user_id
"""

## Read the CSV and delete each listed user
def deleteListedUsers(
    localSetup: LocalSetup,
    errorHandler: errorEmail,
    p1_useThreading: bool = True,
) -> None:
    """
    Process a target CSV of Canvas user IDs and delete each user from Canvas.

    Args:
        localSetup (LocalSetup): Shared setup object for paths, logging, and API configuration.
        errorHandler (errorEmail): Error handler used to send function-level error notifications.
        p1_useThreading (bool): True to process rows concurrently; False for non-threaded testing.

    Returns:
        None
    """
    functionName = "deleteListedUsers"
    try:
        ## Step 1: Load and validate the target CSV
        csvPath = os.path.join(
            localSetup.getInternalResourcePaths("Canvas"), "Target_Canvas_User_Ids.csv"
        )
        df = readTargetCsv(
            localSetup,
            errorHandler,
            csvPath,
            requiredColumns=["canvas_user_id"],
        )
        if df.empty:
            return

        ## Step 2: Define the per-row worker
        def _worker(row):
            userId = str(row["canvas_user_id"]).replace(".0", "").strip()
            if not userId:
                localSetup.logWarningThreadSafe(f"{functionName}: Skipping row with missing canvas_user_id")
                return

            deleteUrl = f"{coreCanvasApiUrl}accounts/1/users/{userId}"
            payload = {"event": "delete"}
            apiResult = makeApiCall(
                localSetup=localSetup,
                p1_apiUrl=deleteUrl,
                p1_payload=payload,
                p1_apiCallType="delete",
            )
            if not apiResult:
                localSetup.logWarningThreadSafe(
                    f"Failed to delete user {userId}. No response received."
                )
                return

            response, _ = apiResult
            statusCode = getattr(response, "status_code", None)
            if statusCode == 200:
                localSetup.logInfoThreadSafe(f"Successfully deleted user {userId}")
            else:
                localSetup.logWarningThreadSafe(
                    f"Failed to delete user {userId}. Status code: {statusCode}"
                )

        ## Step 3: Process rows using selected mode (threaded or non-threaded)
        if p1_useThreading:
            ## Step 3a: Process all rows concurrently (default)
            runThreadedRows(df, _worker)
            modeLabel = "threaded"
        else:
            ## Step 3b: Non-threaded testing path (row-by-row)
            for _, row in df.iterrows():
                _worker(row)
            modeLabel = "non-threaded"

        ## Step 4: Log completion summary
        localSetup.logInfoThreadSafe(
            f"{functionName} completed in {modeLabel} mode. Processed {len(df)} users."
        )

    except Exception as Error:
        errorHandler.sendError(functionName, Error)

if __name__ == "__main__":
    ## Initialize shared LocalSetup (paths, logging)
    localSetup = LocalSetup(datetime.now(), __file__)

    ## Setup the error handler (sends one email per function error)
    errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)

    ## Non-threaded testing section:
    ## Set p1_useThreading=False to run row-by-row without concurrency.
    ## deleteListedUsers(localSetup, errorHandler, p1_useThreading=False)

    ## Default run mode (threaded)
    deleteListedUsers(localSetup, errorHandler, p1_useThreading=True)

    input("Press enter to exit")
