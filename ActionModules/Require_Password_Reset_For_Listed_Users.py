## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

import os, sys, threading
from datetime import datetime

## Add the resource modules path
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

## Try direct imports if run as main, else relative for package usage
try:
    from Local_Setup import LocalSetup
    from Error_Email import errorEmail
    from TLC_Action import requirePasswordReset, terminateAllUserSessions
    from TLC_Common import readTargetCsv, runThreadedRows
except ImportError:  ## When imported as a package/module
    from .Local_Setup import LocalSetup
    from .Error_Email import errorEmail
    from .TLC_Action import requirePasswordReset, terminateAllUserSessions
    from .TLC_Common import readTargetCsv, runThreadedRows

## Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = os.path.basename(__file__).replace(".py", "")
scriptPurpose = r"""
This script reads a CSV file containing Canvas user IDs and:
1) Requires password reset for each user
2) Terminates all user sessions
"""
externalRequirements = r"""
To function properly, this script requires:
- Valid Canvas API configuration in Common_Configs.
- A CSV file named "Target_Canvas_User_Ids.csv" located in the Canvas internal
  resources directory (LocalSetup.getInternalResourcePaths("Canvas")) with column:
    - canvas_user_id
"""

## Read the CSV and require password reset / terminate sessions for each listed Canvas user
def requirePasswordResetForListedUsers(localSetup: LocalSetup, errorHandler: errorEmail) -> None:
    """
    Process a target CSV of Canvas user IDs and perform account security actions.

    The function:
        1. Loads and validates the target CSV.
        2. Runs per-user actions concurrently.
        3. Requires password reset for each user.
        4. Terminates active sessions for users whose reset succeeded.
        5. Logs a summary tally of outcomes.

    Args:
        localSetup (LocalSetup): Shared setup object for paths, logging, and API configuration.
        errorHandler (errorEmail): Error handler used to send function-level error notifications.

    Returns:
        None
    """
    functionName = "requirePasswordResetForListedUsers"
    try:
        ## Step 1: Load and validate the target CSV
        csvPath = os.path.join(
            localSetup.getInternalResourcePaths("Canvas"),
            "Target_Canvas_User_Ids.csv",
        )

        df = readTargetCsv(
            localSetup,
            errorHandler,
            csvPath,
            requiredColumns=["canvas_user_id"],
        )
        if df.empty:
            localSetup.logInfoThreadSafe(f"{functionName}: No rows found in target CSV.")
            return

        ## Step 2: Initialize a thread-safe tally for summary reporting
        tallyLock = threading.Lock()
        tally = {
            "total": len(df),
            "password_reset_success": 0,
            "password_reset_failure": 0,
            "session_termination_success": 0,
            "session_termination_failure": 0,
            "full_success": 0,  ## both reset + terminate succeeded
        }

        ## Step 3: Define the per-row worker
        def _worker(row):
            userId = str(row["canvas_user_id"]).replace(".0", "").strip()

            resetOk = requirePasswordReset(localSetup, errorHandler, userId)

            terminateOk = False
            if resetOk:
                terminateOk = terminateAllUserSessions(localSetup, errorHandler, userId)

            with tallyLock:
                if resetOk:
                    tally["password_reset_success"] += 1
                else:
                    tally["password_reset_failure"] += 1

                if terminateOk:
                    tally["session_termination_success"] += 1
                else:
                    ## Count as failure when reset failed (not attempted) or terminate failed
                    tally["session_termination_failure"] += 1

                if resetOk and terminateOk:
                    tally["full_success"] += 1

        ## Step 4: Process all rows concurrently
        runThreadedRows(df, _worker)

        ## Step 5: Write completion summary
        localSetup.logInfoThreadSafe(
            f"{functionName} completed. "
            f"Total: {tally['total']} | "
            f"Reset Success: {tally['password_reset_success']} | "
            f"Reset Failure: {tally['password_reset_failure']} | "
            f"Session Termination Success: {tally['session_termination_success']} | "
            f"Session Termination Failure: {tally['session_termination_failure']} | "
            f"Full Success: {tally['full_success']}"
        )

    except Exception as Error:
        errorHandler.sendError(functionName, Error)

if __name__ == "__main__":
    ## Initialize shared LocalSetup (paths, logging)
    localSetup = LocalSetup(datetime.now(), __file__)

    ## Setup the error handler (sends one email per function error)
    errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)

    requirePasswordResetForListedUsers(localSetup, errorHandler)

    input("Press enter to exit")