## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Copilot

from datetime import datetime
import os
import sys
import pandas as pd

## Add Script repository to syspath
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

try:  ## Irregular try clause, do not comment out in testing
    from Local_Setup import LocalSetup
    from Canvas_Report import CanvasReport
    from TLC_Common import isPresent, isMissing
    from Error_Email import errorEmail
except ImportError:
    from ResourceModules.Local_Setup import LocalSetup
    from ResourceModules.Canvas_Report import CanvasReport
    from ResourceModules.TLC_Common import isPresent, isMissing
    from ResourceModules.Error_Email import errorEmail

## Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = os.path.basename(__file__).replace(".py", "")

scriptPurpose = r"""
This script retrieves Canvas provisioning admins reports for all existing sub-accounts and
combines them into a single All_Admins.csv file.
"""
externalRequirements = r"""
To function properly this script requires:
1. Access to Canvas provisioning report endpoints.
2. Access to local Canvas resource directories configured by Local_Setup.
"""

## Setup shared helpers
localSetup = LocalSetup(datetime.now(), __file__)
errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)


def _getColumnNameByAliases(p1_sourceDf, p1_aliases):
    """Resolve the first matching column name from a list of aliases.

    Args:
        p1_sourceDf (pd.DataFrame): DataFrame to inspect.
        p1_aliases (list[str]): Candidate column names.

    Returns:
        str: Matching column name, or empty string when not found.
    """
    ## Step 1: Return empty when DataFrame is missing.
    if isMissing(p1_sourceDf):
        return ""

    ## Step 2: Return first matching alias.
    for candidateColumn in p1_aliases:
        if candidateColumn in p1_sourceDf.columns:
            return candidateColumn

    ## Step 3: Fallback when no alias is available.
    return ""


def _getExistingSubAccountsDf(p1_accountsDf):
    """Filter the accounts DataFrame to existing sub-accounts only.

    Args:
        p1_accountsDf (pd.DataFrame): Accounts provisioning DataFrame.

    Returns:
        pd.DataFrame: Existing sub-accounts with root removed.
    """
    ## Step 1: Return empty DataFrame when accounts data is unavailable.
    if isMissing(p1_accountsDf):
        return pd.DataFrame()

    subAccountsDf = p1_accountsDf.copy()

    ## Step 2: Keep only rows where canvas account id is present and not root account id 1.
    canvasAccountIdColumn = _getColumnNameByAliases(
        p1_sourceDf=subAccountsDf,
        p1_aliases=["canvas_account_id", "account_id"],
    )
    if not canvasAccountIdColumn:
        return pd.DataFrame()

    subAccountsDf = subAccountsDf[pd.to_numeric(subAccountsDf[canvasAccountIdColumn], errors="coerce").notna()].copy()
    subAccountsDf = subAccountsDf[
        pd.to_numeric(subAccountsDf[canvasAccountIdColumn], errors="coerce") != 1
    ].copy()

    ## Step 3: Restrict to non-deleted/non-inactive rows when status columns are available.
    statusColumn = _getColumnNameByAliases(
        p1_sourceDf=subAccountsDf,
        p1_aliases=["status", "workflow_state", "account_status"],
    )
    if statusColumn:
        activeStatusValues = {"active", "available", "current", "concluded"}
        subAccountsDf = subAccountsDf[
            subAccountsDf[statusColumn]
            .astype(str)
            .str.lower()
            .isin(activeStatusValues)
        ].copy()

    deletedAtColumn = _getColumnNameByAliases(
        p1_sourceDf=subAccountsDf,
        p1_aliases=["deleted_at"],
    )
    if deletedAtColumn:
        subAccountsDf = subAccountsDf[subAccountsDf[deletedAtColumn].isna()].copy()

    return subAccountsDf


def createAllAdminsReport():
    """Create a combined Canvas admins provisioning report for all existing sub-accounts.

    Args:
        None

    Returns:
        str: Full file path to the generated All_Admins.csv file.
    """
    functionName = "Create All Admins Report"

    try:
        ## Step 1: Load account data and select existing sub-accounts.
        accountsDf = CanvasReport.getAccountsDf(localSetup)
        existingSubAccountsDf = _getExistingSubAccountsDf(accountsDf)

        if existingSubAccountsDf.empty:
            localSetup.logger.warning(f"{functionName}: No existing sub-accounts found.")

        canvasAccountIdColumn = _getColumnNameByAliases(
            p1_sourceDf=existingSubAccountsDf,
            p1_aliases=["canvas_account_id", "account_id"],
        )
        accountNameColumn = _getColumnNameByAliases(
            p1_sourceDf=existingSubAccountsDf,
            p1_aliases=["name", "account_name"],
        )

        combinedAdminsDfList = []

        ## Step 2: Pull admins provisioning report for each sub-account.
        for _, accountRow in existingSubAccountsDf.iterrows():
            accountCanvasId = int(float(accountRow[canvasAccountIdColumn]))
            accountName = (
                str(accountRow[accountNameColumn]).strip()
                if accountNameColumn and not isMissing(accountRow[accountNameColumn])
                else str(accountCanvasId)
            )

            try:
                adminsReport = CanvasReport(
                    localSetup=localSetup,
                    reportType="admins",
                    accountCanvasID=accountCanvasId,
                    accountName=accountName,
                    filename=f"All_Admins_{accountCanvasId}.csv",
                )
                accountAdminsDf = adminsReport.getCurrentDataFrame()
            except Exception as Error:
                localSetup.logger.error(
                    f"{functionName}: Failed account {accountCanvasId} ({accountName}). Error: {Error}"
                )
                continue

            if not isPresent(accountAdminsDf):
                continue

            accountAdminsDf = accountAdminsDf.copy()
            accountAdminsDf["source_canvas_account_id"] = accountCanvasId
            accountAdminsDf["source_account_name"] = accountName
            combinedAdminsDfList.append(accountAdminsDf)

        ## Step 3: Combine and save All_Admins.csv to Canvas resources root.
        if combinedAdminsDfList:
            combinedAdminsDf = pd.concat(combinedAdminsDfList, ignore_index=True)
        else:
            combinedAdminsDf = pd.DataFrame(
                columns=["source_canvas_account_id", "source_account_name"]
            )

        combinedAdminsDf = combinedAdminsDf.drop_duplicates().reset_index(drop=True)
        outputFilePath = os.path.join(
            localSetup.getInternalResourcePaths("Canvas"),
            "All_Admins.csv",
        )
        combinedAdminsDf.to_csv(outputFilePath, index=False)

        localSetup.logger.info(
            f"{functionName}: Saved {len(combinedAdminsDf)} rows to {outputFilePath}"
        )
        return outputFilePath

    except Exception as Error:
        errorHandler.sendError(functionName, Error)
        return ""


if __name__ == "__main__":
    createdFile = createAllAdminsReport()
    if createdFile:
        print(f"Created: {createdFile}")
    input("Press enter to exit")
