## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

## Import necessary modules
import os, sys, argparse
from datetime import datetime, timedelta

## Add Script repository to syspath
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

## Import local modules
try:
    from Local_Setup import LocalSetup
except ImportError:
    from ResourceModules.Local_Setup import LocalSetup

## Create the localsetup variable
localSetup = LocalSetup(datetime.now(), __file__)

## ─────────────────────────────────────────────────────────────────────────────
## Core: Collect and merge log lines from all script log directories
## ─────────────────────────────────────────────────────────────────────────────
## Log level hierarchy for filtering
_LEVEL_RANK = {"info": 0, "warning": 1, "error": 2}

def collectLogs(startDate, endDate, minLevel="info"):
    """
    Walk every subdirectory under the Logs root, read all .txt log files,
    filter lines to the given date range and minimum level, and return them sorted by timestamp.
    """
    logsRoot = os.path.dirname(localSetup.baseLogPath)  ## e.g. .../Logs
    minRank = _LEVEL_RANK.get(minLevel, 0)

    allLines = []

    for scriptDir in os.listdir(logsRoot):
        scriptLogPath = os.path.join(logsRoot, scriptDir)
        if not os.path.isdir(scriptLogPath):
            continue

        for logFile in os.listdir(scriptLogPath):
            if not logFile.endswith(".txt"):
                continue

            logFilePath = os.path.join(scriptLogPath, logFile)
            logType = logFile.replace(".txt", "").strip()  ## e.g. "Info Log"

            try:
                with open(logFilePath, "r", encoding="utf-8", errors="replace") as f:
                    for line in f:
                        line = line.rstrip("\n")
                        if not line.strip():
                            continue

                        ## Try to parse the timestamp from the start of the line
                        lineDatetime = _parseLineTimestamp(line)
                        if lineDatetime is None:
                            continue

                        ## Check if the line falls within the date range
                        if startDate <= lineDatetime.date() <= endDate:
                            ## Check if the line meets the minimum level
                            lineLevel = _parseLineLevel(line)
                            if _LEVEL_RANK.get(lineLevel, 0) >= minRank:
                                allLines.append((lineDatetime, scriptDir, logType, line))
            except Exception:
                continue

    ## Sort all collected lines by timestamp
    allLines.sort(key=lambda x: x[0])
    return allLines


def _parseLineTimestamp(line):
    """
    Attempt to parse a datetime from the beginning of a log line.
    Expected format: 2025-06-13 15:44:40,609 - LEVEL - message
    Returns a datetime or None.
    """
    try:
        ## Take the first 23 characters: "2025-06-13 15:44:40,609"
        rawTs = line[:23]
        return datetime.strptime(rawTs, "%Y-%m-%d %H:%M:%S,%f")
    except (ValueError, IndexError):
        return None


def _parseLineLevel(line):
    """
    Extract the log level from a log line.
    Expected format: 2025-06-13 15:44:40,609 - LEVEL - message
    Returns 'info', 'warning', or 'error' (lowercase), or 'info' as default.
    """
    try:
        ## Level sits between the first and second " - " delimiters
        parts = line.split(" - ", 2)
        if len(parts) >= 2:
            return parts[1].strip().lower()
    except Exception:
        pass
    return "info"


## ─────────────────────────────────────────────────────────────────────────────
## Write collected lines to a single output file
## ─────────────────────────────────────────────────────────────────────────────
def writeCombinedLog(lines, outputPath):
    """Write the sorted log lines to a combined output file."""
    with open(outputPath, "w", encoding="utf-8") as f:
        currentScript = None
        for lineDatetime, scriptDir, logType, rawLine in lines:
            ## Add a visual separator when the source script changes
            if scriptDir != currentScript:
                if currentScript is not None:
                    f.write("\n")
                f.write(f"## ── {scriptDir} ({logType}) ──\n")
                currentScript = scriptDir
            f.write(rawLine + "\n")


## ─────────────────────────────────────────────────────────────────────────────
## Main
## ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Collect all TLC script logs into a single file, filtered by date."
    )
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="Start date (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="End date (YYYY-MM-DD). Defaults to the start date.",
    )
    parser.add_argument(
        "--level",
        type=str,
        default=None,
        choices=["info", "warning", "error"],
        help="Minimum log level: info (all), warning, or error. Prompted if not provided.",
    )
    args = parser.parse_args()

    ## Resolve dates
    if args.start:
        startDate = datetime.strptime(args.start, "%Y-%m-%d").date()
    else:
        startDate = datetime.now().date()

    if args.end:
        endDate = datetime.strptime(args.end, "%Y-%m-%d").date()
    else:
        endDate = startDate

    if endDate < startDate:
        print("Error: end date cannot be before start date.")
        return

    ## Resolve log level
    if args.level:
        minLevel = args.level.lower()
    else:
        print("\nSelect minimum log level:")
        print("  1. Info (all logs)")
        print("  2. Warning and above")
        print("  3. Error only")
        levelChoice = input("Enter choice (1/2/3): ").strip()
        levelMap = {"1": "info", "2": "warning", "3": "error"}
        minLevel = levelMap.get(levelChoice, "info")

    print(f"Collecting logs from {startDate} to {endDate} (level: {minLevel} and above)...")

    ## Collect and sort
    lines = collectLogs(startDate, endDate, minLevel)

    if not lines:
        print("No log entries found for the specified date range.")
        return

    ## Write output
    logsRoot = os.path.dirname(localSetup.baseLogPath)
    if startDate == endDate:
        outputFileName = f"Combined_Logs_{startDate.strftime('%Y-%m-%d')}.txt"
    else:
        outputFileName = f"Combined_Logs_{startDate.strftime('%Y-%m-%d')}_to_{endDate.strftime('%Y-%m-%d')}.txt"

    outputPath = os.path.join(logsRoot, outputFileName)
    writeCombinedLog(lines, outputPath)

    print(f"Wrote {len(lines)} log entries to:\n  {outputPath}")


if __name__ == "__main__":
    os.chdir(os.path.dirname(__file__))
    main()
