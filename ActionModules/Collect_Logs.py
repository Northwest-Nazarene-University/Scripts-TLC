## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

## Import necessary modules
import os, sys, argparse, re
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
    seenLines = set()  ## Track raw line text to prevent duplicates across log files

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

                        ## Skip duplicate lines already seen in another log file
                        if line in seenLines:
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
                                seenLines.add(line)
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
## Sensitive information redaction
## ─────────────────────────────────────────────────────────────────────────────
## Each redacted value gets a deterministic short hash so the same original value
## always maps to the same placeholder across the combined file, letting you
## correlate redacted entries with the true logs without exposing the raw data.
##
## Example:  "brycezmiller@nnu.edu"  →  "<EMAIL_a3f1>"
##           "canvas_course_id=98432" →  "canvas_course_id=<ID_7b02>"

import hashlib

def _shortHash(value, length=4):
    """Return a stable hex tag for *value* so the same input always gets the same placeholder."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:length]


class _Redactor:
    """
    Stateful redactor that builds a lookup table mapping each unique sensitive
    value to a tagged placeholder.  The table is written at the end of the
    combined log file so an operator with access to the true logs can match
    placeholders back to originals by position / hash.
    """

    ## Compiled patterns applied in order; earlier patterns take priority.
    _PATTERNS = [
        ## Bearer / API tokens
        ("TOKEN", re.compile(r"(Bearer\s+)([\w\-\.]+)", re.IGNORECASE), 2),
        ## Generic secrets (access_token, api_key, password, etc.)
        ("SECRET", re.compile(
            r"((?:access_token|api_key|apikey|token|secret|password|Authorization)[=:\s]+)"
            r"([^\s,;\]\)'\"]+)", re.IGNORECASE), 2),
        ## Canvas numeric IDs logged with known prefixes
        ##   canvas_course_id=12345, enrollment 67890, user=111
        ("ID", re.compile(
            r"((?:canvas_course_id|canvas_enrollment_id|enrollment|Instructor_#\d+_ID"
            r"|user_id|user|course_id|account_id|canvasCourseId|enrollmentId)[=:\s]+)"
            r"(\d+)", re.IGNORECASE), 2),
        ## Person names following "Professor(s) " or "Instructor ... name"
        ("NAME", re.compile(
            r"((?:Professors?\s+|Instructor_#\d+_name[=:\s]+))"
            r"([A-Z][a-z]+(?:[\s,]+[A-Z][a-z]+)*)", 0), 2),
        ## Email addresses (broad)
        ("EMAIL", re.compile(r"([\w.+-]+@[\w-]+\.[\w.-]+)"), 1),
        ## Canvas API URLs — redact the domain + path leaving only the endpoint shape
        ("URL", re.compile(r"(https?://[^/]+/api/v1/)([^\s\"']+)"), 2),
    ]

    def __init__(self, workspaceRoot):
        self._workspaceRoot = workspaceRoot
        ## Forward-slash variant for path replacement
        self._workspaceRootFwd = workspaceRoot.replace(os.sep, "/") if workspaceRoot else None
        ## {category: {originalValue: shortHash}}
        self._seen = {}

    def _getPlaceholder(self, category, rawValue):
        """Return a stable placeholder like <EMAIL_a3f1> for the given raw value."""
        if category not in self._seen:
            self._seen[category] = {}
        if rawValue not in self._seen[category]:
            self._seen[category][rawValue] = _shortHash(rawValue)
        return f"<{category}_{self._seen[category][rawValue]}>"

    def redact(self, line):
        """Return a redacted copy of *line*."""
        ## ── Step 1: Replace absolute workspace paths with relative notation ──
        if self._workspaceRoot:
            line = line.replace(self._workspaceRoot + os.sep, "")
            line = line.replace(self._workspaceRoot, "")
        if self._workspaceRootFwd:
            line = line.replace(self._workspaceRootFwd + "/", "")
            line = line.replace(self._workspaceRootFwd, "")

        ## ── Step 2: Regex-based redaction with stable placeholders ───────────
        for category, pattern, captureGroup in self._PATTERNS:
            def _replacer(m, _cat=category, _grp=captureGroup):
                rawValue = m.group(_grp)
                placeholder = self._getPlaceholder(_cat, rawValue)
                ## Rebuild match: everything before the captured group + placeholder
                if _grp == 1:
                    return placeholder
                ## _grp == 2: prefix in group(1), sensitive part in group(2)
                return m.group(1) + placeholder
            line = pattern.sub(_replacer, line)

        return line

    def getSummaryLines(self):
        """
        Return summary lines that list how many unique values were redacted per
        category (without revealing the values themselves).
        """
        lines = ["", "## ── Redaction Summary ──"]
        for category, mapping in sorted(self._seen.items()):
            lines.append(f"##   {category}: {len(mapping)} unique value(s) redacted")
        if not self._seen:
            lines.append("##   No sensitive values detected.")
        return lines


## ─────────────────────────────────────────────────────────────────────────────
## Write collected lines to a single output file
## ─────────────────────────────────────────────────────────────────────────────
def writeCombinedLog(lines, outputPath):
    """Write the sorted, redacted log lines to a combined output file."""
    ## Create a stateful redactor for consistent placeholders across the file
    redactor = _Redactor(localSetup.absolutePath)

    with open(outputPath, "w", encoding="utf-8") as f:
        currentScript = None
        for lineDatetime, scriptDir, logType, rawLine in lines:
            ## Add a visual separator when the source script changes
            if scriptDir != currentScript:
                if currentScript is not None:
                    f.write("\n")
                f.write(f"## ── {scriptDir} ({logType}) ──\n")
                currentScript = scriptDir
            f.write(redactor.redact(rawLine) + "\n")

        ## Append redaction summary so the reader knows what was masked
        for summaryLine in redactor.getSummaryLines():
            f.write(summaryLine + "\n")


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
        startInput = input("Enter start date (YYYY-MM-DD), or press Enter to use today: ").strip()
        startDate = datetime.strptime(startInput, "%Y-%m-%d").date() if startInput else datetime.now().date()

    if args.end:
        endDate = datetime.strptime(args.end, "%Y-%m-%d").date()
    else:
        endInput = input(f"Enter end date (YYYY-MM-DD), or press Enter to use start date ({startDate}): ").strip()
        endDate = datetime.strptime(endInput, "%Y-%m-%d").date() if endInput else startDate

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
