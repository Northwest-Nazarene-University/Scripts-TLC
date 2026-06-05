# ReportModules

Read-focused scripts that gather, process, and export institutional reporting data from Canvas LMS and related systems.

---

## Purpose

`ReportModules` generates recurring reports for institutional assessment, compliance, retention, and administration:

- **Outcomes reporting** — GE and program-level assessment results, outcome attachment verification
- **Syllabi compliance** — University syllabi retrieval, addendum verification
- **Enrollment & instructor reporting** — Inactive enrollments, non-official instructor identification
- **Retention & student success** — Nighthawk 360 grade/activity data, incoming student reports
- **Administrative reporting** — All admins report, course-level grade exports

---

## Modules

### `Outcome_Results_Report.py`

Processes GE and program-level outcome results from Canvas for institutional assessment reporting.

| Item | Details |
|------|---------|
| **Entry function** | `termProcessOutcomeResults(termCode, targetDesignator)` |
| **Data sources** | Canvas Outcomes API, provisioning reports, Automated Outcome Tool Variables |
| **Output** | Outcome results data organized by term and designator |
| **Schedule** | First run daily + bi-weekly on Fridays (1st and 3rd of month) |

---

### `Outcome_Attachment_Report.py`

Verifies that required outcomes are properly attached to active courses.

| Item | Details |
|------|---------|
| **Entry function** | `termOutcomeAttachmentReport(termCode, targetDesignator)` |
| **Data sources** | Canvas course outcomes data, Automated Outcome Tool Variables |
| **Output** | Report identifying courses missing required outcome attachments |
| **Schedule** | First run daily + bi-weekly on Fridays |

---

### `Syllabi_Report.py`

Retrieves and catalogs university syllabi from Canvas courses for compliance tracking.

| Item | Details |
|------|---------|
| **Data sources** | Canvas course syllabus body/tab, course file attachments |
| **Output** | Downloaded syllabi files organized by term, account, and course |
| **Schedule** | First run of the day |

---

### `Syllabus_Addendum_Report.py`

Checks courses for required syllabus addendum compliance (verifies the presence of institutional addendum content).

| Item | Details |
|------|---------|
| **Data sources** | Canvas course syllabus content |
| **Output** | Compliance report identifying courses missing the required addendum |
| **Schedule** | First run of the day |

---

### `Nighthawk_360_Canvas_Report.py`

Generates student-level grade and activity data for the Nighthawk 360 retention and student success platform.

| Item | Details |
|------|---------|
| **Entry function** | `Nighthawk360CanvasReport()` |
| **Data sources** | Canvas enrollments, grades, last-activity timestamps |
| **Output** | Formatted data export for ingestion by the Nighthawk 360 platform |
| **Schedule** | Last run of the day (after 7:00 PM) |

---

### `Incoming_Student_Report.py`

Combines Canvas enrollment data with Slate CRM admissions data to produce comprehensive incoming student reports.

| Item | Details |
|------|---------|
| **Entry function** | `termGetIncomingStudentsInfo(termCode)` |
| **Data sources** | Canvas provisioning reports (users, enrollments), Slate SFTP CSV files |
| **Output** | Combined incoming student data for the specified term |
| **Schedule** | Runs multiple times daily as part of the four-times-daily process |

---

### `Non_Official_Instructors_Report.py`

Identifies non-instructor-of-record enrollments in courses (e.g., TAs, designers, observers with elevated access).

| Item | Details |
|------|---------|
| **Data sources** | Canvas enrollment provisioning reports |
| **Output** | Report listing non-official instructor enrollments per course |

---

### `Inactive_Enrollments_Report.py`

Reports on student enrollments that appear inactive based on last-activity data.

| Item | Details |
|------|---------|
| **Data sources** | Canvas enrollment and user activity data |
| **Output** | Report identifying inactive student enrollments |

---

### `All_Admins_Report.py`

Retrieves Canvas provisioning admin reports for all existing sub-accounts and combines them into a single consolidated file.

| Item | Details |
|------|---------|
| **Data sources** | Canvas provisioning admin reports across all sub-accounts |
| **Output** | `All_Admins.csv` — consolidated list of all Canvas administrators |

---

### `Course_Grades_By_Course_Report.py`

Generates one Excel file per Canvas SIS course containing grades for all active SIS-enrolled students across all published assignments.

| Item | Details |
|------|---------|
| **Data sources** | Canvas Grades API, SIS enrollment/course files |
| **Output** | Per-course Excel files saved to an account/instructor/SIS-metadata path structure |
| **Processing** | Uses threaded grade exports for performance |

---

## How Reports Are Orchestrated

Reports are not run independently — they are called by `IDT_Canvas_Primary.py` based on time-of-day and day-of-week logic:

```
First Run of Day (before 6:00 AM):
├── Outcome Results Report (per term × designator)
├── Outcome Attachment Report (per term × designator)
├── Syllabi Report
├── Syllabus Addendum Report
└── Incoming Student Report (per term)

Multiple Times Daily:
└── Incoming Student Report (per term)

Last Run of Day (after 7:00 PM):
└── Nighthawk 360 Canvas Report
```

---

## Common Pattern

All report modules follow this initialization pattern:

```python
from datetime import datetime
from ResourceModules.Local_Setup import LocalSetup
from ResourceModules.Error_Email import errorEmail

scriptName = os.path.basename(__file__).replace(".py", "")
scriptPurpose = r"""..."""
externalRequirements = r"""..."""

localSetup = LocalSetup(datetime.now(), __file__)
errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)
```

---

## Dependencies

All report modules depend on `ResourceModules` for:

- `LocalSetup` — paths, logging, term logic
- `CanvasReport` — Canvas provisioning report retrieval
- `TLC_Common` — API calls, file utilities, missing-value checks
- `Error_Email` — automated error notifications

Some also use:
- `Get_Slate_Info` — Slate SFTP data (Incoming Student Report)
- `Core_Microsoft_Api` — SharePoint/email operations
- `TLC_Action` — course week calculations, threaded processing

---

## Adding a New Report Module

1. Create a new `.py` file in this directory
2. Follow the initialization pattern above (LocalSetup + errorEmail)
3. Wrap main logic in a function with try/except calling `errorHandler.sendError()`
4. Import and call your function from `IDT_Canvas_Primary.py` at the appropriate schedule point
5. Use `CanvasReport` class methods to retrieve Canvas data as DataFrames
6. Use `isFileRecent()` to avoid re-downloading data that is already current
