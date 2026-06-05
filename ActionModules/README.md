# ActionModules

Write-focused scripts that perform operational updates in Canvas LMS and external systems.

---

## Purpose

`ActionModules` contains task-specific automations that **modify** data in Canvas and third-party platforms:

- **Course lifecycle management** — term-based automated actions (outcome emails, settings changes)
- **Outcome management** — attaching required outcomes to active courses
- **Integration uploads** — catalog and course editor data to Simple Syllabus via SFTP
- **Auto-enrollment** — enrolling student populations in hub/community courses
- **Bulk operations** — CSV-driven mass changes to courses, enrollments, accounts, terms
- **Cleanup & maintenance** — removing orphaned SIS items, fixing discussion settings
- **Security actions** — password resets, user session termination, user deletion
- **Communications** — automated department communication emails to instructors

---

## Scheduled Modules (Run by IDT_Canvas_Primary.py)

These modules are invoked automatically by the orchestrator script:

### `Course_Date_Related_Actions.py`

Determines what date-based course lifecycle actions need to be taken for a specific term and performs them (e.g., sending outcome-related emails to instructors at key points in the semester).

| Item | Details |
|------|---------|
| **Entry function** | `termDetermineAndPerformRelevantActions(termCode, targetDesignator)` |
| **Schedule** | First run daily + bi-weekly |
| **Inputs** | Canvas course/enrollment data, Automated Outcome Tool Variables, communication templates |
| **Actions** | Sends instructor emails, triggers outcome-related processes based on course dates |

---

### `Add_Outcomes_to_Active_Courses.py`

Attaches required learning outcomes to active courses that are missing them.

| Item | Details |
|------|---------|
| **Schedule** | Called as part of Course_Date_Related_Actions workflow |
| **Inputs** | Automated Outcome Tool Variables, Canvas outcome/course data |
| **Actions** | Canvas API calls to link outcomes to course outcome groups |

---

### `Change_Syllabus_Tab.py`

Updates the syllabus tab visibility and settings across active SIS courses.

| Item | Details |
|------|---------|
| **Entry function** | `updateCourseSyllabusTab(sisCourseId)` |
| **Schedule** | Multiple times daily (for each active SIS course) |
| **Actions** | Canvas API call to update course tab settings |

---

### `Send_Catalog_To_Simple_Syllabus.py`

Retrieves TUG and GPS catalog course data from CleanCatalog, formats it into the Simple Syllabus Course Extract format, and uploads via SFTP.

| Item | Details |
|------|---------|
| **Entry function** | `processCatalogCoursesAndUploadToSimpleSyllabus()` |
| **Schedule** | First run of the day |
| **Data sources** | CleanCatalog HTML pages (parsed with BeautifulSoup) |
| **Output** | CSV uploaded to Simple Syllabus SFTP server |
| **Config** | `catalogToSimpleSyllabusConfig` in Common_Configs |

---

### `Send_Course_Editors_To_Simple_Syllabus.py`

Sends course editor/instructor assignment data to Simple Syllabus for syllabus workflow routing.

| Item | Details |
|------|---------|
| **Entry function** | `processCourseEditorsAndUploadToSimpleSyllabus()` |
| **Schedule** | First run of the day (after catalog upload) |
| **Inputs** | `NNU_Course_Editor_File*.csv` and `Course Editor Input.csv` from config path |
| **Output** | CSV uploaded to Simple Syllabus SFTP server |

---

### `Enroll_GPS_Students_In_Grad_Hub.py`

Auto-enrolls GPS (Graduate & Professional Studies) students in the Grad Hub community course.

| Item | Details |
|------|---------|
| **Entry function** | `enrollGPSStudentsInGrad_Hub(termCode)` |
| **Schedule** | First run of the day |
| **Actions** | Canvas enrollment API calls for eligible GPS students |

---

### `Enroll_TUG_Students_In_SGA.py`

Auto-enrolls TUG (Traditional Undergraduate) students in the SGA (Student Government Association) course.

| Item | Details |
|------|---------|
| **Entry function** | `enrollTugStudentsInSga(termCode)` |
| **Schedule** | First run of the day |
| **Actions** | Canvas enrollment API calls for eligible TUG students |

---

### `Remove_Orphaned_SIS_Items.py`

Identifies Canvas courses and enrollments that no longer exist in the SIS feed and removes them. Courses with existing scores are flagged for manual review instead of automatic deletion.

| Item | Details |
|------|---------|
| **Entry function** | `removeOrphanedSisItems()` |
| **Schedule** | Multiple times daily (part of fourTimesDaily) |
| **Actions** | Canvas API calls to delete orphaned courses/enrollments; generates manual review list |

---

### `Send_Department_Communication.py`

Sends automated department communication emails to instructors of active outcome-associated courses.

| Item | Details |
|------|---------|
| **Entry function** | `sendDepartmentCommunication(termCode, targetDesignator, communicationTitle)` |
| **Inputs** | Communication template text files, `Department_Communication_Schedule_Template.csv`, Canvas course/instructor data |
| **Actions** | Sends personalized emails via Microsoft Graph |
| **Schedule template** | Uses week-based scheduling (e.g., "MIDTERM_MINUS_1", "FINAL_MINUS_1") |

---

### `CX_Data_Sync.py`

Synchronizes data with the CX platform for cross-system data consistency.

---

## Manual/On-Demand Modules (CSV-Driven Bulk Operations)

These scripts read a target CSV file and perform bulk operations. They are typically run manually for administrative tasks:

### `Change_Account_For_Listed_Courses.py`

| Input CSV | Required Columns | Action |
|-----------|-----------------|--------|
| Listed in Canvas resource path | `course_id`, `account_id` | Moves courses to specified sub-accounts |

### `Change_Grading_Scheme_For_Listed_Courses.py`

| Input CSV | Required Columns | Action |
|-----------|-----------------|--------|
| Listed in Canvas resource path | `course_id`, `grading_standard_id` | Updates the grading scheme for listed courses |

### `Change_Long_Name_For_Listed_Courses.py`

| Input CSV | Required Columns | Action |
|-----------|-----------------|--------|
| Listed in Canvas resource path | `course_id`, `long_name` | Updates course display names |

### `Change_Role_For_Listed_Enrollments.py`

| Input CSV | Required Columns | Action |
|-----------|-----------------|--------|
| Listed in Canvas resource path | `enrollment_id`, `role` | Changes enrollment roles |

### `Change_Term_For_Listed_Courses.py`

| Input CSV | Required Columns | Action |
|-----------|-----------------|--------|
| Listed in Canvas resource path | `course_id`, `term_id` | Reassigns courses to different terms |

### `Delete_Listed_Courses.py`

| Input CSV | Required Columns | Action |
|-----------|-----------------|--------|
| Listed in Canvas resource path | `course_id` | Permanently deletes listed courses |

### `Delete_Listed_Users.py`

| Input CSV | Required Columns | Action |
|-----------|-----------------|--------|
| `Target_Canvas_User_Ids.csv` in Canvas resource path | `canvas_user_id` | Deletes listed Canvas user accounts |

### `Require_Password_Reset_For_Listed_Users.py`

| Input CSV | Required Columns | Action |
|-----------|-----------------|--------|
| `Target_Canvas_User_Ids.csv` in Canvas resource path | `canvas_user_id` | Forces password reset and terminates all active sessions |

### `Update_Grading_Standard_For_Listed_Courses.py`

> ⚠️ **DEPRECATED** — This script delegates to `Change_Grading_Scheme_For_Listed_Courses.py`. Use that script directly.

---

## Utility Modules

### `Collect_Logs.py`

Walks all script log directories, collects and merges log entries, and outputs a filtered/sorted consolidated log view.

| Feature | Details |
|---------|---------|
| **CLI arguments** | Start date, end date, minimum log level (info/warning/error) |
| **Output** | Sorted, deduplicated log entries from all scripts |

### `Count_Respondus_Quizzes_and_Users.py`

Counts Respondus LockDown Browser quiz usage and unique student submissions for license reporting.

### `Turn_Off_Disallow_Threaded_Replies_In_Discussions.py`

Fixes discussion threading settings across courses where `allow_threaded_replies` was incorrectly disabled.

### `Combine_Scripts.py`

Development utility that combines multiple script files into a single file for review or deployment.

### `Comment Out Error Handling.py` / `Uncomment Out Error Handling.py`

Development utilities that toggle error handling try/except blocks across scripts for debugging purposes.

### `Test.py`

Testing sandbox for experimenting with action module functionality.

---

## Supporting Files

| File | Purpose |
|------|---------|
| `Department_Communication_Schedule_Template.csv` | Template defining when department communications are sent (by week relative to term milestones) |
| `__init__.py` | Package initialization |

---

## Common Pattern

All action modules follow this initialization pattern:

```python
import os, sys
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

from Local_Setup import LocalSetup
from Error_Email import errorEmail

scriptName = os.path.basename(__file__).replace(".py", "")
scriptPurpose = r"""..."""
externalRequirements = r"""..."""

localSetup = LocalSetup(datetime.now(), __file__)
errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)
```

---

## Dependencies

All action modules depend on `ResourceModules` for:

- `LocalSetup` — paths, logging, term logic
- `Error_Email` — automated error notifications
- `TLC_Common` — API calls, file utilities, threading helpers
- `TLC_Action` — SFTP uploads, CSV helpers, outcome operations
- `Canvas_Report` — Canvas provisioning data retrieval
- `Core_Microsoft_Api` — Email sending for communications

---

## Adding a New Action Module

1. Create a new `.py` file in this directory
2. Follow the initialization pattern above (set `sys.path`, LocalSetup + errorEmail)
3. Wrap main logic in a function with try/except calling `errorHandler.sendError()`
4. For scheduled actions: import and call from `IDT_Canvas_Primary.py`
5. For manual/bulk operations: accept a target CSV from the Canvas resource path
6. Use `makeApiCall()` for all Canvas API interactions (handles rate limiting and pagination)
7. Use `runThreadedRows()` for parallel processing of DataFrame rows
