# Scripts-TLC

**Northwest Nazarene University — Teaching & Learning Center (TLC) Automation Scripts**

A Python-based automation suite that manages Canvas LMS operations, data reporting, and integrations for Northwest Nazarene University's Instructional Design & Technology team. Orchestrated by a primary scheduling script, these modules run on a recurring schedule to synchronize SIS data, generate institutional reports, manage course configurations, and integrate with third-party platforms like Simple Syllabus, Slate, and Microsoft services.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Configuration](#configuration)
  - [Running the Primary Script](#running-the-primary-script)
- [Modules](#modules)
  - [Entry Point](#entry-point)
  - [Resource Modules](#resource-modules)
  - [Report Modules](#report-modules)
  - [Action Modules](#action-modules)
- [Module README Files](#module-readme-files)
- [Scheduling & Execution Flow](#scheduling--execution-flow)
- [Author](#author)

---

## Overview

The Scripts-TLC repository contains the automation scripts used by the NNU Teaching & Learning Center to:

- **Synchronize SIS data** with Canvas LMS (terms, users, courses, enrollments)
- **Generate institutional reports** including outcome results, syllabi compliance, enrollment activity, and incoming student data
- **Perform automated Canvas actions** such as managing outcomes, enrollment roles, grading schemes, course settings, and orphaned SIS records
- **Integrate with third-party services** including Simple Syllabus (catalog/course editor uploads), Slate (incoming student SFTP retrieval), and Microsoft Graph API
- **Deliver Nighthawk 360 data** for student success and retention tracking

---

## Architecture

The project follows a modular architecture organized into four main layers:

```
IDT_Canvas_Primary.py        ← Orchestrator / scheduler (entry point)
    │
    ├── ResourceModules/      ← Shared libraries, API clients, and utilities
    ├── ReportModules/        ← Data retrieval and report generation
    ├── ActionModules/        ← Canvas write operations and integrations
    └── Configs/              ← Configuration files (excluded from repo)
```

All modules rely on a common initialization pattern using `LocalSetup` for path management, logging, and date/time context, and `Error_Email` for automated error notification.

---

## Project Structure

```
Scripts-TLC/
├── IDT_Canvas_Primary.py                 # Main orchestrator script
├── __init__.py                           # Package init
├── pyrightconfig.json                    # Pyright type-checking config
│
├── ResourceModules/                      # Shared utilities and API clients
│   ├── README.md                         #   Resource module documentation
│   ├── Local_Setup.py                    #   Environment setup, paths, logging, date context
│   ├── TLC_Common.py                     #   Common helpers (API calls, file checks, encryption)
│   ├── TLC_Action.py                     #   Shared action utilities (CSV ops, SFTP uploads, course logic)
│   ├── Canvas_Report.py                  #   Canvas Reports API client (CanvasReport class)
│   ├── Core_Microsoft_Api.py             #   Microsoft Graph API client
│   ├── Error_Email.py                    #   Automated error email notifications
│   ├── Get_Slate_Info.py                 #   Slate SFTP integration for incoming student data
│   └── __init__.py
│
├── ReportModules/                        # Report generation scripts
│   ├── README.md                         #   Report module documentation
│   ├── Outcome_Results_Report.py         #   GE/program outcome results processing
│   ├── Outcome_Attachment_Report.py      #   Outcome attachment verification
│   ├── Syllabi_Report.py                 #   University syllabi retrieval and compliance
│   ├── Syllabus_Addendum_Report.py       #   Syllabus addendum verification
│   ├── Nighthawk_360_Canvas_Report.py    #   Student grade & activity data for Nighthawk 360
│   ├── Incoming_Student_Report.py        #   Incoming student data (Canvas + Slate)
│   ├── Non_Official_Instructors_Report.py#   Non-instructor-of-record enrollment report
│   ├── Inactive_Enrollments_Report.py    #   Inactive enrollment identification
│   └── __init__.py
│
├── ActionModules/                        # Canvas write operations and integrations
│   ├── README.md                         #   Action module documentation
│   ├── Course_Date_Related_Actions.py    #   Term-based course lifecycle actions
│   ├── Add_Outcomes_to_Active_Courses.py #   Attach outcomes to courses
│   ├── Change_Syllabus_Tab.py            #   Update course syllabus tab settings
│   ├── Send_Catalog_To_Simple_Syllabus.py#   CleanCatalog → Simple Syllabus SFTP upload
│   ├── Send_Course_Editors_To_Simple_Syllabus.py  # Course editor data → Simple Syllabus
│   ├── Enroll_GPS_Students_In_Grad_Hub.py#   Auto-enroll GPS students in Grad Hub
│   ├── Enroll_TUG_Students_In_SGA.py     #   Auto-enroll TUG students in SGA
│   ├── Remove_Orphaned_SIS_Items.py      #   Clean up orphaned SIS courses/enrollments
│   ├── CX_Data_Sync.py                   #   CX platform data synchronization
│   ├── Change_Account_For_Listed_Courses.py      # Bulk course account changes
│   ├── Change_Grading_Scheme_For_Listed_Courses.py # Bulk grading scheme updates
│   ├── Change_Long_Name_For_Listed_Courses.py    # Bulk course name updates
│   ├── Change_Role_For_Listed_Enrollments.py     # Bulk enrollment role changes
│   ├── Change_Term_For_Listed_Courses.py         # Bulk course term changes
│   ├── Update_Grading_Standard_For_Listed_Courses.py # DEPRECATED — delegates to Change_Grading_Scheme_For_Listed_Courses.py
│   ├── Delete_Listed_Courses.py          #   Bulk course deletion
│   ├── Count_Respondus_Quizzes_and_Users.py      # Respondus LockDown Browser usage report
│   ├── Turn_Off_Disallow_Threaded_Replies_In_Discussions.py # Discussion settings fix
│   ├── Combine_Scripts.py                #   Utility to combine scripts into single file
│   ├── Comment Out Error Handling.py     #   Dev utility: comment out error handling
│   ├── Uncomment Out Error Handling.py   #   Dev utility: restore error handling
│   ├── Test.py                           #   Testing sandbox
│   └── __init__.py
│
└── Configs/                              # Configuration files (not tracked in repo)
    └── Common_Configs.py                 #   API URLs, tokens, term mappings, paths
```

---

## Getting Started

### Prerequisites

- **Python 3.8+**
- The following Python packages (install via `pip`):

  | Package         | Purpose                                       |
  |-----------------|-----------------------------------------------|
  | `pandas`        | Data manipulation and CSV processing           |
  | `numpy`         | Numerical operations                           |
  | `requests`      | HTTP requests to Canvas and other APIs         |
  | `paramiko`      | SFTP connections (Slate, Simple Syllabus)       |
  | `python-dotenv` | Environment variable management                |
  | `python-dateutil` | Date parsing utilities                       |
  | `beautifulsoup4`| HTML parsing (catalog processing)              |
  | `pdfkit`        | PDF generation for reports                     |
  | `cryptography`  | Fernet encryption for credential management    |
  | `openpyxl`      | Excel file read/write support                  |
  | `msal`          | Microsoft Authentication Library (Graph API)   |

### Configuration

The repository expects a `Configs/` directory at the project root containing:

- **`Common_Configs.py`** — Central configuration file that defines:
  - `coreCanvasApiUrl` — Base URL for the Canvas API instance
  - `canvasAccessToken` — Canvas API bearer token
  - `scriptLibrary` — Root path to the script library on disk
  - `externalResourcePathsDict` — Paths to SIS feeds, shared drives, and output folders
  - `serviceEmailAccount` — Email account used for error notifications
  - Term mapping dictionaries (`undgTermsCodesToWordsDict`, `gradTermsCodesToWordsDict`, etc.)
  - `termMonthRanges` / `termSchoolYearLogic` — Term date logic
  - `catalogToSimpleSyllabusConfig` — Simple Syllabus SFTP and catalog settings
  - Additional integration-specific configurations

- **`External_Resource_Paths.json`** — File paths to external shared drive resources

> ⚠️ **Note:** The `Configs/` directory contains sensitive credentials and is excluded from version control via `.gitignore`.

### Running the Primary Script

```bash
python IDT_Canvas_Primary.py
```

The primary script automatically determines the current term, time of day, and day of the week to decide which sub-processes to execute.

---

## Modules

### Entry Point

#### `IDT_Canvas_Primary.py`

The main orchestrator that schedules and runs all IDT Canvas-related automation. It determines target terms using date logic, then dispatches work across multiple execution paths based on the time of day and day of the week:

- **First run of the day** — Full Canvas data sync (terms, users, courses, enrollments), outcome reports, syllabi processing, and Simple Syllabus integrations
- **Multiple times daily** — Partial data syncs (courses and enrollments) and course date-related actions
- **Last run of the day** — Nighthawk 360 data retrieval

### Resource Modules

Shared libraries consumed by both Report and Action modules:

| Module | Description |
|--------|-------------|
| **`Local_Setup.py`** | `LocalSetup` class — initializes working directories, logging, date/time context, resource paths, and Canvas resource folder structure |
| **`TLC_Common.py`** | Common utility functions — `makeApiCall()` (paginated Canvas API helper), `isFileRecent()`, `downloadFile()`, `flattenApiObjectToJsonList()`, encryption key management |
| **`TLC_Action.py`** | Higher-level action utilities — CSV reading with encoding detection, Simple Syllabus SFTP uploads, course week determination, outcome attachment, and data retrieval orchestration |
| **`Canvas_Report.py`** | `CanvasReport` class — full lifecycle management of Canvas provisioning reports (request, poll status, download, load to DataFrame). Supports accounts, terms, users, courses, enrollments, sections, outcomes, and more |
| **`Core_Microsoft_Api.py`** | Microsoft Graph API client — authentication via MSAL, email sending, SharePoint/OneDrive file operations |
| **`Error_Email.py`** | `errorEmail` class — captures exceptions and sends formatted error notification emails with script context |
| **`Get_Slate_Info.py`** | Connects to NNU's Slate SFTP server to retrieve incoming student CSVs |

### Report Modules

Data retrieval and report generation scripts:

| Module | Description |
|--------|-------------|
| **`Outcome_Results_Report.py`** | Processes GE and program-level outcome results from Canvas for assessment reporting |
| **`Outcome_Attachment_Report.py`** | Verifies that required outcomes are attached to active courses |
| **`Syllabi_Report.py`** | Retrieves and catalogs university syllabi from Canvas courses |
| **`Syllabus_Addendum_Report.py`** | Checks courses for required syllabus addendum compliance |
| **`Nighthawk_360_Canvas_Report.py`** | Generates student-level grade and activity data for the Nighthawk 360 retention platform |
| **`Incoming_Student_Report.py`** | Combines Canvas and Slate data to produce incoming student reports |
| **`Non_Official_Instructors_Report.py`** | Identifies non-instructor-of-record enrollments per course |
| **`Inactive_Enrollments_Report.py`** | Reports on inactive student enrollments |

### Action Modules

Scripts that perform write operations against Canvas and external systems:

| Module | Description |
|--------|-------------|
| **`Course_Date_Related_Actions.py`** | Term lifecycle automation — performs actions based on course start/end dates |
| **`Add_Outcomes_to_Active_Courses.py`** | Attaches required learning outcomes to active courses |
| **`Change_Syllabus_Tab.py`** | Updates the syllabus tab visibility/settings across courses |
| **`Send_Catalog_To_Simple_Syllabus.py`** | Retrieves TUG/GPS catalog data from CleanCatalog, formats it, and uploads to Simple Syllabus via SFTP |
| **`Send_Course_Editors_To_Simple_Syllabus.py`** | Sends course editor/instructor data to Simple Syllabus |
| **`Enroll_GPS_Students_In_Grad_Hub.py`** | Auto-enrolls GPS (Graduate & Professional Studies) students in the Grad Hub course |
| **`Enroll_TUG_Students_In_SGA.py`** | Auto-enrolls TUG (Traditional Undergraduate) students in the SGA course |
| **`Remove_Orphaned_SIS_Items.py`** | Identifies and removes Canvas courses/enrollments no longer in the SIS feed; flags scored courses for manual review |
| **`CX_Data_Sync.py`** | Synchronizes data with the CX platform |
| **Bulk Operation Scripts** | `Change_Account_For_Listed_Courses.py`, `Change_Grading_Scheme_For_Listed_Courses.py`, `Change_Long_Name_For_Listed_Courses.py`, `Change_Role_For_Listed_Enrollments.py`, `Change_Term_For_Listed_Courses.py`, `Delete_Listed_Courses.py` — CSV-driven bulk Canvas operations. (`Update_Grading_Standard_For_Listed_Courses.py` is deprecated; use `Change_Grading_Scheme_For_Listed_Courses.py` instead.) |
| **`Count_Respondus_Quizzes_and_Users.py`** | Counts Respondus LockDown Browser quiz usage and unique student submissions |
| **`Turn_Off_Disallow_Threaded_Replies_In_Discussions.py`** | Fixes discussion threading settings across courses |

## Module README Files

Each module directory has its own README for focused documentation:

- `ResourceModules/README.md` - shared utilities, API clients, and setup helpers
- `ReportModules/README.md` - report generation modules and outputs
- `ActionModules/README.md` - write-action modules and operational scripts

---

## Scheduling & Execution Flow

`IDT_Canvas_Primary.py` uses time-of-day and day-of-week logic to determine what runs on each invocation. It is designed to be triggered by an external task scheduler (e.g., Windows Task Scheduler) multiple times per day.

```
┌─────────────────────────────────────────────────────────┐
│                   IDT_Canvas_Primary.py                 │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  1st Run of Day (e.g., early morning)                   │
│  ├── Create complete Canvas inputs (SIS CSV sync)       │
│  ├── Run outcome reports & actions                      │
│  ├── Process syllabi reports                            │
│  ├── Send catalog to Simple Syllabus                    │
│  ├── Send course editors to Simple Syllabus             │
│  └── Additional first-run-only processes                │
│                                                         │
│  2nd & 3rd Runs (midday)                                │
│  ├── Create partial Canvas inputs (courses/enrollments) │
│  └── Run four-times-daily processes                     │
│                                                         │
│  Last Run of Day                                        │
│  ├── Create partial Canvas inputs                       │
│  ├── Run four-times-daily processes                     │
│  └── Fetch Nighthawk 360 data                           │
│                                                         │
│  Monthly/Weekly triggers                                │
│  ├── Course date-related lifecycle actions               │
│  └── Orphaned SIS item cleanup                          │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

---

## Author

**Bryce Miller** — [brycezmiller@nnu.edu](mailto:brycezmiller@nnu.edu)  
Teaching and Learning Center, Northwest Nazarene University
