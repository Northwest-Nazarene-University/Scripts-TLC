# ActionModules

Write-focused scripts that perform operational updates in Canvas and external systems.

## Purpose

`ActionModules` contains task-specific automations for:

- course and enrollment updates
- term and settings adjustments
- integration uploads
- cleanup and maintenance actions

## Modules

- `Add_Outcomes_to_Active_Courses.py` - attaches outcomes to active courses
- `CX_Data_Sync.py` - sync operations for CX-related data
- `Change_Account_For_Listed_Courses.py` - bulk course account updates
- `Change_Grading_Scheme_For_Listed_Courses.py` - bulk grading scheme changes
- `Change_Long_Name_For_Listed_Courses.py` - bulk long-name updates
- `Change_Role_For_Listed_Enrollments.py` - bulk enrollment role changes
- `Change_Syllabus_Tab.py` - syllabus tab setting updates
- `Change_Term_For_Listed_Courses.py` - bulk term reassignment
- `Collect_Logs.py` - operational log collection utility
- `Combine_Scripts.py` - utility for combining scripts
- `Comment Out Error Handling.py` - development utility to comment error handling
- `Count_Respondus_Quizzes_and_Users.py` - Respondus usage counting
- `Course_Date_Related_Actions.py` - date-based course lifecycle actions
- `Delete_Listed_Courses.py` - bulk course deletion
- `Enroll_GPS_Students_In_Grad_Hub.py` - Grad Hub auto-enrollment
- `Enroll_TUG_Students_In_SGA.py` - SGA auto-enrollment
- `Remove_Orphaned_SIS_Items.py` - removes orphaned SIS-linked items
- `Send_Catalog_To_Simple_Syllabus.py` - catalog export to Simple Syllabus
- `Send_Course_Editors_To_Simple_Syllabus.py` - editor export to Simple Syllabus
- `Send_Department_Communication.py.py` - department communication automation
- `Test.py` - action module testing sandbox
- `Turn_Off_Disallow_Threaded_Replies_In_Discussions.py` - discussion setting cleanup
- `Uncomment Out Error Handling.py` - development utility to restore error handling
- `Update_Grading_Standard_For_Listed_Courses.py` - legacy grading standard updater

## Usage Notes

- These scripts are invoked by `IDT_Canvas_Primary.py` or manually for targeted operations.
- Most modules rely on shared setup and helper logic from `ResourceModules`.
