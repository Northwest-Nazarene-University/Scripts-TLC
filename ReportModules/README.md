# ReportModules

Read-focused scripts that gather, process, and export institutional reporting data.

## Purpose

`ReportModules` generates recurring reports from Canvas and related systems, including:

- outcomes reporting
- syllabi compliance reporting
- enrollment and instructor status reporting
- incoming student and retention-related reporting

## Modules

- `Inactive_Enrollments_Report.py` - identifies inactive enrollments
- `Incoming_Student_Report.py` - combines Canvas and Slate data for incoming students
- `Nighthawk_360_Canvas_Report.py` - prepares data exports for Nighthawk 360
- `Non_Official_Instructors_Report.py` - identifies non-official instructor enrollments
- `Outcome_Attachment_Report.py` - checks outcome attachment coverage
- `Outcome_Results_Report.py` - processes outcome results for reporting
- `Syllabi_Report.py` - retrieves and catalogs syllabi information
- `Syllabus_Addendum_Report.py` - checks syllabus addendum compliance

## Usage Notes

- These modules are orchestrated by `IDT_Canvas_Primary.py`.
- Report scripts depend on shared services in `ResourceModules`.
