# ResourceModules

Shared libraries used by both report and action workflows.

## Purpose

`ResourceModules` contains the reusable foundation for the automation suite:

- local setup and runtime context
- Canvas and external API clients
- shared helper functions
- centralized error email handling

## Modules

- `Api_Caller.py` - reusable API request helpers
- `Canvas_Report.py` - Canvas Reports API retrieval and dataframe loading
- `Core_Microsoft_Api.py` - Microsoft Graph authentication and operations
- `Error_Email.py` - standard error notification handling
- `Get_Slate_Info.py` - Slate SFTP retrieval helpers
- `Local_Setup.py` - runtime paths, logging, and date context setup
- `TLC_Action.py` - shared action-side helper functions
- `TLC_Common.py` - common utility helpers used across modules

## Usage Notes

- Most scripts initialize `LocalSetup` before using these modules.
- These modules are imported by `IDT_Canvas_Primary.py`, `ActionModules`, and `ReportModules`.
