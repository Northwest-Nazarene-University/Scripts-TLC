# Copilot Instructions

## Project Guidelines
- Catalog DataFrame uses 'Title' (capital T) and 'course_code_norm' instead of 'title' and 'course_code'; requiredColumns validation should use actual column names present in the catalog (e.g., 'Title', 'course_code_norm', and the CSV likely lacks 'account_id').
- Canvas course_id format is "FA2026_ACCT2065_01" (term code uses 4-digit year, not 2-digit). The pattern is "{TermPrefix}{FullYear}_{CourseCode}_{Section}".
- For Canvas provisioning reports in this project, use the actual SIS term code value (e.g., GS26) consistently, including in filenames and payload term filters.
- When making claims about solution-wide usage, verify against the user's provided solution-context matches before concluding.

## Code Editing Guidelines
- Use encoding-safe editing rules: keep Python source files and comments ASCII-safe and avoid extended Unicode characters (like box-drawing) to prevent cp1252 UnicodeDecodeError in local tooling.
- New or updated scripts/functions must include solution-style documentation: provide a script purpose/requirements, function docstrings with Args/Returns, and step-based inline comments.
- Use p#_ parameter prefixes to indicate the level from where a parameter was first defined, not argument position order in the local function signature.
