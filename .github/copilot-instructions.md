# Copilot Instructions

## Project Guidelines
- Catalog DataFrame uses 'Title' (capital T) and 'course_code_norm' instead of 'title' and 'course_code'; requiredColumns validation should use actual column names present in the catalog (e.g., 'Title', 'course_code_norm', and the CSV likely lacks 'account_id').
- Canvas course_id format is "FA2026_ACCT2065_01" (term code uses 4-digit year, not 2-digit). The pattern is "{TermPrefix}{FullYear}_{CourseCode}_{Section}".
- When making claims about solution-wide usage, verify against the user's provided solution-context matches before concluding.