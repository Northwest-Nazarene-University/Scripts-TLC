# Configs

This folder stores local configuration and secrets used by the Scripts-TLC automation suite.

Important:
- Keep real config files local only.
- Do not commit tokens, passwords, private keys, or credential files.
- This repository tracks only this README in `Configs/`.

## Why this README is inferential

The real config files are intentionally excluded from version control.  
This document is based on how the code references `localSetup.configPath`, `Common_Configs`, and related file names.

## Primary file: `Common_Configs.py`

Most modules import shared settings from `Common_Configs.py`. The following names are referenced in code:

- `scriptLibrary`
- `externalResourcePathsDict`
- `coreCanvasApiUrl`
- `canvasAccessToken`
- `serviceEmailAccount`
- `authorContactInformation`
- `undgTermsWordsToCodesDict`
- `undgTermsCodesToWordsDict`
- `gradTermsWordsToCodesDict`
- `gradTermsCodesToWordsDict`
- `termMonthRanges`
- `termSchoolYearLogic`
- `catalogToSimpleSyllabusConfig`

### Expected shape notes

- `externalResourcePathsDict` is used for external roots like `"SIS"`, `"IE"`, and `"TLC"`.
- `catalogToSimpleSyllabusConfig` is expected to include:
  - `catalogProduction` (catalog source links/settings)
  - `sftp` with keys such as `host`, `port`, `username`, and optional `remote_dir`.

## Other config files referenced by path

The following file names are read directly from the config path by one or more modules:

- `External_Resource_Paths.json`
- `Canvas_Access_Token.txt`
- `Core_Canvas_Url.txt`
- `.env` (must define `ENCRYPTION_KEY`)
- `Slate_Creds.json`
- `Slate_Public_Key.txt`
- `SimpSylSSH.txt`
- `SSPrivKP.txt` (one-time plaintext passphrase input; encrypted then removed)
- `SSPrivKP_Encrypted.txt` (encrypted Simple Syllabus key passphrase)
- `Simple Syllabus Organizations.csv`
- `Outlook_API_Config.cfg`
- `OneDrive_and_Sharepoint_API_Config.cfg`

## Course editor input files discovered in config path

`Send_Course_Editors_To_Simple_Syllabus.py` scans this config directory for:

- `NNU_Course_Editor_File*.csv`
- `Course Editor Input.csv`

These files are optional and can be supplemented by files in catalog year folders.

## Minimal local setup checklist

1. Create `Configs/Common_Configs.py` with the variables listed above.
2. Provide Canvas and resource path config used by your run mode.
3. Add `.env` with a valid `ENCRYPTION_KEY`.
4. Add integration files only for features you run (Slate, Simple Syllabus, Microsoft Graph).
5. Confirm no secrets are staged before commit.
