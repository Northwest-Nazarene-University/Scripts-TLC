# Configs

This folder stores local configuration and secrets used by the Scripts-TLC automation suite.

> ⚠️ **Security Notice:** Real config files contain sensitive credentials and are excluded from version control via `.gitignore`. Only this README is tracked in the repository.

---

## Quick Start Setup

To get the automation suite running locally, follow these steps:

### Step 1: Create `Common_Configs.py`

This is the primary configuration file imported by nearly every module. Create it at `Configs/Common_Configs.py` with the following variables:

```python
# ─── Core Canvas Configuration ───────────────────────────────────────────────
scriptLibrary = "TLC"                          # Library identifier (used in path construction)
coreCanvasApiUrl = "https://your-institution.instructure.com/api/v1"
canvasAccessToken = "your-canvas-api-token"     # Canvas API token with admin permissions

# ─── Service & Contact Info ───────────────────────────────────────────────────
serviceEmailAccount = "service-account@institution.edu"   # Used for error emails
authorContactInformation = "Contact: Your Name - email@institution.edu"

# ─── File Paths ───────────────────────────────────────────────────────────────
# Root path to the script library on disk (parent of Scripts_TLC folder)
# scriptLibrary defined above as "TLC"

# Paths to external shared drive resources
# Keys used: "SIS", "IE", "TLC"
externalResourcePathsDict = {
    "SIS": r"\\server\share\SIS_Feeds\\",
    "IE":  r"\\server\share\Institutional_Effectiveness\\",
    "TLC": r"\\server\share\TLC\\"
}

# ─── Term Mapping Dictionaries ────────────────────────────────────────────────
# Map term prefix codes to/from human-readable words
# Undergraduate terms
undgTermsWordsToCodesDict = {"Fall": "FA", "Spring": "SP", "Summer": "SU"}
undgTermsCodesToWordsDict = {"FA": "Fall", "SP": "Spring", "SU": "Summer"}

# Graduate terms
gradTermsWordsToCodesDict = {"Grad Fall": "GF", "Grad Spring": "GS", "Summer Grad": "SG"}
gradTermsCodesToWordsDict = {"GF": "Grad Fall", "GS": "Grad Spring", "SG": "Summer Grad"}

# ─── Term Date Logic ──────────────────────────────────────────────────────────
# Maps months to active term prefixes
termMonthRanges = {
    1: ["SP", "GS"], 2: ["SP", "GS"], 3: ["SP", "GS"], 4: ["SP", "GS"], 5: ["SP", "GS"],
    6: ["SU", "SG"], 7: ["SU", "SG"],
    8: ["FA", "GF"], 9: ["FA", "GF"], 10: ["FA", "GF"], 11: ["FA", "GF"], 12: ["FA", "GF"]
}

# Maps terms to their school year membership
termSchoolYearLogic = {
    "Fall": {"year_offset": 0, "next": "Spring"},
    "Spring": {"year_offset": 1, "next": "Summer"},
    # ... etc.
}

# ─── Simple Syllabus Integration ─────────────────────────────────────────────
catalogToSimpleSyllabusConfig = {
    "catalogProduction": {
        "tug_url": "https://catalog.institution.edu/tug",
        "gps_url": "https://catalog.institution.edu/gps"
    },
    "sftp": {
        "host": "sftp.simplesyllabus.com",
        "port": 22,
        "username": "your-username",
        "remote_dir": "/incoming/"
    }
}
```

### Step 2: Create the `.env` File

Create `Configs/.env` with your encryption key:

```env
ENCRYPTION_KEY=your-fernet-encryption-key-here
```

Generate a Fernet key with:
```python
from cryptography.fernet import Fernet
print(Fernet.generate_key().decode())
```

### Step 3: Create Canvas Credential Files

| File | Contents |
|------|----------|
| `Canvas_Access_Token.txt` | Your Canvas API bearer token (plain text) |
| `Core_Canvas_Url.txt` | Your Canvas instance base URL |

### Step 4: Set Up Integration Files (as needed)

Only create these if you use the corresponding features:

#### Slate Integration
| File | Contents |
|------|----------|
| `Slate_Creds.json` | `{"host": "...", "port": 22, "username": "...", "password": "..."}` |
| `Slate_Public_Key.txt` | Slate SFTP server public key for host verification |

#### Simple Syllabus Integration
| File | Contents |
|------|----------|
| `SimpSylSSH.txt` | Path to the SSH private key file for Simple Syllabus SFTP |
| `SSPrivKP.txt` | Private key passphrase (plain text — encrypted on first use, then deleted) |
| `SSPrivKP_Encrypted.txt` | Encrypted passphrase (auto-generated from `SSPrivKP.txt`) |
| `Simple Syllabus Organizations.csv` | Organization mapping data for Simple Syllabus |

#### Microsoft Graph API
| File | Contents |
|------|----------|
| `Outlook_API_Config.cfg` | INI-format config with `[azure]` section containing `clientId`, `tenantId`, `clientSecret` |
| `OneDrive_and_Sharepoint_API_Config.cfg` | INI-format config for SharePoint/OneDrive access |

---

## All Configuration Files Reference

### Primary file: `Common_Configs.py`

Most modules import shared settings from `Common_Configs.py`. The following names are referenced in code:

| Variable | Type | Used By |
|----------|------|---------|
| `scriptLibrary` | `str` | Local_Setup (path construction) |
| `externalResourcePathsDict` | `dict` | Local_Setup (external paths) |
| `coreCanvasApiUrl` | `str` | Api_Caller, Canvas_Report, all API calls |
| `canvasAccessToken` | `str` | Api_Caller, Canvas_Report |
| `serviceEmailAccount` | `str` | Error_Email, Core_Microsoft_Api |
| `authorContactInformation` | `str` | Error_Email (email body) |
| `undgTermsWordsToCodesDict` | `dict` | Local_Setup (term logic) |
| `undgTermsCodesToWordsDict` | `dict` | Local_Setup, Primary script |
| `gradTermsWordsToCodesDict` | `dict` | Local_Setup (term logic) |
| `gradTermsCodesToWordsDict` | `dict` | Local_Setup, Get_Slate_Info |
| `termMonthRanges` | `dict` | Local_Setup (current term detection) |
| `termSchoolYearLogic` | `dict` | Local_Setup (school year logic) |
| `catalogToSimpleSyllabusConfig` | `dict` | TLC_Action, Send_Catalog_To_Simple_Syllabus |

### Expected shape notes

- `externalResourcePathsDict` is used for external roots like `"SIS"`, `"IE"`, and `"TLC"`.
- `catalogToSimpleSyllabusConfig` is expected to include:
  - `catalogProduction` (catalog source links/settings)
  - `sftp` with keys such as `host`, `port`, `username`, and optional `remote_dir`.

### Other config files referenced by path

The following file names are read directly from the config path by one or more modules:

| File | Used By | Purpose |
|------|---------|---------|
| `External_Resource_Paths.json` | Local_Setup | File paths to external shared drive resources |
| `Canvas_Access_Token.txt` | Various | Canvas API token (alternative to Common_Configs) |
| `Core_Canvas_Url.txt` | Various | Canvas base URL (alternative to Common_Configs) |
| `.env` | TLC_Common | Must define `ENCRYPTION_KEY` for Fernet encryption |
| `Slate_Creds.json` | Get_Slate_Info | Slate SFTP connection credentials |
| `Slate_Public_Key.txt` | Get_Slate_Info | Slate SFTP host key verification |
| `SimpSylSSH.txt` | TLC_Action | Path to Simple Syllabus SSH private key |
| `SSPrivKP.txt` | TLC_Action | One-time plaintext passphrase (encrypted then removed) |
| `SSPrivKP_Encrypted.txt` | TLC_Action | Encrypted Simple Syllabus key passphrase |
| `Simple Syllabus Organizations.csv` | Send_Course_Editors | Organization mapping for Simple Syllabus |
| `Outlook_API_Config.cfg` | Core_Microsoft_Api | Microsoft Graph email config |
| `OneDrive_and_Sharepoint_API_Config.cfg` | Core_Microsoft_Api | SharePoint/OneDrive config |

### Course editor input files discovered in config path

`Send_Course_Editors_To_Simple_Syllabus.py` scans this config directory for:

- `NNU_Course_Editor_File*.csv`
- `Course Editor Input.csv`

These files are optional and can be supplemented by files in catalog year folders.

---

## Directory Structure

```
Configs/
├── README.md                              ← This file (tracked in git)
├── Common_Configs.py                      ← Primary config (NOT tracked)
├── .env                                   ← Encryption key (NOT tracked)
├── Canvas_Access_Token.txt                ← Canvas token (NOT tracked)
├── Core_Canvas_Url.txt                    ← Canvas URL (NOT tracked)
├── External_Resource_Paths.json           ← Resource paths (NOT tracked)
├── Slate_Creds.json                       ← Slate credentials (NOT tracked)
├── Slate_Public_Key.txt                   ← Slate host key (NOT tracked)
├── SimpSylSSH.txt                         ← SSH key path (NOT tracked)
├── SSPrivKP_Encrypted.txt                 ← Encrypted passphrase (NOT tracked)
├── Simple Syllabus Organizations.csv      ← Org mapping (NOT tracked)
├── Outlook_API_Config.cfg                 ← Email config (NOT tracked)
├── OneDrive_and_Sharepoint_API_Config.cfg ← SharePoint config (NOT tracked)
└── NNU_Course_Editor_File*.csv            ← Editor files (NOT tracked)
```

---

## Minimal Local Setup Checklist

1. ✅ Create `Configs/Common_Configs.py` with all required variables listed above
2. ✅ Add `.env` with a valid `ENCRYPTION_KEY`
3. ✅ Provide Canvas API credentials (token + URL)
4. ✅ Add integration files only for features you intend to run:
   - Slate integration → `Slate_Creds.json` + `Slate_Public_Key.txt`
   - Simple Syllabus → `SimpSylSSH.txt` + SSH private key + passphrase
   - Microsoft Graph → `Outlook_API_Config.cfg` (+ OneDrive config if needed)
5. ⚠️ **Confirm no secrets are staged before commit** — run `git status` to verify
