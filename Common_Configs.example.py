## Example configuration template for Scripts_TLC deployments.
## Copy this file to Configs/Common_Configs.py and replace placeholder values.

## Core Canvas settings
coreCanvasApiUrl = "https://yourinstitution.instructure.com/api/v1"
canvasAccessToken = "REPLACE_WITH_CANVAS_ACCESS_TOKEN"

## Script library marker used by Local_Setup.py
scriptLibrary = "TLC"

## Service account used for automated email notifications
serviceEmailAccount = "service-account@yourinstitution.edu"

## External resource root paths used throughout the automation scripts
externalResourcePathsDict = {
    "SIS": r"\\server\share\SIS\",
    "TLC": r"\\server\share\TLC\",
    "Slate": r"\\server\share\Slate\",
    "Microsoft": r"\\server\share\Microsoft\",
    "SimpleSyllabus": r"\\server\share\SimpleSyllabus\",
}

## Undergraduate term code mappings
undgTermsWordsToCodesDict = {
    "Fall": "FA",
    "Spring": "SP",
    "Summer": "SU",
}

undgTermsCodesToWordsDict = {
    "FA": "Fall",
    "SP": "Spring",
    "SU": "Summer",
}

## Graduate term code mappings
gradTermsWordsToCodesDict = {
    "Fall": "GF",
    "Spring": "GS",
    "Summer": "SG",
}

gradTermsCodesToWordsDict = {
    "GF": "Fall",
    "GS": "Spring",
    "SG": "Summer",
}

## Month ranges used for determining current term
termMonthRanges = {
    "Fall": (9, 12),
    "Spring": (1, 5),
    "Summer": (6, 8),
}

## Logic used to determine school-year span from term
termSchoolYearLogic = {
    "Fall": "current-next",
    "Spring": "previous-current",
    "Summer": "previous-current",
}

## Simple Syllabus integration settings
catalogToSimpleSyllabusConfig = {
    "sftp": {
        "host": "sftp.simplesyllabus.com",
        "port": 22,
        "username": "REPLACE_WITH_SFTP_USERNAME",
        "remote_dir": "/imports",
    },
    "catalog": {
        "catalog_csv_path": r"\\server\share\catalog\catalog.csv",
        "course_code_column": "course_code_norm",
        "title_column": "Title",
    },
}
