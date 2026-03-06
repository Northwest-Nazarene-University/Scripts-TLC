import os
import sys
from datetime import datetime

# Make ResourceModules importable when running from ActionModules
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

from Local_Setup import LocalSetup
from TLC_Common import downloadFile   # <-- uses your existing helper

# Initialize LocalSetup (for logging/paths if you want them)
localSetup = LocalSetup(datetime.now(), __file__)

def main():
    # URL you provided
    download_url = (
    )

    # Where to save it – adjust as you like
    # Example: put it in the SIS external resource path if configured,
    # otherwise fall back to current working directory.
    try:
        base_path = localSetup.getExternalResourcePath("SIS")
    except Exception:
        base_path = os.getcwd()

    os.makedirs(base_path, exist_ok=True)
    output_path = os.path.join(r'C:\Users\brycezmiller\desktop', "gps-course-report.csv")

    # Use your download function
    # Third argument is the file mode; "w" is consistent with your other usage
    downloadFile(localSetup, download_url, output_path, "w")

    localSetup.logger.info(f"Downloaded GPS course report to: {output_path}")
    print(f"Downloaded GPS course report to: {output_path}")

if __name__ == "__main__":
    main()