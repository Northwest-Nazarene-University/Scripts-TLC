## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

import os, sys, pandas as pd
from datetime import datetime

# Make ResourceModules importable (mirrors Course_Date_Related_Actions.py)
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

try:  # Irregular try clause, do not comment out in testing
    from Local_Setup import LocalSetup
    from Error_Email import errorEmail
    from TLC_Common import makeApiCall, runThreadedRows
except ImportError:
    # Fallback to relative imports if package layout differs
    from ResourceModules.Local_Setup import LocalSetup
    from ResourceModules.Error_Email import errorEmail
    from ResourceModules.TLC_Common import makeApiCall, runThreadedRows

#
# Define the script name, purpose, and external requirements for logging and error reporting purposes
scriptName = "Count_Respondus_Quizzes"
scriptPurpose = r"""
This script counts the number of Respondus quizzes and the number of unique students who have submitted to
these quizzes in Canvas courses.
"""
externalRequirements = r"""
To function properly, this script requires:
1. Access to the Canvas API via the standard TLC makeApiCall / LocalSetup configuration.
2. A CSV file in the Canvas resources directory listing target course IDs
   (e.g. 'Target_Canvas_Course_Ids.csv' / 'courses_to_check.csv').
"""

# Initialize LocalSetup and error handler (mirrors Course_Date_Related_Actions.py)
localSetup = LocalSetup(datetime.now(), __file__)
errorHandler = errorEmail(scriptName, scriptPurpose, externalRequirements, localSetup)

## Import Config Variables
from Common_Configs import coreCanvasApiUrl

# This function counts Respondus quizzes and students for a given course
def countRespondusQuizzes(p1_courseId: str, result_dict: dict) -> None:
    functionName = "countRespondusQuizzes"
    try:
        quizzes_count = 0
        students_count = set()

        # Get the assignments for the course
        assignments_url = f"{coreCanvasApiUrl}courses/{p1_courseId}/assignments"
        courseAssignmentsParams = {
            "search_term": "Respondus",
            "include[]": "submission",
        }

        # Header is provided by makeApiCall (via LocalSetup), no explicit header needed
        response, _ = makeApiCall(
            localSetup,
            p1_apiUrl=assignments_url,
            p1_payload=courseAssignmentsParams,
            p1_apiCallType="get",
        )

        # If the response is successful
        if response.status_code == 200:
            # Convert the response to JSON
            assignments = response.json()

            # For each assignment in the course
            for assignment in assignments:
                # If the assignment name contains "Respondus"
                if "Respondus" in assignment.get("name", ""):
                    # Save the assignment ID
                    assignment_id = assignment["id"]

                    # Save the assignment URL
                    assignment_details_url = (
                        f"{coreCanvasApiUrl}courses/{p1_courseId}/assignments/{assignment_id}"
                    )

                    # Make an API call to get the assignment details
                    assignment_response, _ = makeApiCall(
                        localSetup,
                        p1_apiUrl=assignment_details_url,
                        p1_apiCallType="get",
                    )

                    # If the response is successful
                    if assignment_response.status_code == 200:
                        # Convert the response to JSON
                        assignment_details = assignment_response.json()

                        # If the assignment is published and has submitted submissions
                        if assignment_details.get("published") and assignment_details.get(
                            "has_submitted_submissions"
                        ):
                            quizzes_count += 1

                            # If we haven't loaded enrollments yet, do it once per course
                            if len(students_count) == 0:
                                # Define an API url to get the course's enrollments
                                enrollments_url = (
                                    f"{coreCanvasApiUrl}courses/{p1_courseId}/enrollments"
                                )
                                # Define a payload to get only student enrollments
                                enrollments_params = {
                                    "type[]": "StudentEnrollment",
                                }

                                # Make an API call to get the course's enrollments
                                enrollments_response, _ = makeApiCall(
                                    localSetup,
                                    p1_apiUrl=enrollments_url,
                                    p1_payload=enrollments_params,
                                    p1_apiCallType="get",
                                )

                                # If the response is successful
                                if enrollments_response.status_code == 200:
                                    # Convert the response to JSON
                                    enrollments = enrollments_response.json()

                                    # For each enrollment in the course
                                    for enrollment in enrollments:
                                        # If the enrollment is active
                                        if (
                                            enrollment.get("enrollment_state")
                                            == "active"
                                        ):
                                            # Get the user ID from the enrollment
                                            user_id = enrollment["user_id"]
                                            # Add the user ID to the students_count set
                                            students_count.add(user_id)
                                else:
                                    localSetup.logger.warning(
                                        f"Failed to get enrollments for course {p1_courseId}. "
                                        f"Status code: {enrollments_response.status_code}"
                                    )
        else:
            localSetup.logger.warning(
                f"Failed to get assignments for course with ID: {p1_courseId}. "
                f"Status code: {response.status_code}"
            )

        # Store results as (quiz_count, set_of_student_ids)
        result_dict[p1_courseId] = (quizzes_count, students_count)

    except Exception as Error:
        # Use the shared error handler from Error_Email
        errorHandler.sendError(functionName, Error)
        # Optionally, still record something in result_dict to avoid KeyError downstream
        result_dict[p1_courseId] = (0, set())


## This function reads the CSV file and counts Respondus quizzes and students for the listed courses
def countListedCoursesRespondusQuizzes() -> None:
    functionName = "countListedCoursesRespondusQuizzes"
    try:
        ## Step 1: Load the target courses CSV
        baseCanvasResourcesPath = localSetup.getInternalResourcePaths('Canvas')
        targetCoursesCsvFilePath = os.path.join(
            baseCanvasResourcesPath, "Target_Canvas_Course_Ids.csv"
        )
        rawTargetCoursesDf = pd.read_csv(targetCoursesCsvFilePath)
        targetCoursesDf = rawTargetCoursesDf[rawTargetCoursesDf["canvas_course_id"].notna()]

        ## Step 2: Run the per-course count concurrently, accumulating into a shared dict
        ## Each courseId key is unique so concurrent dict writes are safe without a lock
        result_dict = {}
        def _worker(row):
            courseId = str(row["canvas_course_id"]).replace(".0", "")
            countRespondusQuizzes(courseId, result_dict)

        runThreadedRows(targetCoursesDf, _worker)

        ## Step 3: Aggregate and log the totals
        total_quizzes = sum(result[0] for result in result_dict.values())
        total_students = set()
        for result in result_dict.values():
            total_students.update(result[1])

        localSetup.logger.info(f"Total Respondus quizzes: {total_quizzes}")
        localSetup.logger.info(
            f"Total unique students: {len(total_students)} across all courses"
        )

    except Exception as Error:
        errorHandler.sendError(functionName, Error)

if __name__ == "__main__":
    ## Set working directory
    os.chdir(os.path.dirname(__file__))

    ## Count Respondus quizzes for the listed courses
    countListedCoursesRespondusQuizzes()

    input("Press enter to exit")
