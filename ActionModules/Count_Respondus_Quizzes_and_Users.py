## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

import os, sys, threading, time, pandas as pd
from datetime import datetime

# Make ResourceModules importable (mirrors Course_Date_Related_Actions.py)
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

try:  # Irregular try clause, do not comment out in testing
    from Local_Setup import LocalSetup
    from Error_Email import errorEmail
    from TLC_Common import makeApiCall
except ImportError:
    # Fallback to relative imports if package layout differs
    from ResourceModules.Local_Setup import LocalSetup
    from ResourceModules.Error_Email import errorEmail
    from ResourceModules.TLC_Common import makeApiCall

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
        # Locate the Canvas resources directory using LocalSetup, similar to SIS in Course_Date_Related_Actions.py
        baseCanvasResourcesPath = localSetup.getInternalResourcePaths('Canvas')

        # Adjust the filename here if you prefer 'courses_to_check.csv'
        targetCoursesCsvFilePath = os.path.join(
            baseCanvasResourcesPath, "Target_Canvas_Course_Ids.csv"
        )

        # Define the necessary thread list
        ongoingCountThreads = []
        result_dict = {}

        # Read the CSV file using pandas
        rawTargetCoursesDf = pd.read_csv(targetCoursesCsvFilePath)

        # Retain only rows that have a value in canvas_course_id
        targetCoursesDf = rawTargetCoursesDf[
            rawTargetCoursesDf["canvas_course_id"].notna()
        ]

        # Iterate over each row in the DataFrame
        for index, row in targetCoursesDf.iterrows():
            # Get the course id from the row
            courseId = str(row["canvas_course_id"]).replace(".0", "")

            # Create a thread to count Respondus quizzes for the course
            countThread = threading.Thread(
                target=countRespondusQuizzes,
                args=(courseId, result_dict),
            )

            # Start the thread
            countThread.start()

            # Add the thread to the ongoing count threads list
            ongoingCountThreads.append(countThread)

            # Sleep for a short time to avoid overloading the server
            time.sleep(0.1)

        # Wait for all count threads to complete
        for thread in ongoingCountThreads:
            thread.join()

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
