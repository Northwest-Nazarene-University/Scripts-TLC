## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller
##
## DEPRECATED — superseded by Change_Grading_Scheme_For_Listed_Courses.py
##
## This file previously contained a duplicate implementation for updating the
## Canvas grading standard on a list of courses.  It had syntax errors and was
## never imported by the orchestrator (IDT_Canvas_Primary.py).
##
## Use Change_Grading_Scheme_For_Listed_Courses.py instead.
## All functionality is available through changeListedCoursesGradingStandard().

import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ResourceModules"))

from Change_Grading_Scheme_For_Listed_Courses import (   # noqa: F401
    changeListedCoursesGradingStandard,
)
