## Author: Bryce Miller - brycezmiller@nnu.edu
## Last Updated by: Bryce Miller

## Define the script name, purpose, and external requirements for 
## logging and error reporting purposes
script_name = "Error_Email_API"

purpose_of_script = r"""
This class contains the methods and variables to send error emails \
with the Gmail API utilizing the local Core Gmail Api module
"""
external_requirements_to_work_properly = r"""
See https://developers.google.com/gmail/api/auth/web-server for the \
gmail api authorization requirements
"""


from Core_Microsoft_Api import sendOutlookEmail

class errorEmailApi():
    """Used to send function specific error emails"""

    ## This class method creates a formated error email
    def createErrorEmailBody (p2_ScriptName, p2_ScriptPurpose,
                                 p1_ExternalRequirements, p3_functionName, p3_ErrorInfo):
        functionName = "createErrorEmailBody"
        formated_email_body = (f"""
    To the LMS Admin or the department chair of Instructional Design and Technology,

    An Error has occured in the {p2_ScriptName} script while running the function "{p3_functionName}"

    Error Description/Code: {p3_ErrorInfo}.

    Details on the purpose and function of the Syllabus Retrieval script are below.



    Author Contact Information
        Name: Bryce Miller
        Phone: 509-995-1170 
        Work Email: brycezmiller@nnu.edu 
        Personal Email: brycezmiller@gmail.com

    Details regarding this script

    Purpose:
        {p2_ScriptPurpose}

    Requirements to work properly:
        {p1_ExternalRequirements}
    """)
        return formated_email_body

    ## Public Methods

    ## This static method handles function errors
    def sendEmailError (p2_ScriptName, p2_ScriptPurpose,
                       p2_ExternalRequirements, p2_ErrorLocation, p2_ErrorInfo):
        
        functionName = "sendEmailError"

        ## Create the formated email contents
        main_error_email = errorEmailApi.createErrorEmailBody(p2_ScriptName, p2_ScriptPurpose, p2_ExternalRequirements, p2_ErrorLocation, str(p2_ErrorInfo))

        ## Send the error alert email
        sendOutlookEmail(p1_microsoftUserName = "lmsservice@nnu.edu"
                         , p1_subject = f"{p2_ScriptName}: Error in \"{p2_ErrorLocation}\""
                         , p1_body = main_error_email
                         , p1_recipientEmailList = "tlc@nnu.edu"
                         , p1_shared_mailbox = "tlc@nnu.edu")
        

## For testing
if __name__ == "__main__":
    errorEmailApi.sendEmailError("test Script", "test purpose", "test requirements", "test error location", "test error info")
    input("Press enter to exit")