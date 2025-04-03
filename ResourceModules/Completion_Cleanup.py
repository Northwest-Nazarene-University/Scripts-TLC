import logging, atexit

## This function closes the handlers within the root handler for the script in question
## It is meant to be run by the primary script upon script completion
def completionCleanup ():
    rootLogger = logging.getLogger()

    loggingHandlers = rootLogger.handlers

    for handler in loggingHandlers:
        handler.close()


# Register the cleanup function to be executed when the program exits
atexit.register(completionCleanup)