
import traceback

import lib.logging.utils_logging as utils_logging
#import logging.utils_logging

class CDP_DATActl_Exception(Exception):
  
    # Error is derived class for Exception, but
    # Base class for exceptions in this module
    pass

def print_caught_message(logger):

    utils_logging.log_info(logger, "\nCDP_DATActl_Exception:", True)
    utils_logging.log_info(logger, "Exiting CDP_DATActl With ERRORS.", True)
    utils_logging.log_info(logger, "Check the log file for more details on exceptions raised.", True)
    utils_logging.log_info(logger, f"=======================================", True)

def unhandled_exception_message(e, logger):

    utils_logging.log_info(logger, str(type(e)), True)
    utils_logging.log_info(logger, traceback.format_exc(), True)
    utils_logging.log_info(logger, "UNHANDLED EXCEPTION RAISED: An unknown problem occurred.", True)
    utils_logging.log_info(logger, "\nCheck the log file for more details on exceptions raised.", True)
    utils_logging.log_info(logger, "\nExiting CDP_DATActl With ERRORS.\n", True)
    utils_logging.log_info(logger, "Raising unhandled exeception..............." + str(print(type(e))), True)
    utils_logging.log_info(logger, f"=======================================\n", True)
