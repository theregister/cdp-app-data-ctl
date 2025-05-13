
import  os                  as      os
import  sys                 as      sys
import  errno               as      errno
import  configparser        as      cp
import  traceback           as      traceback
import  inspect             as      inspect
import  datetime            as      datetime
import  shutil              as      shutil

import cdp_data_ctl_exceptions  as      exceptions
import cdp_data_ctl_globals     as      globals

import lib.logging.utils_logging as utils_logging

class Config():
    '''class encapsulating all config data
        - including click options
        - config file data
    '''

    # initialize the Config class.
    # It's possible that a config file does not exist under normal operations (command line config new).
    # In this case set config_file_available = False
    def __init__(   self,
                    logger,
                    dir_config,
                    dir_log,
                    dir_out,
                    dir_in,
                    debug,
                    debug_level,
                    unique_identifier
                    ):
    
        try:
            utils_logging.log_info(logger, "==================================================", False)
            utils_logging.log_info(logger, "START - Config Initialize", False)

            utils_logging.log_info(logger, "Config object", False)
            self.dir_config                 = os.path.abspath(dir_config or '.')
            self.dir_log                    = os.path.abspath(dir_log    or '.')
            # set out directory to the unique identifier subdirectory
            self.dir_out                    = os.path.join(os.path.abspath(dir_out    or '.'), unique_identifier)
            # and create
            os.mkdir(self.dir_out)

            self.dir_in                         = os.path.abspath(dir_in     or '.')
            self.debug                          = debug
            self.debug_level                    = debug_level
            self.logger                         = logger
            self.unique_identifier              = unique_identifier

            utils_logging.log_info(logger, '====================================================================', False)
            utils_logging.log_info(logger, 'Config:', False)
            utils_logging.log_info(logger, "self.dir_config                     = " + self.dir_config, False)
            utils_logging.log_info(logger, "self.dir_log                        = " + self.dir_log, False)
            utils_logging.log_info(logger, "self.dir_out                        = " + self.dir_out, False)
            utils_logging.log_info(logger, "self.dir_in                         = " + self.dir_in, False)
            utils_logging.log_info(logger, "self.debug                          = " + str(self.debug), False)
            utils_logging.log_info(logger, "self.debug_level                    = " + str(self.debug_level), False)
            utils_logging.log_info(logger, "self.logger                         = " + str(self.logger), False)
            utils_logging.log_info(logger, "self.unique_identifier              = " + str(self.logger), False)
            utils_logging.log_info(logger, '====================================================================', False)

        except:
            raise

        utils_logging.log_info(logger, "END - Config Initialize", False)
        utils_logging.log_info(logger, "==================================================", False)
