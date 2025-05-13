'''
cdp_data_ctl
'''

import  sys
import  click

import  cdp_data_ctl_config                                         as  config
import  cdp_data_ctl_globals                                        as  globals
import  cdp_data_ctl_exceptions                                     as  exceptions

import  lib.general.utils_general                               as  utils_general
import  lib.logging.utils_logging                               as  utils_logging

# import click module for each supported command
from    Commands.cdp_data_ctl               import click_cdp_data_ctl   as  click_cdp_data_ctl

@click.group(invoke_without_command=True, no_args_is_help=True)
@click.option('--dir_config',              default='.',     type=click.Path(exists=True), nargs=1, envvar='CDP_DATA_CTL_HOME_CONFIG',                        help="Config Directory or use CDP_DATA_CTL_HOME_CONFIG")
@click.option('--dir_log',                 default='.',     type=click.Path(exists=True), nargs=1, envvar='CDP_DATA_CTL_HOME_LOG',                           help="Log Directory or use CDP_DATA_CTL_HOME_LOG")
@click.option('--dir_out',                 default='.',     type=click.Path(exists=True), nargs=1, envvar='CDP_DATA_CTL_HOME_OUT',                           help="Out Directory or use CDP_DATA_CTL_HOME_OUT")
@click.option('--dir_in',                  default='.',     type=click.Path(exists=True), nargs=1, envvar='CDP_DATA_CTL_HOME_IN',                            help="In Directory or use CDP_DATA_CTL_HOME_IN")

@click.option('--debug/--no-debug',        default=False,                                          envvar='CDP_DATA_CTL_HOME_DEBUG',                         help="Turn on Debug (not implemented) or use  CDP_DATA_CTL_HOME_DEBUG")
@click.option('--debug_level',             default=False,                                          envvar='CDP_DATA_CTL_HOME_DEBUG_LEVEL',                   help="Debug Level (not implemented) or use CDP_DATA_CTL_HOME_DEBUG_LEVEL")

@click.pass_context
@click.version_option()

def main(   ctx,
            dir_config,
            dir_log,
            dir_out,
            dir_in,
            debug,
            debug_level
        ):

    try:

        # get unique identifier for current execution and pass it to set up logger
        unique_identifier = utils_general.get_unique_identifier()
        # get logger
        logger1 = utils_logging.get_logger(globals.program_name, dir_log, unique_identifier)
        utils_logging.start_message(logger1, unique_identifier)\
        # consolidate all global configuration into context object (ctx), including logger and config plus global command line options.
        # ctx can then be passed throughout the program.
        ctx.obj = config.Config(logger1,
                                dir_config,
                                dir_log,
                                dir_out,
                                dir_in,
                                debug,
                                debug_level,
                                unique_identifier
                                )
        utils_logging.end_message(logger1)

    # known exception
    except exceptions.CDP_DATA_CTL_Exception as e:
        exceptions.CDP_DATA_CTL_Exception.print_caught_message(logger1)
        sys.exit()

    # unknown exception
    except Exception as e:
        exceptions.unhandled_exception_message(e, logger1)
        raise e

# add options to the top level menu
main.add_command(click_cdp_data_ctl.load)
main.add_command(click_cdp_data_ctl.env)
#main.add_command(click_cdp_data_ctl.import_scc)
main.add_command(click_cdp_data_ctl.process)

# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == '__main__':
    main()
