
import datetime

program_name            = 'cdp_data_ctl'

sequence_run_id         = 'cdp_data_run_id_seq'
sequence_batch_id       = 'cdp_data_batch_id_seq'

#==========================================================================================
# SSH
#==========================================================================================
#from Commands.Ssh                   import commands     as ssh_command_module

#==========================================================================================
# PYTHON
#==========================================================================================
#from Commands.Python.Python         import commands     as python_python_command_module
#from Commands.Python.Executable     import commands     as python_executable_command_module
#from Commands.Python.Clean          import commands     as python_clean_command_module

#==========================================================================================
# TDA
#==========================================================================================
#from Commands.TDA.auth               import commands     as tda_auth_command_module
#from Commands.TDA.user               import commands     as tda_user_command_module
#from Commands.TDA.data               import commands     as tda_data_command_module

#==========================================================================================
# AWS
#==========================================================================================
#from Commands.AWS.Ec2.Instance  import commands     as aws_ec2_instance_command_module
#from Commands.AWS.Account       import commands     as aws_account_command_module
#from Commands.AWS.Resources     import commands     as aws_resources_command_module
#from Commands.AWS.Vpc           import commands     as aws_vpc_command_module

#==========================================================================================
# GOOGLE
#==========================================================================================
#from Commands.Google            import  commands    as  google_drive_command_module

#==========================================================================================
# ANALYZE
#==========================================================================================
#from Commands.Analyze               import commands     as analyze_command_module

#==========================================================================================
# Data
#==========================================================================================
#from Commands.Data.Financial  import commands     as data_financial_command_module
#from Commands.Data.News       import commands     as data_news_command_module

from Commands.cdp_data_ctl import click_cdp_data_ctl as click_cdp_data_ctl_command_module

program_name='cdp_data_ctl'

# define command hierarchy and function to call
command_hierarchy = {
}

def get_datetime_string_3ms():

        datetime_string_3ms = str(datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")[:-3])
        return(datetime_string_3ms)