# =============================================================================
# Defines functions for each top level menu option.


import click

# load sub commands
#from Commands.cdp_app_data_ctl.import_scc   import click_import     as click_import
from    Commands.cdp_app_data_ctl.env          import click_env        as click_env
from    Commands.cdp_app_data_ctl.report       import click_report     as click_report
from    Commands.cdp_app_data_ctl.extract      import click_extract    as click_extract
from    Commands.cdp_app_data_ctl.load         import click_load       as click_load

import  cdp_app_data_ctl_lib                    as cdp_app_data

import  utils_cdp_app_data_ctl_project_insights as utils_cdp_app_data_ctl_project_insights

# =============================================================================
# ENV GROUP
# =============================================================================
@click.group()
@click.version_option()
@click.pass_context
def env(ctx):
    """environment operations"""
    click.echo("environment opterations")

# =============================================================================
# PROCESS GROUP
# =============================================================================
@click.group()
@click.version_option()
@click.pass_context
def process(ctx):
    """processing functions"""
    click.echo("processing functions")

@process.command()
@click.pass_context
def generate_project_insights(ctx):
    """extract keywords from assets"""
    click.echo("extract keywords from assets")
    utils_cdp_app_data_ctl_project_insights.generate(ctx)

#@process.command()
#@click.pass_context
#def match_classification_to_taxonomy(ctx):
#    """match the published asset classification to a taxonomy"""
#    click.echo("match the published asset classification to a taxonomy")
#    #utils_cdp_app_data_ctl_extract.match_classification_to_taxonomy(ctx)

#@process.command()
#@click.pass_context
#def data_version_new(ctx):
#    """create a new data version"""
#    click.echo("create a new data version")
#    #utils_cdp_app_data_ctl_data_version.new(ctx)

# =============================================================================
# LOAD GROUP
# =============================================================================
@click.group()
@click.version_option()
@click.pass_context
def load(ctx):
    """build data operations"""
    click.echo("build data operations")

# =============================================================================
# LOAD GROUP - TO_STAGE - and then commands
# =============================================================================
@load.group()
@click.version_option()
@click.pass_context
def to_stage(ctx):
    """load data to staging"""
    click.echo("load data to staging")

@to_stage.command()
@click.pass_context
def taxonomy(ctx):
    """load taxonomies to staging"""
    click.echo("load taxonomies to staging")
    #utils_cdp_app_data_ctl_to_stage_taxonomy.load_to_stage(ctx)
    print("load taxonomies to staging")

# =============================================================================
# LOAD GROUP - FROM_STAGE - and then commands
# =============================================================================
@load.group()
@click.version_option()
@click.pass_context
def from_stage(ctx):
    """load data from staging to data"""
    click.echo("load data from staging to data")

@from_stage.command()
@click.pass_context
def taxonomy(ctx):
    """load taxonomies from staging to data"""
    click.echo("taxonomies")
    #utils_cdp_app_data_ctl_from_stage_taxonomy.load_from_stage(ctx)
    print("load taxonomies from staging to data")