# =============================================================================
# Add options to the top level LOAD menu
# =============================================================================

import click

# =============================================================================
# HIERARCHY group
# =============================================================================
#@click.group()
#@click.version_option()
#@click.pass_context
#def hierarchy(ctx):
#    """load a hierarchy"""
#    click.echo("load a hierarchy")

# =============================================================================
# SOURCE group
# =============================================================================
@click.group()
@click.version_option()
@click.pass_context
def source(ctx):
    """load source data"""
    click.echo("load source data")

# hiararchy-command
#@hierarchy.command()
#@click.pass_context
#def hierarchy_command(ctx):
#    click.echo("load a hiararchy - hierarchy-command")