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
#@click.group()
#@click.version_option()
#@click.pass_context
#def classification_data(ctx):
#    """import classification data"""
#    click.echo("import classification data")

# classification_data command
@click.command()
@click.pass_context
def classification_data(ctx):
    click.echo("load classification data")