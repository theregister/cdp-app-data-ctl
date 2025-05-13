
from    pathlib        import  Path
from    io             import  StringIO

from    psycopg        import sql

import  datetime    as dt
import  pandas      as pd
import  json        as json
from    bs4         import BeautifulSoup
import  re
import  io
import  csv

import  lib.data.utils_data                             as  utils_data
import  lib.postgres.utils_psycopb                      as  utils_psycopb

import  utils_cdp_data_ctl                              as  utils_cdp_data_ctl

import  cdp_data_ctl_globals                            as  cdp_data_ctl_globals

import  utils_cdp_data_ctl_data_version_sql             as  utils_cdp_data_ctl_data_version_sql

#import AI.AI_03.enttech      as EntityTechNER

#import  vertexai
#from    vertexai.generative_models import GenerativeModel
#from    vertexai.preview.language_models import TextGenerationModel

def new(ctx):
    """
    Create a new data version
    """
    ctx.obj.logger.info("data version create")

    # Connect to the database
    with utils_psycopb.connect(ctx.obj.logger) as connection:
        with connection.cursor() as cursor:

            # ============================================================
            # get a new run id/timestamp
            # ============================================================
            current_run_id = utils_psycopb.get_next_seq_id(connection, cdp_data_ctl_globals.sequence_run_id)
            current_run_timestamp = pd.Timestamp.now()
            ctx.obj.logger.info(f"this run_id: {current_run_id}")

            # get a new batch_id - assume for this process 1 run, 1 batch
            current_batch_id = utils_psycopb.get_next_seq_id(connection, "cdp_data_batch_id_seq")
            ctx.obj.logger.info(f"this batch_id: {current_batch_id}")

            # create new data version row
            cursor.execute(utils_cdp_data_ctl_data_version_sql.data_version_insert_new,
                               ( current_run_id, current_batch_id  ))
            data_version_id = cursor.fetchone()[0]
            ctx.obj.logger.info(f"Created new data version with ID: {data_version_id}")

            # insert new rows (version) for leads.delivered, ggladmanager etc
            ctx.obj.logger.info("INSERT >> data_version_leads_delivered_insertd")
            cursor.execute(utils_cdp_data_ctl_data_version_sql.data_version_leads_delivered_insert,
                               ( data_version_id, ))
            inserted_rows = cursor.rowcount
            #inserted_rows = cursor.fetchall()
            print(f"inserted rows: {inserted_rows}")

            # insert new rows (version) for leads.delivered, ggladmanager etc
            ctx.obj.logger.info("INSERT >> data_version_ggladmanager_insert")
            cursor.execute(utils_cdp_data_ctl_data_version_sql.data_version_ggladmanager_insert,
                               ( data_version_id, ))
            inserted_rows = cursor.rowcount
            #inserted_rows = cursor.fetchall()
            print(f"inserted rows: {inserted_rows}")

            ctx.obj.logger.info(f"Created new data version with ID: {data_version_id}")

