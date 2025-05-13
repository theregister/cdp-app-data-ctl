
import      pandas                                          as pd
import      lib.postgres.utils_psycopb                      as utils_psycopb
#import      utils_cdp_data                                 as utils_cdp_data
import      utils_cdp_data_ctl_from_stage_taxonomy        as utils_cdp_data_ctl_from_stage_taxonomy
import      utils_cdp_data_ctl_from_stage_taxonomy_sql    as utils_cdp_data_ctl_from_stage_taxonomy_sql

import  cdp_data_ctl_globals                               as cdp_data_ctl_globals

def load_from_stage(ctx):

    # set dataframe output options
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)

    connection = utils_psycopb.connect(ctx.obj.logger)
    ctx.obj.logger.info("Connected to database")

    # get a new run id and pass to functions for insert.
    current_run_id = utils_psycopb.get_next_seq_id(connection, cdp_data_ctl_globals.sequence_run_id)
    ctx.obj.logger.info(f"this run_id: {current_run_id}")

    with connection.cursor() as cursor:

        # get list of hierarchies to create (from import table)
        cursor.execute(utils_cdp_data_ctl_from_stage_taxonomy_sql.sql_select_taxonomies_to_import)
        hierarchies_to_create = cursor.fetchall()
        hierarchies_to_create_df = pd.DataFrame(hierarchies_to_create, columns=[desc[0] for desc in cursor.description])
        ctx.obj.logger.info("hierarchies_to_create_df:")
        ctx.obj.logger.info('\n' + str(hierarchies_to_create_df))

        for _, row in hierarchies_to_create_df.iterrows():

            ctx.obj.logger.info(f"LOOPING")

            # get a new batch_id

            current_batch_id = utils_psycopb.get_next_seq_id(connection, "cdp_data_batch_id_seq")
            ctx.obj.logger.info(f"this batch_id: {current_batch_id}")

            # this is what is used to query the source rows in cdp_stg_taxonomy
            source_batch_id = row['int_current_batch_id']
            ctx.obj.logger.info(f"Processing source taxonomy with int_current_batch_id: {source_batch_id}")

            # does this taxonomy already exist?
            ctx.obj.logger.info('check if taxonomy exists with this name: ' + row['int_source_id'])
            cursor.execute(utils_cdp_data_ctl_from_stage_taxonomy_sql.sql_check_taxonomy_exists,
                           (row['int_source_id'],))
            taxonomy_exists = cursor.fetchone()

            if not taxonomy_exists:
                # Create a new taxonomy
                ctx.obj.logger.info(f"Creating new taxonomy")

                # fetch taxonomy rows
                cursor.execute( utils_cdp_data_ctl_from_stage_taxonomy_sql.sql_select_taxonomy_to_import_by_load_id,
                                (source_batch_id,))
                taxonomy_data_to_load = cursor.fetchall()
                taxonomy_data_to_load_df = pd.DataFrame(taxonomy_data_to_load, columns=[desc[0] for desc in cursor.description])
                ctx.obj.logger.info('\n' + str(taxonomy_data_to_load_df))

                cursor.execute(utils_cdp_data_ctl_from_stage_taxonomy_sql.sql_insert_taxonomy,
                               (    row['int_source_id'],
                                    row['int_source_system'],
                                    row['data_type'],
                                    'cdp_data_ctl load taxonomy',
                                    'use taxonomy name',
                                    '',
                                    source_batch_id,
                                    current_batch_id,
                                    current_run_id))
                taxonomy_id = cursor.fetchone()[0]
                ctx.obj.logger.info(f"Created new taxonomy with ID: {taxonomy_id}")
            
                # create new taxonomy version
                # create a new taxonomy version
                # Get the next version number for the taxonomy
                cursor.execute(utils_cdp_data_ctl_from_stage_taxonomy_sql.sql_get_next_taxonomy_version, (taxonomy_id,))
                next_version_number = cursor.fetchone()[0]
                ctx.obj.logger.info(f"Next version number for taxonomy {taxonomy_id}: {next_version_number}")
            
                cursor.execute(utils_cdp_data_ctl_from_stage_taxonomy_sql.sql_insert_taxonomy_version,
                               (taxonomy_id,
                                next_version_number,
                                'cdp_data_ctl load taxonomy',
                                'use taxonomy version data',
                                '',
                                source_batch_id,
                                current_batch_id,
                                current_run_id))
                taxonomy_version_id = cursor.fetchone()[0]
                ctx.obj.logger.info(f"Created new taxonomy version with ID: {taxonomy_version_id}")

                ctx.obj.logger.info("LOADING THIS taxonomy:")

                ctx.obj.logger.info("taxonomy_data_to_load_df:")
                ctx.obj.logger.info('\n' + str(taxonomy_data_to_load_df))

                taxonomy_data_to_load_df['taxonomy_version_id'] = taxonomy_version_id

                try: 

                    function_map = {
                        1: [utils_cdp_data_ctl_from_stage_taxonomy_sql.sql_insert_taxonomy_level_01_rows,
                            utils_cdp_data_ctl_from_stage_taxonomy_sql.sql_insert_taxonomy_level_01],
                        2: [utils_cdp_data_ctl_from_stage_taxonomy_sql.sql_insert_taxonomy_level_02_rows,
                            utils_cdp_data_ctl_from_stage_taxonomy_sql.sql_insert_taxonomy_level_02],
                        3: [utils_cdp_data_ctl_from_stage_taxonomy_sql.sql_insert_taxonomy_level_03_rows,
                            utils_cdp_data_ctl_from_stage_taxonomy_sql.sql_insert_taxonomy_level_03],
                        4: [utils_cdp_data_ctl_from_stage_taxonomy_sql.sql_insert_taxonomy_level_04_rows,
                            utils_cdp_data_ctl_from_stage_taxonomy_sql.sql_insert_taxonomy_level_04],
                        5: [utils_cdp_data_ctl_from_stage_taxonomy_sql.sql_insert_taxonomy_level_05_rows,
                            utils_cdp_data_ctl_from_stage_taxonomy_sql.sql_insert_taxonomy_level_05]
                    }

                    ctx.obj.logger.info("=============================================================")
                    ctx.obj.logger.info("Loop Over Hierachies to Import")
                    ctx.obj.logger.info("=============================================================")

                    for taxonomy_level in range(1, 6):
                        
                        ctx.obj.logger.info("confirm rows - level: "    + str(taxonomy_level))
                        function=function_map[taxonomy_level][0]
                        ctx.obj.logger.info("function: "                + str(function))

                        ctx.obj.logger.info("current_run_id: "          + str(current_run_id))
                        ctx.obj.logger.info("current_batch_id: "        + str(current_batch_id))
                        ctx.obj.logger.info("source_batch_id: "         + str(source_batch_id))
                        ctx.obj.logger.info("taxonomy_version_id: "     + str(taxonomy_version_id))
                        cursor.execute(     function, 
                                            (   current_run_id,         # int_run_id
                                                current_batch_id,       # int_current_batch_id
                                                source_batch_id,        # int_source_batch_id
                                                taxonomy_version_id     # int_taxonomy_version_id
                                            ))

                        level_rows = cursor.fetchall()
                        level_df = pd.DataFrame(level_rows, columns=[desc[0] for desc in cursor.description])
                        ctx.obj.logger.info("DataFrame - LEVEL: "       + str(taxonomy_level))
                        ctx.obj.logger.info('\n' + str(level_df))

                        ctx.obj.logger.info("insert - START LEVEL: "    + str(taxonomy_level))
                        function=function_map[taxonomy_level][1]
                        ctx.obj.logger.info("function: " + str(function))

                        ctx.obj.logger.info("current_run_id: "          + str(current_run_id))
                        ctx.obj.logger.info("current_batch_id: "        + str(current_batch_id))
                        ctx.obj.logger.info("source_batch_id: "         + str(source_batch_id))
                        ctx.obj.logger.info("taxonomy_version_id: "     + str(taxonomy_version_id))
                        cursor.execute(     function, 
                                            (   current_run_id,         # int_run_id
                                                current_batch_id,       # int_current_batch_id
                                                source_batch_id,        # int_source_batch_id
                                                taxonomy_version_id     # int_taxonomy_version_id
                                            ))

                        level_data = cursor.fetchall()
                        ctx.obj.logger.info("Inserted Nodes for Level: " + str(taxonomy_level))
                        for row in level_data:
                            ctx.obj.logger.info(f"Inserted row: {row}")
                        ctx.obj.logger.info("DONE - Inserted Nodes for Level: " + str(taxonomy_level))

                    connection.commit()

                except Exception as e:
                    print("error creating taxonomy")
                    ctx.obj.logger.error(f"Error moving taxonomy from staging to data: {e}")
                    connection.rollback()   

    connection.close()

