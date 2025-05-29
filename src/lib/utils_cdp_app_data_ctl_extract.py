
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

import  lib.data.utils_data                         as  utils_data
import  lib.postgres.utils_psycopb                  as  utils_psycopb

import  utils_cdp_data_ctl                          as  utils_cdp_data_ctl

import  cdp_data_ctl_globals                        as  cdp_data_ctl_globals

import utils_cdp_data_ctl_prompt                    as  utils_cdp_data_ctl_prompt
import utils_cdp_data_ctl_prompt_taxonomy_entries   as  utils_cdp_data_ctl_prompt_taxonomy_entries
import utils_cdp_data_ctl_extract_sql               as  utils_cdp_data_ctl_extract_sql
import utils_cdp_data_ctl_llm                       as  utils_cdp_data_ctl_llm

#import AI.AI_03.enttech      as EntityTechNER

#import  vertexai
#from    vertexai.generative_models import GenerativeModel
#from    vertexai.preview.language_models import TextGenerationModel

def match_classification_to_taxonomy(ctx):

    """
    This routine

        - takes a set of assets and their classifications (marked PUBLISH or some other label)
        - takes a taxonomy
        - asks LLM to match classifications to the taxonomy
    """

    ctx.obj.logger.info("match_classification_to_taxonomy")

    # Connect to the database
    with utils_psycopb.connect(ctx.obj.logger) as conn:
        with conn.cursor() as cur:

            # ============================================================
            # get a new run id/timestamp
            # ============================================================
            current_run_id = utils_psycopb.get_next_seq_id(conn, cdp_data_ctl_globals.sequence_run_id)
            current_run_timestamp = pd.Timestamp.now()
            ctx.obj.logger.info(f"this run_id: {current_run_id}")

            # ============================================================
            # Get ASSET DATA - for a set of identified assets - has asset classifications published with prompt/llm combination
            # ============================================================
            cur.execute(utils_cdp_data_ctl_extract_sql.sql_get_assets_for_taxonomy_matching, )
            asset_df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
            message = "number of assets selected for processing :" + str(len(asset_df))
            ctx.obj.logger.info(message)
            print(message)
            ctx.obj.logger.info('\n' + str(asset_df))

            # Output asset_df to a file
            output_path = Path(ctx.obj.dir_out) / "asset_df.csv"
            utils_data.write_df_to_csv(asset_df, output_path)
            # Log the output path
            ctx.obj.logger.info(f"Asset DataFrame written to: {output_path}")

            # ============================================================
            # Get TAXONOMY
            # ============================================================
            #taxonomy_name = 'data/import/tech.taxonomy.claude.001.csv'
            #taxonomy_name = 'taxonomy.version.02.for.initial.upload.csv'
            taxonomy_name = 'taxonomy.version.02.for.update.csv'
            taxonomy_version = 1

            # get taxonomy_version_id
            cur.execute(utils_cdp_data_ctl_extract_sql.sql_get_taxonomy_version_id, (taxonomy_name, taxonomy_version,))
            int_taxonomy_version_id = cur.fetchone()[0]
            ctx.obj.logger.info(f"int_taxonomy_version_id: {int_taxonomy_version_id}")

            # get taxonomy
            cur.execute(utils_cdp_data_ctl_extract_sql.sql_get_taxonomy_tree, (int_taxonomy_version_id,))
            taxonomy_df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
            ctx.obj.logger.info("Taxonomy data loaded")
            ctx.obj.logger.info(taxonomy_df)
            output_path = Path(ctx.obj.dir_out) / "taxonomy_df.csv"
            utils_data.write_df_to_csv(taxonomy_df, output_path)

            # ============================================================
            # SET PROMPT
            # ============================================================
            #input_prompt="prompt_classification_taxonomy_03"
            input_prompt="prompt_taxonomy_match_20250428"

            # ============================================================
            # LOOP through the Asset Dataframe rows
            # ============================================================
            for index, row in asset_df.iterrows():

                # get new batch_id
                current_batch_id = utils_psycopb.get_next_seq_id(conn, "cdp_data_batch_id_seq")
                ctx.obj.logger.info(f"this batch_id: {current_batch_id}")

                ctx.obj.logger.info("=============================================================")
                ctx.obj.logger.info(str(row['id']) + str(row['title']) + str(len(row['asset_text'])))
                ctx.obj.logger.info("=============================================================")

                output_path = Path(ctx.obj.dir_out) / f"paper_{row['id']}.csv"
                row_df = pd.DataFrame([row])
                utils_data.write_df_to_csv(row_df, output_path)

                # ============================================================
                # Get ASSET CLASSIFICATION DATA - use single set of assets with associated classifications
                # ============================================================
                print("=============================================================")
                print("using ASSET_ID: " + str(row['id']))
                print("=============================================================")
                cur.execute(utils_cdp_data_ctl_extract_sql.sql_get_asset_classifications_for_matching, (row['id'],))  # should be an asset_id
                asset_classification_df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
                message = "number of asset classifications selected for processing :" + str(len(asset_classification_df))
                if len(asset_classification_df) == 0:
                    ctx.obj.logger.info("No asset classifications found for processing.")
                    raise ValueError("No asset classifications found for processing.")
                ctx.obj.logger.info(message)
                print(message)
                ctx.obj.logger.info('\n' + str(asset_classification_df))

                # Output asset_df to a file
                output_path = Path(ctx.obj.dir_out) / f"asset_{row['id']}_classification_df.csv"
                utils_data.write_df_to_csv(asset_classification_df, output_path)
                # Log the output path
                ctx.obj.logger.info(f"Asset Classifications DataFrame written to: {output_path}")

                # ============================================================
                # construct prompt
                # ============================================================
                input_prompt_type = 'taxonomy_matching'
                prompt = utils_cdp_data_ctl_prompt.get_prompt_text(prompt_type=input_prompt_type,
                                                                   prompt_name=input_prompt,
                                                                   input_text=row['asset_text'],
                                                                   taxonomy_df=taxonomy_df,
                                                                   asset_classification_df = asset_classification_df)
                prompt_output_path = Path(ctx.obj.dir_out) / f"paper_{row['id']}_prompt.txt"
                with open(prompt_output_path, 'w', encoding='utf-8') as f:
                    f.write(prompt)

                # call LLM
                llm_response_text = utils_cdp_data_ctl_llm.call_gemini(ctx, prompt)

                # ============================================================
                # Output RAW llm_response_text to file
                # ============================================================
                output_path = Path(ctx.obj.dir_out) / f"paper_{row['id']}_llm_response_text.txt"
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(llm_response_text)

                # ============================================================
                # Attempt conversion to DF
                # ============================================================

                # Find and extract the TSV content
                lines = llm_response_text.strip().split('\n')
                ctx.obj.logger.info(f"Type of lines: {type(lines)}")
                ctx.obj.logger.info(f"Number of lines: {len(lines)}")

                # using prompt to classify only
                expected_tab_count = 7 # 8 columns so tab count is 7

                # Filter out any lines that don't look like TSV data
                tsv_lines_true  = []
                tsv_lines_false = []
                for line in lines:
                    if '\t' in line:  # Only keep lines with tabs
                        tab_count = line.count('\t')
                        if tab_count != expected_tab_count:
                            tsv_lines_false.append(line)
                        else:
                            tsv_lines_true.append(line)
                        ctx.obj.logger.info(f"Line: {line} | Tab count: {tab_count}")
                    else:
                        tsv_lines_false.append(line)
                
                ctx.obj.logger.info(f"Number of valid TSV lines: {len(tsv_lines_true)}")
                ctx.obj.logger.info(f"Number of invalid TSV lines: {len(tsv_lines_false)}")

                # loop over tsv_lines_true and write to dataframe
                # Initialize an empty DataFrame with the specified column names
                # llm_response_df = pd.DataFrame(columns=['technology_term', 'relevance_score', 'confidence_score', 'matching_taxonomy_path', 'match_type', 'llm_matched_int_taxonomy_level_05_id', 'llm_matched_int_taxonomy_level_04_id', 'notes'])
                llm_response_df = pd.DataFrame(columns=['int_asset_cls_result_id', 'cls_match_type', 'cls_int_taxonomy_node_name', 'cls_int_taxonomy_node_level', 'cls_int_taxonomy_node_id', 'cls_relevance_score', 'cls_confidence_score', 'cls_notes'])

                # Skip the first line (assumed to be the header)
                for line in tsv_lines_true[1:]:
                    # Split the line by tab character
                    values = line.split('\t')
                    # Append the values as a new row to the DataFrame
                    llm_response_df = pd.concat([llm_response_df, pd.DataFrame([values], columns=llm_response_df.columns)], ignore_index=True)
                # output llm response df to csv and table
                #output_path = Path(ctx.obj.dir_out) / f"paper_{row['id']}_llm_response_df.csv"
                #utils_data.write_df_to_csv(llm_response_df, output_path)


                #llm_response_df['asset_id'] = row['id']
                # Output the llm_response_df to a CSV file
                output_path = Path(ctx.obj.dir_out) / f"paper_{row['id']}_llm_response_df.csv"
                utils_data.write_df_to_csv(llm_response_df, output_path)

                #print(llm_response_df['relevance_score'])
                llm_response_df['cls_confidence_score'] = pd.to_numeric(llm_response_df['cls_confidence_score'], errors='coerce').fillna(-1).astype(int)
                llm_response_df['cls_relevance_score'] = pd.to_numeric(llm_response_df['cls_relevance_score'], errors='coerce').fillna(-1).astype(int)

                #llm_response_df['relevance_score']          = llm_response_df['relevance_score'].astype(int)
                #llm_response_df['confidence_score']         = llm_response_df['confidence_score'].astype(int)
                llm_response_df['int_run_id']               = current_run_id
                llm_response_df['int_run_timestamp']        = current_run_timestamp
                llm_response_df['int_current_batch_id']     = current_batch_id
                llm_response_df['int_source_batch_id']      = -1
                llm_response_df['int_source_type']          = 'taxonomy_matching'
                llm_response_df['cls_type']                 = 'taxonomy_matching'
                llm_response_df['cls_int_taxonomy_version_id']  = int_taxonomy_version_id
                #llm_response_df['taxonomy_name']            = taxonomy_name
                llm_response_df['cls_prompt']                   = input_prompt
                llm_response_df['cls_llm']                      = "gemini-1.5-flash-002"

                # Output the concatenated DataFrame to a CSV file
                output_path = Path(ctx.obj.dir_out) / f"paper_{row['id']}_llm_response_df_final.csv"
                utils_data.write_df_to_csv(llm_response_df, output_path)

                # write to results table
                try:
                    utils_psycopb.df_to_table2(conn, llm_response_df, "marsol_dev_01.cdp_data_asset_cls_taxonomy_result")
                except Exception as e:
                    ctx.obj.logger.error(f"extract_from_asset: Error writing concatenated_df to table: {e}")
                    conn.rollback()

                # now update taxonomy with value level 04 id's identified
                
def extract_keywords_from_assets(ctx):

    """
    This routine
        - read control file - get query_or_view_name,taxonomy_name,prompt_name,llm_name 
        - loops over asset text and passes for classification
        - a taxonomy is specified to use to perform classification
        - a prompt is specified to use to perform classification
        - the asset text is passed to the LLM function
        - the response is written to a file
        - the response is converted to a DataFrame
        - the DataFrame is written to a file
        - the DataFrame is written to a SQL table
    """

    ctx.obj.logger.info("ENTER EXTRACT FROM CONTENT")

    # Connect to the database
    with utils_psycopb.connect(ctx.obj.logger) as conn:
        with conn.cursor() as cur:

            # ============================================================
            # get a new run id/timestamp
            # ============================================================
            current_run_id = utils_psycopb.get_next_seq_id(conn, cdp_data_ctl_globals.sequence_run_id)
            current_run_timestamp = pd.Timestamp.now()
            ctx.obj.logger.info(f"this run_id: {current_run_id}")

            # ============================================================
            # Get ASSET DATA
            # ============================================================
            cur.execute(utils_cdp_data_ctl_extract_sql.sql_get_assets_latest, )
            asset_df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
            message = "number of assets selected for processing :" + str(len(asset_df))
            ctx.obj.logger.info(message)
            print(message)
            ctx.obj.logger.info('\n' + str(asset_df))

            # ============================================================
            # Get TAXONOMY
            # ============================================================
            #taxonomy_name = 'taxonomy.version.02.for.update.csv'
            #taxonomy_version = 1

            # get taxonomy_version_id
            #cur.execute(utils_cdp_data_ctl_extract_sql.sql_get_taxonomy_version_id, (taxonomy_name, taxonomy_version,))
            #int_taxonomy_version_id = cur.fetchone()[0]
            #ctx.obj.logger.info(f"int_taxonomy_version_id: {int_taxonomy_version_id}")

            # get taxonomy
            #cur.execute(utils_cdp_data_ctl_extract_sql.sql_get_taxonomy, (int_taxonomy_version_id,))
            #taxonomy_df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
            #ctx.obj.logger.info("Taxonomy data loaded")
            #ctx.obj.logger.info(taxonomy_df)
            #output_path = Path(ctx.obj.dir_out) / "taxonomy_df.csv"
            #utils_data.write_df_to_csv(taxonomy_df, output_path)

            # ============================================================
            # SET PROMPT
            # ============================================================
            #input_prompt="prompt_classification_taxonomy_03"
            input_prompt="prompt_keyword_identification_20250428"

            # ============================================================
            # LOOP through the Asset Dataframe rows
            # ============================================================
            for index, row in asset_df.iterrows():

                # get new batch_id
                current_batch_id = utils_psycopb.get_next_seq_id(conn, "cdp_data_batch_id_seq")
                ctx.obj.logger.info(f"this batch_id: {current_batch_id}")

                ctx.obj.logger.info("=============================================================")
                ctx.obj.logger.info(str(row['id']) + str(row['title']) + str(len(row['asset_text'])))
                ctx.obj.logger.info("=============================================================")

                output_path = Path(ctx.obj.dir_out) / f"paper_{row['id']}.csv"
                row_df = pd.DataFrame([row])
                utils_data.write_df_to_csv(row_df, output_path)

                # ============================================================
                # construct prompt
                # ============================================================
                input_prompt_type = 'keyword_extraction'
                prompt = utils_cdp_data_ctl_prompt.get_prompt_text(input_prompt_type, input_prompt, row['asset_text'])
                prompt_output_path = Path(ctx.obj.dir_out) / f"paper_{row['id']}_prompt.txt"
                with open(prompt_output_path, 'w', encoding='utf-8') as f:
                    f.write(prompt)

                # call LLM
                llm_response_text = utils_cdp_data_ctl_llm.call_gemini(ctx, prompt)

                # ============================================================
                # Output RAW llm_response_text to file
                # ============================================================
                output_path = Path(ctx.obj.dir_out) / f"paper_{row['id']}_llm_response_text.txt"
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(llm_response_text)

                # ============================================================
                # Attempt conversion to DF
                # ============================================================

                # Find and extract the TSV content
                lines = llm_response_text.strip().split('\n')
                ctx.obj.logger.info(f"Type of lines: {type(lines)}")
                ctx.obj.logger.info(f"Number of lines: {len(lines)}")

                # using prompt to classify only
                expected_tab_count = 3 # 4 columns so tab count is 3

                # Filter out any lines that don't look like TSV data
                tsv_lines_true  = []
                tsv_lines_false = []
                for line in lines:
                    if '\t' in line:  # Only keep lines with tabs
                        tab_count = line.count('\t')
                        if tab_count != expected_tab_count:
                            tsv_lines_false.append(line)
                        else:
                            tsv_lines_true.append(line)
                        ctx.obj.logger.info(f"Line: {line} | Tab count: {tab_count}")
                    else:
                        tsv_lines_false.append(line)
                
                ctx.obj.logger.info(f"Number of valid TSV lines: {len(tsv_lines_true)}")
                ctx.obj.logger.info(f"Number of invalid TSV lines: {len(tsv_lines_false)}")

                # loop over tsv_lines_true and write to dataframe
                # Initialize an empty DataFrame with the specified column names
                # llm_response_df = pd.DataFrame(columns=['technology_term', 'relevance_score', 'confidence_score', 'matching_taxonomy_path', 'match_type', 'llm_matched_int_taxonomy_level_05_id', 'llm_matched_int_taxonomy_level_04_id', 'notes'])
                llm_response_df = pd.DataFrame(columns=['cls_result', 'cls_relevance_score', 'cls_confidence_score', 'cls_notes'])
                # Skip the first line (assumed to be the header)
                for line in tsv_lines_true[1:]:
                    # Split the line by tab character
                    values = line.split('\t')
                    # Append the values as a new row to the DataFrame
                    llm_response_df = pd.concat([llm_response_df, pd.DataFrame([values], columns=llm_response_df.columns)], ignore_index=True)
                # output llm response df to csv and table
                #output_path = Path(ctx.obj.dir_out) / f"paper_{row['id']}_llm_response_df.csv"
                #utils_data.write_df_to_csv(llm_response_df, output_path)

                ##print(llm_response_df)

                # add a column for the algorithm
                #llm_response_df['algorithm_name'] = 'test'
                llm_response_df['asset_id'] = row['id']

                #print(llm_response_df)

                #print("output to df to csv")
                # Output the llm_response_df to a CSV file
                output_path = Path(ctx.obj.dir_out) / f"paper_{row['id']}_llm_response_df.csv"
                utils_data.write_df_to_csv(llm_response_df, output_path)

                #print(llm_response_df['relevance_score'])
                llm_response_df['cls_confidence_score']     = pd.to_numeric(llm_response_df['cls_confidence_score'], errors='coerce').fillna(-1).astype(int)
                llm_response_df['cls_relevance_score']      = pd.to_numeric(llm_response_df['cls_relevance_score'], errors='coerce').fillna(-1).astype(int)
                llm_response_df['int_run_id']               = current_run_id
                llm_response_df['int_run_timestamp']        = current_run_timestamp
                llm_response_df['int_current_batch_id']     = current_batch_id
                llm_response_df['int_source_batch_id']      = -1
                llm_response_df['int_source_type']          = 'keyword_classification'
                llm_response_df['cls_type']                 = 'keyword_classification'
                llm_response_df['cls_prompt']               = input_prompt
                llm_response_df['cls_llm']                  = "gemini-1.5-flash-002"

                # Output the concatenated DataFrame to a CSV file
                output_path = Path(ctx.obj.dir_out) / f"paper_{row['id']}_llm_response_df_final.csv"
                utils_data.write_df_to_csv(llm_response_df, output_path)

                # write to results table
                try:
                    utils_psycopb.df_to_table2(conn, llm_response_df, "marsol_dev_01.cdp_data_asset_cls_result")
                except Exception as e:
                    ctx.obj.logger.error(f"extract_from_asset: Error writing concatenated_df to table: {e}")
                    conn.rollback()

                # now update taxonomy with value level 04 id's identified

def classify_asset(ctx):

    """
    This routine
        - read control file - get query_or_view_name,taxonomy_name,prompt_name,llm_name 
        - loops over asset text and passes for classification
        - a taxonomy is specified to use to perform classification
        - a prompt is specified to use to perform classification
        - the asset text is passed to the LLM function
        - the response is written to a file
        - the response is converted to a DataFrame
        - the DataFrame is written to a file
        - the DataFrame is written to a SQL table
    """

    ctx.obj.logger.info("ENTER EXTRACT FROM CONTENT")

    # Connect to the database
    with utils_psycopb.connect(ctx.obj.logger) as conn:
        with conn.cursor() as cur:

            # ============================================================
            # get a new run id/timestamp
            # ============================================================
            current_run_id = utils_psycopb.get_next_seq_id(conn, cdp_data_ctl_globals.sequence_run_id)
            current_run_timestamp = pd.Timestamp.now()
            ctx.obj.logger.info(f"this run_id: {current_run_id}")

            # ============================================================
            # Get ASSET DATA
            # ============================================================
            cur.execute(utils_cdp_data_ctl_extract_sql.sql_get_assets_02, )
            asset_df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
            message = "number of assets selected for processing :" + str(len(asset_df))
            ctx.obj.logger.info(message)
            print(message)
            ctx.obj.logger.info('\n' + str(asset_df))

            # ============================================================
            # Get TAXONOMY
            # ============================================================
            #taxonomy_name = 'data/import/tech.taxonomy.claude.001.csv'
            #taxonomy_name = 'taxonomy.version.02.for.initial.upload.csv'
            taxonomy_name = 'taxonomy.version.02.for.update.csv'
            taxonomy_version = 1

            # get taxonomy_version_id
            cur.execute(utils_cdp_data_ctl_extract_sql.sql_get_taxonomy_version_id, (taxonomy_name, taxonomy_version,))
            int_taxonomy_version_id = cur.fetchone()[0]
            ctx.obj.logger.info(f"int_taxonomy_version_id: {int_taxonomy_version_id}")

            # get taxonomy
            cur.execute(utils_cdp_data_ctl_extract_sql.sql_get_taxonomy, (int_taxonomy_version_id,))
            taxonomy_df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
            ctx.obj.logger.info("Taxonomy data loaded")
            ctx.obj.logger.info(taxonomy_df)
            output_path = Path(ctx.obj.dir_out) / "taxonomy_df.csv"
            utils_data.write_df_to_csv(taxonomy_df, output_path)

            # ============================================================
            # SET PROMPT
            # ============================================================
            input_prompt="prompt_keyword_identification_20250428"

            # ============================================================
            # LOOP through the Asset Dataframe rows
            # ============================================================
            for index, row in asset_df.iterrows():

                # get new batch_id
                current_batch_id = utils_psycopb.get_next_seq_id(conn, "cdp_data_batch_id_seq")
                ctx.obj.logger.info(f"this batch_id: {current_batch_id}")

                ctx.obj.logger.info("=============================================================")
                ctx.obj.logger.info(str(row['id']) + str(row['title']) + str(len(row['asset_text'])))
                ctx.obj.logger.info("=============================================================")

                output_path = Path(ctx.obj.dir_out) / f"paper_{row['id']}.csv"
                row_df = pd.DataFrame([row])
                utils_data.write_df_to_csv(row_df, output_path)

                # ============================================================
                # construct prompt
                # ============================================================
                prompt = utils_cdp_data_ctl_prompt.get_prompt_text(input_prompt, row['asset_text'], taxonomy_df)
                prompt_output_path = Path(ctx.obj.dir_out) / f"paper_{row['id']}_prompt.txt"
                with open(prompt_output_path, 'w', encoding='utf-8') as f:
                    f.write(prompt)

                # call LLM
                llm_response_text = utils_cdp_data_ctl_llm.call_gemini(ctx, prompt)

                # ============================================================
                # Output RAW llm_response_text to file
                # ============================================================
                output_path = Path(ctx.obj.dir_out) / f"paper_{row['id']}_llm_response_text.txt"
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(llm_response_text)

                # ============================================================
                # Attempt conversion to DF
                # ============================================================

                # Find and extract the TSV content
                lines = llm_response_text.strip().split('\n')
                ctx.obj.logger.info(f"Type of lines: {type(lines)}")
                ctx.obj.logger.info(f"Number of lines: {len(lines)}")

                # Filter out any lines that don't look like TSV data
                tsv_lines_true  = []
                tsv_lines_false = []
                for line in lines:
                    if '\t' in line:  # Only keep lines with tabs
                        tab_count = line.count('\t')
                        if tab_count != 7:
                            tsv_lines_false.append(line)
                        else:
                            tsv_lines_true.append(line)
                        ctx.obj.logger.info(f"Line: {line} | Tab count: {tab_count}")
                    else:
                        tsv_lines_false.append(line)
                
                ctx.obj.logger.info(f"Number of valid TSV lines: {len(tsv_lines_true)}")
                ctx.obj.logger.info(f"Number of invalid TSV lines: {len(tsv_lines_false)}")

                # loop over tsv_lines_true and write to dataframe
                # Initialize an empty DataFrame with the specified column names
                llm_response_df = pd.DataFrame(columns=['classification_result', 'relevance_score', 'confidence_score', 'matching_taxonomy_path', 'match_type', 'llm_matched_int_taxonomy_level_05_id', 'llm_matched_int_taxonomy_level_04_id', 'notes'])

                # Loop over each line in tsv_lines_true and append to the DataFrame
                for line in tsv_lines_true:
                    # Split the line by tab character
                    values = line.split('\t')
                    # Append the values as a new row to the DataFrame
                    llm_response_df = pd.concat([llm_response_df, pd.DataFrame([values], columns=llm_response_df.columns)], ignore_index=True)
               
                # output llm response df to csv and table
                output_path = Path(ctx.obj.dir_out) / f"paper_{row['id']}_llm_response_df.csv"
                utils_data.write_df_to_csv(llm_response_df, output_path)

                # add a column for the algorithm
                llm_response_df['algorithm_name'] = 'test'
                llm_response_df['asset_id'] = row['id']

                print("output to df to csv")
                # Output the llm_response_df to a CSV file
                output_path = Path(ctx.obj.dir_out) / f"paper_{row['id']}_llm_response_df.csv"
                utils_data.write_df_to_csv(llm_response_df, output_path)

                llm_response_df['relevance_score']          = llm_response_df['relevance_score'].fillna(-1).astype(int)
                llm_response_df['confidence_score']         = llm_response_df['confidence_score'].fillna(-1).astype(int)
                llm_response_df['relevance_score']          = llm_response_df['relevance_score'].astype(int)
                llm_response_df['confidence_score']         = llm_response_df['confidence_score'].astype(int)
                llm_response_df['int_run_id']               = current_run_id
                llm_response_df['int_run_timestamp']        = current_run_timestamp
                llm_response_df['int_current_batch_id']     = current_batch_id
                llm_response_df['int_source_batch_id']      = -1
                llm_response_df['int_source_type']          = 'classification'
                llm_response_df['int_taxonomy_version_id']  = int_taxonomy_version_id
                llm_response_df['taxonomy_name']            = taxonomy_name
                llm_response_df['prompt']                   = input_prompt
                llm_response_df['llm']                      = "gemini-1.5-flash-002"

                # Output the concatenated DataFrame to a CSV file
                output_path = Path(ctx.obj.dir_out) / f"paper_{row['id']}_llm_response_df_final.csv"
                utils_data.write_df_to_csv(llm_response_df, output_path)

                # write to results table
                try:
                    utils_psycopb.df_to_table2(conn, llm_response_df, "marsol_dev_01.cdp_data_asset_cls_result")
                except Exception as e:
                    ctx.obj.logger.error(f"extract_from_asset: Error writing concatenated_df to table: {e}")
                    conn.rollback()

                # now update taxonomy with value level 04 id's identified

def get_taxonomy_entries(ctx):

    """
    This routine
        - given a query with classification results that came from an initial classification
        - suggest new entries to the taxonomy.

        - read control file - get query_or_view_name,taxonomy_name,prompt_name,llm_name 
        - loops over asset text and passes for classification
        - a taxonomy is specified to use to perform classification
        - a prompt is specified to use to perform classification
        - the asset text is passed to the LLM function
        - the response is written to a file
        - the response is converted to a DataFrame
        - the DataFrame is written to a file
        - the DataFrame is written to a SQL table
    """

    ctx.obj.logger.info("ENTER EXTRACT FROM CONTENT")

    # Connect to the database
    with utils_psycopb.connect(ctx.obj.logger) as conn:
        with conn.cursor() as cur:

            # get a new run id and pass to functions for insert.
            current_run_id = utils_psycopb.get_next_seq_id(conn, cdp_data_ctl_globals.sequence_run_id)
            current_run_timestamp = pd.Timestamp.now()
            ctx.obj.logger.info(f"this run_id: {current_run_id}")

            # ============================================================
            # Get keyword data
            # ============================================================

            cur.execute(utils_cdp_data_ctl_extract_sql.sql_get_keywords, )
            keywords_df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
            message = "number of keywords for processing :" + str(len(keywords_df))
            ctx.obj.logger.info(message)
            print(message)
            ctx.obj.logger.info('\n' + str(keywords_df))

            # ============================================================
            # Get TAXONOMY
            # ============================================================
            # read taxonomy from star view
            # what is taxonomy_version_id to use?

            #taxonomy_name = 'data/import/tech.taxonomy.claude.001.csv'
            #taxonomy_name = 'taxonomy.version.02.for.initial.upload.csv'
            taxonomy_name = 'taxonomy.version.02.for.update.csv'
            taxonomy_version = 1
            cur.execute(utils_cdp_data_ctl_extract_sql.sql_get_taxonomy, (taxonomy_name, taxonomy_version,))
            taxonomy_df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
            ctx.obj.logger.info("Taxonomy data loaded")
            ctx.obj.logger.info(taxonomy_df)
            output_path = Path(ctx.obj.dir_out) / "taxonomy_df.csv"
            utils_data.write_df_to_csv(taxonomy_df, output_path)

            # ============================================================
            # SET PROMPT
            # ============================================================
            #input_prompt="prompt_classification_taxonomy_03"
            input_prompt="prompt_suggest_new_taxonomy_entries_01"

            # construct prompt
            prompt = utils_cdp_data_ctl_prompt_taxonomy_entries.get_prompt_text(input_prompt, keywords_df, taxonomy_df)
            # Write the prompt to a file
            prompt_output_path = Path(ctx.obj.dir_out) / f"prompt.txt"
            with open(prompt_output_path, 'w', encoding='utf-8') as f:
                f.write(prompt)

            #ctx.obj.logger.info(prompt)
            llm_response_text = utils_cdp_data_ctl_llm.call_gemini(ctx, prompt)

            # ============================================================
            # Output RAW llm_response_text to file
            # ============================================================
            output_path = Path(ctx.obj.dir_out) / f"llm_response_text.txt"
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(llm_response_text)

            # ============================================================
            # Attempt conversion to DF
            # ============================================================

            # Find and extract the TSV content
            lines = llm_response_text.strip().split('\n')
            ctx.obj.logger.info(f"Type of lines: {type(lines)}")
            ctx.obj.logger.info(f"Number of lines: {len(lines)}")

            # Filter out any lines that don't look like TSV data
            tsv_lines_true  = []
            tsv_lines_false = []
            for line in lines:
                if '\t' in line:  # Only keep lines with tabs
                    tab_count = line.count('\t')
                    if tab_count != 8:
                        tsv_lines_false.append(line)
                    else:
                        tsv_lines_true.append(line)
                    ctx.obj.logger.info(f"Line: {line} | Tab count: {tab_count}")
                else:
                    tsv_lines_false.append(line)
                
            ctx.obj.logger.info(f"Number of valid TSV lines: {len(tsv_lines_true)}")
            ctx.obj.logger.info(f"Number of invalid TSV lines: {len(tsv_lines_false)}")

            # loop over tsv_lines_true and write to dataframe
            # Initialize an empty DataFrame with the specified column names
            llm_response_df = pd.DataFrame(columns=['input_technology_term', 'input_matching_taxonomy_path', 'input_notes', 'output_entry_type', 'output_level_05_entry', 'output_level_04_parent', 'output_level_04_id', 'output_notes', 'output_confidence_score'])

            # Loop over each line in tsv_lines_true and append to the DataFrame
            for line in tsv_lines_true:
                # Split the line by tab character
                values = line.split('\t')
                # Append the values as a new row to the DataFrame
                llm_response_df = pd.concat([llm_response_df, pd.DataFrame([values], columns=llm_response_df.columns)], ignore_index=True)
               
            output_path = Path(ctx.obj.dir_out) / f"llm_response_df.csv"
            utils_data.write_df_to_csv(llm_response_df, output_path)

