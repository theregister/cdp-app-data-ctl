
from    pathlib        import  Path

import  pandas                                              as pd

import  lib.postgres.utils_psycopb                          as  utils_psycopb

import  lib.data.utils_data                                 as  utils_data

import  utils_cdp_app_data_ctl_prompt
import  cdp_app_data_ctl_globals                            as  cdp_app_data_ctl_globals
import  utils_cdp_app_data_ctl_project_insights_sql         as  utils_cdp_app_data_ctl_project_insights_sql
import  utils_cdp_app_data_ctl_llm                          as  utils_cdp_data_ctl_llm

def generate(ctx):
    """
    This routine does the following:
    1. Connects to the database
    2. Gets the projects to be processed - for now these are going to be campaign insights projects
    3. For each project, it gets the project data - need to determine exactly what this is
    4. For each project, have LLM generate the appropriate insights
    5. Write the results to the database
    """

    print("generate the project insights")

    ctx.obj.logger.info("ENTER GENERATE - project insights")

    # Connect to the database
    with utils_psycopb.connect(ctx.obj.logger) as conn:

        with conn.cursor() as cur:

            # ============================================================
            # get a new run id/timestamp
            # ============================================================
            current_run_id = utils_psycopb.get_next_seq_id(conn, cdp_app_data_ctl_globals.sequence_run_id)
            current_run_timestamp = pd.Timestamp.now()
            ctx.obj.logger.info(f"this run_id: {current_run_id}")

            # ============================================================
            # Get ASSET DATA
            # ============================================================
            cur.execute(utils_cdp_app_data_ctl_project_insights_sql.get_projects_for_insight_generation, )
            project_df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
            message = "number of assets selected for processing :" + str(len(project_df))
            ctx.obj.logger.info(message)
            print(message)
            ctx.obj.logger.info('\n' + str(project_df))

            # ============================================================
            # LOOP through the Asset Dataframe rows
            # ============================================================
            for index, row in project_df.iterrows():

                current_project_id      = row['project_id']
                current_project_name    = row['name']

                # get new batch_id
                current_batch_id = utils_psycopb.get_next_seq_id(conn, "cdp_app_batch_id_seq")
                ctx.obj.logger.info(f"this batch_id: {current_batch_id}")

                ctx.obj.logger.info("=============================================================")
                ctx.obj.logger.info(str(current_project_id) + str(current_project_name) )
                ctx.obj.logger.info("=============================================================")

                output_path = Path(ctx.obj.dir_out) / f"paper_{current_project_id}.csv"
                row_df = pd.DataFrame([row])
                utils_data.write_df_to_csv(row_df, output_path)

                # ============================================================
                # get input data  - project leads data
                # ============================================================
                # for the project, get the campaign, assets, leads etc.
                cur.execute(utils_cdp_app_data_ctl_project_insights_sql.get_project_leads_data, (current_project_id,))
                project_leads_df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])

                ctx.obj.logger.info("project leads data loaded")
                ctx.obj.logger.info(project_leads_df)
                output_path = Path(ctx.obj.dir_out) / "project_leads_df.csv"
                utils_data.write_df_to_csv(project_leads_df, output_path)
            
                # ============================================================
                # construct prompt
                # ============================================================
                input_prompt_type = 'campaign_insights'
                input_prompt_name = 'campaign_insights_20250528_01'
                prompt = utils_cdp_app_data_ctl_prompt.get_prompt_text_campaign_insights(input_prompt_type, input_prompt_name, project_leads_df)
                prompt_output_path = Path(ctx.obj.dir_out) / f"project_{current_project_id}_prompt.txt"
                with open(prompt_output_path, 'w', encoding='utf-8') as f:
                    f.write(prompt)

                # call LLM
                llm_response_text = utils_cdp_data_ctl_llm.call_gemini(ctx, prompt)

                # ============================================================
                # Output RAW llm_response_text to file
                # ============================================================
                output_path = Path(ctx.obj.dir_out) / f"project_{current_project_id}_llm_response_text.txt"
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
                expected_tab_count = 1 # 2 columns so tab count is 1

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

                llm_response_df = pd.DataFrame(columns=['section_name', 'section_content'])
                # Skip the first line (assumed to be the header)
                for line in tsv_lines_true[1:]:
                    # Split the line by tab character
                    values = line.split('\t')
                    # Append the values as a new row to the DataFrame
                    llm_response_df = pd.concat([llm_response_df, pd.DataFrame([values], columns=llm_response_df.columns)], ignore_index=True)
                # output llm response df to csv and table
                output_path = Path(ctx.obj.dir_out) / f"project_{row['project_id']}_llm_response_df.csv"
                utils_data.write_df_to_csv(llm_response_df, output_path)

                # add a column for project
                llm_response_df['project_id'] = current_project_id

                #print("output to df to csv")
                # Output the llm_response_df to a CSV file
                output_path = Path(ctx.obj.dir_out) / f"paper_{current_project_id}_llm_response_df.csv"
                utils_data.write_df_to_csv(llm_response_df, output_path)

                #print(llm_response_df['relevance_score'])
                #llm_response_df['cls_confidence_score']     = pd.to_numeric(llm_response_df['cls_confidence_score'], errors='coerce').fillna(-1).astype(int)
                #llm_response_df['cls_relevance_score']      = pd.to_numeric(llm_response_df['cls_relevance_score'], errors='coerce').fillna(-1).astype(int)
                llm_response_df['int_run_id']               = current_run_id
                llm_response_df['int_run_timestamp']        = current_run_timestamp
                llm_response_df['int_current_batch_id']     = current_batch_id
                llm_response_df['int_source_batch_id']      = -1
                llm_response_df['int_source_type']          = 'content_insights'
                llm_response_df['prompt_type']              = 'content_insights'
                llm_response_df['prompt_name']              = input_prompt_name
                llm_response_df['llm_name']                 = "gemini-1.5-flash-002"

                # Output the concatenated DataFrame to a CSV file
                output_path = Path(ctx.obj.dir_out) / f"project_{current_project_id}_llm_response_df_final.csv"
                utils_data.write_df_to_csv(llm_response_df, output_path)

                # write to results table
                try:
                    utils_psycopb.df_to_table2(conn, llm_response_df, "cdp_app_project_insight_result")
                except Exception as e:
                    ctx.obj.logger.error(f"project insights: Error writing llm_response_df to table: {e}")
                    conn.rollback()
