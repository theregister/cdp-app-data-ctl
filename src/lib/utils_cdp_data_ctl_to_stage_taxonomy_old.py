
import  pandas                                  as pd
import  lib.postgres.utils_psycopb              as utils_psycopb
#import  utils_cdp_data                         as utils_cdp_data

from    pathlib                                 import  Path

import  lib.data.utils_data                     as utils_data
import  utils_cdp_data_ctl                     as utils_cdp_data_ctl


import  cdp_data_ctl_globals                   as cdp_data_ctl_globals

from    bs4         import BeautifulSoup

def load_to_stage(ctx):

    try:
        connection = utils_psycopb.connect(ctx.obj.logger)
    except Exception as e:
        print("ERROR in connection")
    
    # get a new run id and pass to functions for insert.
    current_run_id = utils_psycopb.get_next_seq_id(connection, cdp_data_ctl_globals.sequence_run_id)
    ctx.obj.logger.info(f"Next sequence value: {current_run_id}")

    # load control file data from csv file
    print("LOAD TAXONOMY DATA control file to DF")
    path=Path(ctx.obj.dir_in) / "cdp_data_taxonomy_load_to_stage.csv"
    import_df = utils_data.load_csv_to_df(path, ctx.obj.logger)
    ctx.obj.logger.info("taxonomy import control file")
    ctx.obj.logger.info(import_df)

    # loop and perform imports
    for index, row in import_df.iterrows():
        print("ITERATING LOAD CONTROL FILE DATA - START ===============================================")
        print(Path(row['file_name']))
        if Path(row['file_name']).suffix == ".csv":
            process_csv_file(ctx, connection, row, current_run_id)
        elif Path(row['file_name']).suffix == ".html":
            process_html_file(ctx, connection, row, current_run_id)
        print("ITERATING LOAD CONTROL FILE DATA - END ===============================================")

def process_html_file(ctx, connection, row, current_run_id):

    file_name = row['file_name']
    print(row['file_name'])
    path=Path(ctx.obj.dir_in) / f"../data/import/{file_name}"

    if file_name == "gartner.magic.quadrant.critical.capabilities.20241202.html":
        process_mq(ctx, connection, row)
    elif file_name == "gartner_magic_quadrant_critical_capabilities.20241202.html":
        print("Processing: " + file_name)
        process_mq(ctx, connection, row)
    else:
        print(f"FILE NOT PROCESSED: {file_name}")

def process_csv_file(ctx, connection, row, current_run_id):

    file_name = row['file_name']
    print(row['file_name'])
    # read the data into dataframe
    path=Path(ctx.obj.dir_in) / f"../data/import/{file_name}"
    print(path)
    import_df = utils_data.load_csv_to_df(path, ctx.obj.logger)

    if file_name == "vendors_and_their_competitors.csv":
        process_vendors_and_their_competitors(ctx, connection, import_df, current_run_id)
    elif file_name == "us.accounts.csv":
        print("Processing: " + file_name)
    elif file_name == "test.taxonomy.001.csv":
        print("Processing: " + file_name)
        process_tech_taxonomy(ctx, file_name, connection, import_df, current_run_id)
    elif file_name == "test.taxonomy.002.csv":
        print("Processing: " + file_name)
        process_tech_taxonomy(ctx, file_name, connection, import_df, current_run_id)
    elif file_name == "gartner_magic_quadrant_critical_capabilities.20241202.html":
        print("Processing: " + file_name)
        process_mq(ctx, connection, import_df, current_run_id)
    elif file_name == "tech.taxonomy.gartner.claude.csv":
        print("Processing: " + file_name)
        process_tech_taxonomy_gartner_claude(ctx, connection, import_df, current_run_id)
    elif file_name == "test.tech.taxonomy.claude.001.csv":
        print("Processing: " + file_name)
        process_tech_taxonomy(ctx, file_name, connection, import_df, current_run_id)
    elif file_name == "taxonomy.version.02.for.initial.upload.csv":
        print("Processing: " + file_name)
        process_tech_taxonomy(ctx, file_name, connection, import_df, current_run_id)
    else:
        print(f"FILE NOT PROCESSED: {file_name}")

def process_vendors_and_their_competitors(ctx, connection, import_df, current_run_id):

    # Specific processing for file1.csv
    print("Processing vendors_and_their_competitors.csv")
    # set column names
    import_df.columns = ['technology', 'vendor']
    # clean - strip leading and trailing whitespace from all string columns
    import_df = import_df.map(lambda x: x.strip() if isinstance(x, str) else x)
    # clean - replace newline characters in 'level_05_leaf' and 'level_05_description' columns
    import_df['technology']             = import_df['technology'].str.replace('\n', ' ')
    import_df['vendor']                 = import_df['vendor'].str.replace('\n', ' ')

    # process and output
    # load unique report_title as a set of technology values
    technology_set = set(import_df['technology'])
    # Convert the unqiue technology set into a dataframe column
    output_df_tech = pd.DataFrame(list(technology_set), columns=['level_05_leaf'])
    #print(import_df)
    current_batch_id = utils_psycopb.get_next_seq_id(connection, cdp_data_ctl_globals.sequence_batch_id)

    # add other columns
    output_df_tech['level_01']                  = 'Enterprise Technology'
    output_df_tech['level_02']                  = ''
    output_df_tech['level_03']                  = ''
    output_df_tech['level_04']                  = ''
    output_df_tech['level_05_description']      = ''
    output_df_tech['int_current_batch_id']      = current_batch_id
    output_df_tech['int_run_id']                = current_run_id
    output_df_tech['int_run_timestamp']         = pd.Timestamp.now()
    #output_df_tech['int_created_by']            = 'import classification-data'
    output_df_tech['data_type']                 = 'values_technology'
    output_df_tech['int_source_type']           = 'SPREADSHEET'
    output_df_tech['int_source_system']         = 'Phil Spreadsheet - 18-Nov-2024'
    output_df_tech['int_source_object']         = 'File System'
    output_df_tech['int_source_id']             = 'data/import/vendors_and_their_competitors.csv'
    output_df_tech['int_source_batch_id']       = -1

    # output to file
    utils_data.write_df_to_csv(output_df_tech, Path(ctx.obj.dir_out) / "import_df_tech.csv")
    # write to db
    utils_psycopb.df_to_table(connection, output_df_tech, "cdp_data_stg_taxonomy")

    vendor_set = set(import_df['vendor'])
    # Convert the unqiue technology set into a dataframe column
    output_df_vendor = pd.DataFrame(list(vendor_set), columns=['level_05_leaf'])
    current_batch_id = utils_psycopb.get_next_seq_id(connection, cdp_data_ctl_globals.sequence_batch_id)

    # add other columns
    output_df_vendor['level_01']                    = 'Enterprise Technology Vendors'
    output_df_vendor['level_02']                    = ''
    output_df_vendor['level_03']                    = ''
    output_df_vendor['level_04']                    = ''
    output_df_vendor['level_05_description']        = ''
    output_df_vendor['int_current_batch_id']        = current_batch_id
    output_df_vendor['int_run_id']                  = current_run_id
    output_df_vendor['int_run_timestamp']           = pd.Timestamp.now()
    #output_df_vendor['int_created_by']              = 'import classification-data'
    output_df_vendor['data_type']                   = 'values_vendor'
    output_df_vendor['int_source_type']             = 'SPREADSHEET'
    output_df_vendor['int_source_system']           = 'Phil Spreadsheet - 18-Nov-2024'
    output_df_vendor['int_source_object']           = 'File System'
    output_df_vendor['int_source_id']               = 'data/import/vendors_and_their_competitors.csv'
    output_df_vendor['int_source_batch_id']         = -1

    # output to file
    utils_data.write_df_to_csv(output_df_vendor, Path(ctx.obj.dir_out) / "output_df_vendor.csv")
    # write to db
    utils_psycopb.df_to_table(connection, output_df_vendor, "cdp_data_stg_taxonomy")

def process_tech_taxonomy_gartner_claude(ctx, connection, import_df, current_run_id):
     
    ctx.obj.logger.info("process_test_taxonomy_002: " + str(connection.connection.closed))
    ctx.obj.logger.info(import_df)

    # set column names
    import_df.columns = ['level_02', 'level_03', 'level_04', 'level_05_leaf', 'level_05_description']

    current_batch_id = utils_psycopb.get_next_seq_id(connection, cdp_data_ctl_globals.sequence_batch_id)

    output_df = import_df

    # add other columns
    output_df['int_run_id']             = current_run_id
    output_df['level_01']               = 'Enterprise Technology'
    output_df['int_current_batch_id']   = current_batch_id
    output_df['int_run_timestamp']      = pd.Timestamp.now()
    #output_df['int_created_by']         = 'import classification-data'
    output_df['data_type']              = 'taxonomy_technology_multi_parent'
    output_df['int_source_type']        = 'SPREADSHEET'
    output_df['int_source_system']      = 'Gartner Claude Merge - L2:Top-level category/L3:Sub-category/L4:tech area/L5:tech solution type'
    output_df['int_source_object']      = 'File System'
    output_df['int_source_id']          = 'data/import/tech.taxonomy.gartner.claude.csv'
    output_df['int_source_batch_id']    = -1

    # output to file
    utils_data.write_df_to_csv(output_df, Path(ctx.obj.dir_out) / "output.df.tech.taxonomy.gartner.claude.csv")
    # write to db
    utils_psycopb.df_to_table(connection, output_df, "cdp_data_stg_taxonomy")

def process_mq(ctx, connection, row):

    file_name = row['file_name']
    print(row['file_name'])
    file_path=Path(ctx.obj.dir_in) / f"../data/import/{file_name}"

    # Load the HTML file
    #file_path = file_name
    with open(file_path, 'r', encoding='utf-8') as file:
        html_content = file.read()

    # Parse the HTML content with BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')

    # Find the publication table (update 'your_table_class_or_id' with the actual identifier)
    publication_table = soup.find('table', {'class': 'publication-table'})  # Replace with correct identifier

    # Extract rows and columns
    if publication_table:
        rows = publication_table.find_all('tr')
        table_data = []
        for row in rows:
            cells = row.find_all(['th', 'td'])
            table_data.append([cell.get_text(strip=True) for cell in cells])
            # Print or process the table data
            new_df = pd.DataFrame(columns=['Column1', 'Column2', 'Column3'])
            for row in table_data:
                if len(row) == 3:
                    # Remove "Magic Quadrant" or "Critical Capabilities" from the data in column 1
                    row[0] = row[0].replace("Magic Quadrant", "").replace("Critical Capabilities", "").strip()
                    print(row)
                    new_row = pd.DataFrame([row], columns=['Column1', 'Column2', 'Column3'])
                    new_df = pd.concat([new_df, new_row], ignore_index=True)
                else:
                    print("NOT PROCESSED")
                    print(row)
            print(new_df)
            # Output the new DataFrame to a CSV file
            utils_data.write_df_to_csv(new_df, Path(ctx.obj.dir_out) / "output_df_mq.csv")
            # Extract the first column with unique values

            unique_first_column_df = new_df[['Column1']].drop_duplicates().reset_index(drop=True)
            unique_first_column_df['id'] = unique_first_column_df.index + 1

            # Output the unique first column DataFrame to a CSV file
            utils_data.write_df_to_csv(unique_first_column_df, Path(ctx.obj.dir_out) / "output_df_mq_unique.csv")
    else:
        print("Publication table not found.")
        # Print a summary of the parsed HTML
        print("\nSummary of the parsed HTML:")
        print(f"Total number of tables: {len(soup.find_all('table'))}")
        print(f"Total number of rows in the publication table: {len(rows) if publication_table else 0}")
        print(f"Total number of columns in the first row: {len(rows[0].find_all(['th', 'td'])) if rows else 0}")

def process_test_taxonomy_001(ctx, connection, import_df, current_run_id):
     
    ctx.obj.logger.info("process_test_taxonomy_001: " + str(connection.connection.closed))
    ctx.obj.logger.info(import_df)

    # set column names
    import_df.columns = ['level_01', 'level_02', 'level_03', 'level_04', 'level_05_leaf', 'level_05_description']

    current_batch_id = utils_psycopb.get_next_seq_id(connection, cdp_data_ctl_globals.sequence_batch_id)

    output_df = import_df

    # add other columns
    output_df['int_run_id']             = current_run_id
    output_df['int_current_batch_id']   = current_batch_id
    output_df['int_run_timestamp']      = pd.Timestamp.now()
    #output_df['int_created_by']         = 'import classification-data'
    output_df['data_type']              = 'taxonomy_test'
    output_df['int_source_type']        = 'SPREADSHEET'
    output_df['int_source_system']      = 'test'
    output_df['int_source_object']      = 'File System'
    output_df['int_source_id']          = 'data/import/test.taxonomy.001.csv'
    output_df['int_source_batch_id']    = -1

    # output to file
    utils_data.write_df_to_csv(output_df, Path(ctx.obj.dir_out) / "output.df.test.taxonomy.001.csv")
    # write to db
    utils_psycopb.df_to_table(connection, output_df, "cdp_data_stg_taxonomy")

def process_test_taxonomy_002(ctx, connection, import_df, current_run_id):
     
    ctx.obj.logger.info("process_test_taxonomy_002: " + str(connection.connection.closed))
    ctx.obj.logger.info(import_df)

    # set column names
    import_df.columns = ['level_01', 'level_02', 'level_03', 'level_04', 'level_05_leaf', 'level_05_description']

    current_batch_id = utils_psycopb.get_next_seq_id(connection, cdp_data_ctl_globals.sequence_batch_id)

    output_df = import_df

    # add other columns
    output_df['int_run_id']                 = current_run_id
    output_df['int_current_batch_id']       = current_batch_id
    output_df['int_run_timestamp']          = pd.Timestamp.now()
    #output_df['int_created_by']             = 'import classification-data'
    output_df['data_type']                  = 'taxonomy_test'
    output_df['int_source_type']            = 'SPREADSHEET'
    output_df['int_source_system']          = 'test'
    output_df['int_source_object']          = 'File System'
    output_df['int_source_id']              = 'data/import/test.taxonomy.002.csv'
    output_df['int_source_batch_id']        = -1

    # output to file
    utils_data.write_df_to_csv(output_df, Path(ctx.obj.dir_out) / "output.df.test.taxonomy.002.csv")
    # write to db
    utils_psycopb.df_to_table(connection, output_df, "cdp_data_stg_taxonomy")

def process_tech_taxonomy_claude(ctx, connection, import_df, current_run_id):
    
    ctx.obj.logger.info("process_tech_taxonomy_claude_01: " + str(connection.connection.closed))
    ctx.obj.logger.info(import_df)
    ctx.obj.logger.info("length of import_df: " + str(len(import_df.columns)))
    
    # Check the number of columns in the dataframe
    if len(import_df.columns) == 6:
        import_df.columns = ['level_01', 'level_02', 'level_03', 'level_04', 'level_05_leaf', 'level_05_description']
    elif len(import_df.columns) == 5:
        import_df.columns = ['level_01', 'level_02', 'level_03', 'level_04', 'level_05_leaf']
    current_batch_id = utils_psycopb.get_next_seq_id(connection, cdp_data_ctl_globals.sequence_batch_id)

    output_df = import_df
    print(output_df)

    # add other columns
    output_df['int_run_id']                 = current_run_id
    output_df['int_current_batch_id']       = current_batch_id
    output_df['int_run_timestamp']          = pd.Timestamp.now()
    #output_df['int_created_by']             = 'import classification-data'
    output_df['data_type']                  = 'taxonomy_technology'
    output_df['int_source_type']            = 'SPREADSHEET'
    output_df['int_source_system']          = 'Claude'
    output_df['int_source_object']          = 'File System'
    output_df['int_source_id']              = 'data/import/tech.taxonomy.claude.001.csv'
    output_df['int_source_batch_id']        = -1
   
    # output to file
    utils_data.write_df_to_csv(output_df, Path(ctx.obj.dir_out) / "output.df.tech.taxonomy.claude.001.csv")
    # write to db
    utils_psycopb.df_to_table2(connection, output_df, "cdp_data_stg_taxonomy")

def process_tech_taxonomy(ctx, file_name, connection, import_df, current_run_id):
    
    ctx.obj.logger.info("process_tech_taxonomy: ")
    ctx.obj.logger.info(import_df)
    ctx.obj.logger.info("length of import_df: " + str(len(import_df.columns)))
    
    # Check the number of columns in the dataframe
    if len(import_df.columns) == 6:
        import_df.columns = ['level_01', 'level_02', 'level_03', 'level_04', 'level_05_leaf', 'level_05_description']
    elif len(import_df.columns) == 5:
        import_df.columns = ['level_01', 'level_02', 'level_03', 'level_04', 'level_05_leaf']
    current_batch_id = utils_psycopb.get_next_seq_id(connection, cdp_data_ctl_globals.sequence_batch_id)

    output_df = import_df
    print(output_df)

    # add other columns
    output_df['int_run_id']                 = current_run_id
    output_df['int_current_batch_id']       = current_batch_id
    output_df['int_run_timestamp']          = pd.Timestamp.now()
    output_df['data_type']                  = 'taxonomy_technology'
    output_df['int_source_type']            = 'SPREADSHEET'
    output_df['int_source_system']          = 'Claude'
    output_df['int_source_object']          = 'File System'
    output_df['int_source_id']              = file_name
    output_df['int_source_batch_id']        = -1
   
    # output to file
    utils_data.write_df_to_csv(output_df, Path(ctx.obj.dir_out) / file_name)
    # write to db
    utils_psycopb.df_to_table2(connection, output_df, "cdp_data_stg_taxonomy")