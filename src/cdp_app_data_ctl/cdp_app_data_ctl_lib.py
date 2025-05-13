
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

import  lib.data.utils_data              as utils_data
import  lib.postgres.utils_psycopb       as utils_psycopb

import  utils_cdp_data_ctl               as utils_cdp_data_ctl

import  cdp_data_ctl_globals            as cdp_data_ctl_globals

#import AI.AI_03.enttech      as EntityTechNER

import  vertexai
from    vertexai.generative_models import GenerativeModel

def load_extraction_data(data, conn_string):
    """
    Loads the given data into a PostgreSQL database.

    Args:
        data: The data to load, a list of dictionaries.
        conn_string: The connection string to the database.
    """

    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()

    # Create tables if they don't exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            article_id SERIAL PRIMARY KEY,
            title TEXT
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS keywords (
            keyword_id SERIAL PRIMARY KEY,
            keyword_or_term TEXT,
            type TEXT,
            relevance_score INTEGER
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS article_keywords (
            article_id INTEGER REFERENCES articles(article_id),
            keyword_id INTEGER REFERENCES keywords(keyword_id),
            PRIMARY KEY (article_id, keyword_id)
        );
    """)

    # Insert data into tables
    for article in data:
        cur.execute("INSERT INTO articles (title) VALUES (%s) RETURNING article_id", (article['title'],))
        article_id = cur.fetchone()[0]

        for keyword in article['keywords']:
            cur.execute("INSERT INTO keywords (keyword_or_term, type, relevance_score) VALUES (%s, %s, %s) RETURNING keyword_id",
                        (keyword['keyword_or_term'], keyword['type'], keyword['relevance_score']))
            keyword_id = cur.fetchone()[0]

            cur.execute("INSERT INTO article_keywords (article_id, keyword_id) VALUES (%s, %s)", (article_id, keyword_id))

    conn.commit()
    cur.close()
    conn.close()

def extract_entities_ner(text: str):

    #-> Dict[str, List[str]]:
    """Example usage of EnhancedTechNER"""
    try:
        # Initialize the NER system
        ner = EntityTechNER.EnhancedTechNER()

        # Example text
        #sample_text = """
        #The company is migrating their infrastructure to AWS Cloud and using 
        #Azure Functions for serverless computing. They use SQL Server 2019 
        #for database management and are implementing TensorFlow for their 
        #machine learning initiatives. The development team primarily uses 
        #Visual Studio Code for their .NET development work.
        #Their containerization strategy involves using Docker and Kubernetes
        #for orchestration.
        #"""
        
        # Analyze the text
        results = ner.analyze_text(text)
        
        # Print results
        print("Technology Analysis Results:")
        print("=" * 50)
        
        print("\nTechnologies by Category:")
        for category, techs in results["categories"].items():
            print(f"\n{category}:")
            for tech in techs:
                print(f"- {tech['technology']} (mentioned as: {tech['mentioned_as']})")
                if tech['vendor']:
                    print(f"  Vendor: {tech['vendor']}")
        
        print("\nVendors Mentioned:")
        for vendor in results["vendors"]:
            print(f"- {vendor}")
        
        print("\nDetailed Technology Information:")
        for tech_name, tech_info in results["technologies"].items():
            print(f"\n{tech_name}:")
            print(f"  Category: {tech_info['category']}")
            print(f"  Vendor: {tech_info['vendor']}")
            print(f"  Mentions: {', '.join(tech_info['mentions'])}")
            if tech_info['related_terms_found']:
                print(f"  Related Terms Found: {', '.join(tech_info['related_terms_found'])}")
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")

def process_vendors_and_their_competitors(ctx, connection, import_df):

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
    load_id = utils_psycopb.get_next_seq_id(connection, "cdp_data_import_load_id_seq")
    ctx.obj.logger.info(f"Next sequence value: {load_id}")

    # add other columns
    output_df_tech['level_01']               = 'Enterprise Technology'
    output_df_tech['level_02']               = ''
    output_df_tech['level_03']               = ''
    output_df_tech['level_04']               = ''
    output_df_tech['level_05_description']   = ''
    output_df_tech['current_data_load_id']           = load_id
    output_df_tech['data_load_timestamp']    = pd.Timestamp.now()
    output_df_tech['created_by']             = 'import classification-data'
    output_df_tech['data_type']              = 'values_technology'
    output_df_tech['source_system']          = 'Phil Spreadsheet - 18-Nov-2024'
    output_df_tech['source_object']          = 'File System'
    output_df_tech['source_id']              = 'data/import/vendors_and_their_competitors.csv'
    output_df_tech['source_data_load_id']    = -1

    # output to file
    utils_data.write_df_to_csv(output_df_tech, Path(ctx.obj.dir_out) / "import_df_tech.csv")
    # write to db
    utils_psycopb.df_to_table(connection, output_df_tech, "marsol_dev_01.cdp_data_stg_taxonomy")

    vendor_set = set(import_df['vendor'])
    # Convert the unqiue technology set into a dataframe column
    output_df_vendor = pd.DataFrame(list(vendor_set), columns=['level_05_leaf'])
    load_id = utils_psycopb.get_next_seq_id(connection, "cdp_data_import_load_id_seq")
    print(f"Next sequence value: {load_id}")

    # add other columns
    output_df_vendor['level_01']               = 'Enterprise Technology Vendors'
    output_df_vendor['level_02']               = ''
    output_df_vendor['level_03']               = ''
    output_df_vendor['level_04']               = ''
    output_df_vendor['level_05_description']   = ''
    output_df_vendor['current_data_load_id']           = load_id
    output_df_vendor['data_load_timestamp']    = pd.Timestamp.now()
    output_df_vendor['created_by']             = 'import classification-data'
    output_df_vendor['data_type']              = 'values_vendor'
    output_df_vendor['source_system']          = 'Phil Spreadsheet - 18-Nov-2024'
    output_df_vendor['source_object']          = 'File System'
    output_df_vendor['source_id']              = 'data/import/vendors_and_their_competitors.csv'
    output_df_vendor['source_data_load_id']    = -1

    # output to file
    utils_data.write_df_to_csv(output_df_vendor, Path(ctx.obj.dir_out) / "output_df_vendor.csv")
    # write to db
    utils_psycopb.df_to_table(connection, output_df_vendor, "marsol_dev_01.cdp_data_stg_taxonomy")

def process_tech_taxonomy_gartner_claude(ctx, connection, import_df):
     
    ctx.obj.logger.info("process_test_hierarchy_002: " + str(connection.connection.closed))
    ctx.obj.logger.info(import_df)

    # set column names
    import_df.columns = ['level_02', 'level_03', 'level_04', 'level_05_leaf', 'level_05_description']

    load_id = utils_psycopb.get_next_seq_id(connection, "cdp_data_import_load_id_seq")
    ctx.obj.logger.info(f"Next sequence value: {load_id}")

    output_df = import_df

    # add other columns
    output_df['level_01']               = 'Enterprise Technology'
    output_df['current_data_load_id']   = load_id
    output_df['data_load_timestamp']    = pd.Timestamp.now()
    output_df['created_by']             = 'import classification-data'
    output_df['data_type']              = 'hierarchy_technology_multi_parent'
    output_df['source_system']          = 'Gartner Claude Merge - L2:Top-level category/L3:Sub-category/L4:tech area/L5:tech solution type'
    output_df['source_object']          = 'File System'
    output_df['source_id']              = 'data/import/tech.taxonomy.gartner.claude.csv'
    output_df['source_data_load_id']    = -1

    # output to file
    utils_data.write_df_to_csv(output_df, Path(ctx.obj.dir_out) / "output.df.tech.taxonomy.gartner.claude.csv")
    # write to db
    utils_psycopb.df_to_table(connection, output_df, "marsol_dev_01.cdp_data_stg_taxonomy")

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

def process_test_hierarchy_001(ctx, connection, import_df):
     
    ctx.obj.logger.info("process_test_hierarchy_001: " + str(connection.connection.closed))
    ctx.obj.logger.info(import_df)

    # set column names
    import_df.columns = ['level_01', 'level_02', 'level_03', 'level_04', 'level_05_leaf', 'level_05_description']

    load_id = utils_psycopb.get_next_seq_id(connection, "cdp_data_import_load_id_seq")
    ctx.obj.logger.info(f"Next sequence value: {load_id}")

    output_df = import_df

    # add other columns
    output_df['current_data_load_id']           = load_id
    output_df['data_load_timestamp']    = pd.Timestamp.now()
    output_df['created_by']             = 'import classification-data'
    output_df['data_type']              = 'hierarchy_test'
    output_df['source_system']          = 'test'
    output_df['source_object']          = 'File System'
    output_df['source_id']              = 'data/import/test.hierarchy.001.csv'
    output_df['source_data_load_id']    = -1

    # output to file
    utils_data.write_df_to_csv(output_df, Path(ctx.obj.dir_out) / "output.df.test.hierarchy.001.csv")
    # write to db
    utils_psycopb.df_to_table(connection, output_df, "marsol_dev_01.cdp_data_stg_taxonomy")

def process_test_hierarchy_002(ctx, connection, import_df):
     
    ctx.obj.logger.info("process_test_hierarchy_002: " + str(connection.connection.closed))
    ctx.obj.logger.info(import_df)

    # set column names
    import_df.columns = ['level_01', 'level_02', 'level_03', 'level_04', 'level_05_leaf', 'level_05_description']

    load_id = utils_psycopb.get_next_seq_id(connection, "cdp_data_import_load_id_seq")
    ctx.obj.logger.info(f"Next sequence value: {load_id}")

    output_df = import_df

    # add other columns
    output_df['current_data_load_id']           = load_id
    output_df['data_load_timestamp']    = pd.Timestamp.now()
    output_df['created_by']             = 'import classification-data'
    output_df['data_type']              = 'hierarchy_test'
    output_df['source_system']          = 'test'
    output_df['source_object']          = 'File System'
    output_df['source_id']              = 'data/import/test.hierarchy.002.csv'
    output_df['source_data_load_id']    = -1

    # output to file
    utils_data.write_df_to_csv(output_df, Path(ctx.obj.dir_out) / "output.df.test.hierarchy.002.csv")
    # write to db
    utils_psycopb.df_to_table(connection, output_df, "marsol_dev_01.cdp_data_stg_taxonomy")

def process_tech_hierarchy_claude(ctx, connection, import_df):
    ctx.obj.logger.info("process_tech_hierarchy_claude_01: " + str(connection.connection.closed))
    ctx.obj.logger.info(import_df)

    # Check the number of columns in the dataframe
    if len(import_df.columns) == 6:
        import_df.columns = ['level_01', 'level_02', 'level_03', 'level_04', 'level_05_leaf', 'level_05_description']
    elif len(import_df.columns) == 5:
        import_df.columns = ['level_01', 'level_02', 'level_03', 'level_04', 'level_05_leaf']
    load_id = utils_psycopb.get_next_seq_id(connection, "cdp_data_import_load_id_seq")
    ctx.obj.logger.info(f"Next sequence value: {load_id}")

    output_df = import_df

    # add other columns
    output_df['current_data_load_id']           = load_id
    output_df['data_load_timestamp']    = pd.Timestamp.now()
    output_df['created_by']             = 'import classification-data'
    output_df['data_type']              = 'hierarchy_technology'
    output_df['source_system']          = 'Claude'
    output_df['source_object']          = 'File System'
    output_df['source_id']              = 'data/import/tech.hierarchy.claude.001.csv'
    output_df['source_data_load_id']    = -1

    # output to file
    utils_data.write_df_to_csv(output_df, Path(ctx.obj.dir_out) / "output.df.tech.hierarchy.claude.001.csv")
    # write to db
    utils_psycopb.df_to_table(connection, output_df, "marsol_dev_01.cdp_data_stg_taxonomy")

def import_classification_data(ctx):

    connection = utils_psycopb.connect(ctx.obj.logger)
    ctx.obj.logger.info("import_classification_data: " + str(connection.connection.closed))

    # load control file data from csv file
    print("LOAD TAXONOMY DATA control file to DF")
    path=Path(ctx.obj.dir_in) / "scc_import_classification_data.csv"
    import_df = utils_data.load_csv_to_df(path, ctx.obj.logger)

    # loop and perform imports
    for index, row in import_df.iterrows():

        if Path(row['file_name']).suffix == ".csv":
            print("processing csv")
            process_csv_file(ctx, connection, row)
        elif Path(row['file_name']).suffix == ".html":
            print("processing html")
            process_html_file(ctx, connection, row)

def process_html_file(ctx, connection, row):

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

def process_csv_file(ctx, connection, row):

    file_name = row['file_name']
    print(row['file_name'])
    # read the data into dataframe
    path=Path(ctx.obj.dir_in) / f"../data/import/{file_name}"
    import_df = utils_data.load_csv_to_df(path, ctx.obj.logger)

    if file_name == "vendors_and_their_competitors.csv":
        process_vendors_and_their_competitors(ctx, connection, import_df)
    elif file_name == "us.accounts.csv":
        print("Processing: " + file_name)
    elif file_name == "test.hierarchy.001.csv":
        print("Processing: " + file_name)
        process_test_hierarchy_001(ctx, connection, import_df)
    elif file_name == "test.hierarchy.002.csv":
        print("Processing: " + file_name)
        process_test_hierarchy_002(ctx, connection, import_df)
    elif file_name == "gartner_magic_quadrant_critical_capabilities.20241202.html":
        print("Processing: " + file_name)
        process_mq(ctx, connection, import_df)
    elif file_name == "tech.taxonomy.gartner.claude.csv":
        print("Processing: " + file_name)
        process_tech_taxonomy_gartner_claude(ctx, connection, import_df)
    elif file_name == "tech.hierarchy.claude.001.csv":
        print("Processing: " + file_name)
        process_tech_hierarchy_claude(ctx, connection, import_df)
    else:
        print(f"FILE NOT PROCESSED: {file_name}")

def stage_hierarchy_not_used(ctx):
    """
    This routine stages the hierarchy data for import into the database.
    It loads data into hirarchy_stage table - rename it.    
    """

    # Get database connection instance# get connection    
    connection = utils_psycopb.connect(ctx.obj.logger)
    print("test_db_operations: " + str(connection.connection.closed))

    # load datafrom from csv file
    print("LOAD A HIERARCHY to DF")
    print(ctx.obj.dir_in)
    path=Path(ctx.obj.dir_in) / "hierarchy.001.csv"
    df = utils_data.load_csv_to_df(path, ctx.obj.logger)

    # fix column names
    df.columns = ['level_01', 'level_02', 'level_03', 'level_04', 'level_05_leaf', 'level_05_description']
    # get load_id
    next_value = utils_psycopb.get_next_seq_id(connection, "cdp_data_import_load_id_seq")
    print(f"Next sequence value: {next_value}")

    # add additional columns to dataframe
    df['current_data_load_id'] = next_value
    df['data_load_timestamp'] = pd.Timestamp.now()
    df['created_by'] = ['test_db_operations'] * len(df)

    utils_psycopb.df_to_table(connection, df, "marsol_dev_01.cdp_data_stg_taxonomy")

def load_hierarchy(ctx):
    """
    use one run id for loading all hierarchies
    """


    # scc_hierarchy             - first create new hierarchy entry
    # scc_hierarchy_version     - then entry in scc_hierarchy_version
    # scc_hierarchy_node
    # scc_hierarchy_node_closure

    connection = utils_psycopb.connect(ctx.obj.logger)
    ctx.obj.logger.info("test_db_operations: " + str(connection.connection.closed))

    with connection.cursor() as cursor:

        # get a new run_id for this run - this will populate current_data_load_id
        current_data_load_id = utils_psycopb.get_next_seq_id(connection, "cdp_data_import_load_id_seq")
        ctx.obj.logger.info(f"Next sequence value: {current_data_load_id}")

        # get list of hierarchies to create (from import table)
        cursor.execute(utils_cdp_data_ctl.sql_select_hierarchy_import)
        hierarchies_to_create = cursor.fetchall()

        # Convert the results to a DataFrame
        hierarchies_to_create_df = pd.DataFrame(hierarchies_to_create, columns=[desc[0] for desc in cursor.description])

        # Print the DataFrame
        ctx.obj.logger.info("hierarchies_to_create_df:")
        ctx.obj.logger.info(hierarchies_to_create_df)
        hierarchy_data_list = []
        for _, row in hierarchies_to_create_df.iterrows():

            # this is what is used to query the source rows in scc_hierarchy_import
            source_data_load_id = row['current_data_load_id']

            ctx.obj.logger.info(f"Processing hierarchy with data_load_id: {source_data_load_id}")
            cursor.execute(utils_cdp_data_ctl.sql_select_hierarchy_import_by_load_id, (source_data_load_id,))
            hierarchy_data_to_load = cursor.fetchall()
            hierarchy_data_to_load_df = pd.DataFrame(hierarchy_data_to_load, columns=[desc[0] for desc in cursor.description])
            ctx.obj.logger.info(hierarchy_data_to_load_df)
            # create a hierarchy (if we decide not to use an existing value to be hierarchy name and create new version instead)
            # Check if hierarchy already exists

            # hierarchy_import data_load_id
            cursor.execute(utils_cdp_data_ctl.sql_check_hierarchy_exists, (row['source_system'],))
            hierarchy_exists = cursor.fetchone()

            if not hierarchy_exists:
                # Create a new hierarchy
                ctx.obj.logger.info(f"Creating new hierarchy")
                cursor.execute(utils_cdp_data_ctl.sql_insert_hierarchy, (row['source_system'], row['source_system'], row['data_type'], 'cdp_data_ctl load hierarchy', 'use hierarchy name', '', source_data_load_id, current_data_load_id))
                hierarchy_id = cursor.fetchone()[0]
                ctx.obj.logger.info(f"Created new hierarchy with ID: {hierarchy_id}")
            else:
                ctx.obj.logger.info("Using existing hierarchy")
                hierarchy_id = hierarchy_exists[0]
                ctx.obj.logger.info(f"Using existing hierarchy with ID: {hierarchy_id}")
            
            # commit hierarchy creation
            connection.commit()

            # create new hierarchy version
            # create a new hierarchy version
            # Get the next version number for the hierarchy
            cursor.execute(utils_cdp_data_ctl.sql_get_next_hierarchy_version, (hierarchy_id,))
            next_version_number = cursor.fetchone()[0]
            ctx.obj.logger.info(f"Next version number for hierarchy {hierarchy_id}: {next_version_number}")
            
            cursor.execute(utils_cdp_data_ctl.sql_insert_hierarchy_version, (hierarchy_id, next_version_number, 'cdp_data_ctl load hierarchy', 'use hierarchy version data', '', source_data_load_id, current_data_load_id))
            hierarchy_version_id = cursor.fetchone()[0]
            ctx.obj.logger.info(f"Created new hierarchy version with ID: {hierarchy_version_id}")

            # commit hierarchy version creation
            connection.commit()

            # Print the columns of hierarchy_load_df to the log file
            ctx.obj.logger.info("Columns of hierarchy_data_to_load_df:")
            ctx.obj.logger.info(hierarchy_data_to_load_df.columns.tolist())
            # Print the first few rows of the hierarchy DataFrame
            ctx.obj.logger.info("First few rows of hierarchy_data_to_load_df:")
            ctx.obj.logger.info(hierarchy_data_to_load_df.head())
            # Print the data types of the hierarchy DataFrame
            ctx.obj.logger.info("Data types of hierarchy_data_to_load_df:")
            ctx.obj.logger.info(hierarchy_data_to_load_df.info) #hierarchy_df['hierarchy_version_id'] = hierarchy_version_id
            
            hierarchy_data_to_load_df['hierarchy_version_id'] = hierarchy_version_id

            try: 

                ctx.obj.logger.info("=============================================================")
                ctx.obj.logger.info("Loop Over Hierachies to Import")
                ctx.obj.logger.info("=============================================================")

                # Insert level 05 nodes
                ctx.obj.logger.info("Batch inserting level 01 nodes")
   
                ctx.obj.logger.info("Level 01 INSERT - QUERY - START")
                ctx.obj.logger.info(utils_cdp_data_ctl.sql_insert_hierarchy_level_01_rows)
                ctx.obj.logger.info("hierarchy_version_id: "    + str(hierarchy_version_id))
                ctx.obj.logger.info("source_data_load_id: "     + str(source_data_load_id))
                ctx.obj.logger.info("current_data_load_id: "    + str(current_data_load_id))
                cursor.execute(utils_cdp_data_ctl.sql_insert_hierarchy_level_01_rows, (hierarchy_version_id, current_data_load_id, source_data_load_id))
                level_01_rows = cursor.fetchall()
                level_01_df = pd.DataFrame(level_01_rows, columns=[desc[0] for desc in cursor.description])
                ctx.obj.logger.info("Level 01 DataFrame:")
                ctx.obj.logger.info(level_01_df)

                ctx.obj.logger.info("Level 01 INSERT - START")
                ctx.obj.logger.info(utils_cdp_data_ctl.sql_insert_hierarchy_level_01)
                ctx.obj.logger.info("hierarchy_version_id: "    + str(hierarchy_version_id))
                ctx.obj.logger.info("source_data_load_id: "     + str(source_data_load_id))
                ctx.obj.logger.info("current_data_load_id: "    + str(current_data_load_id))
                cursor.execute(utils_cdp_data_ctl.sql_insert_hierarchy_level_01, (hierarchy_version_id, current_data_load_id, source_data_load_id))
                level_data = cursor.fetchall()
                ctx.obj.logger.info("Inserted level 01 nodes:")
                for row in level_data:
                    ctx.obj.logger.info(f"Inserted row: {row}")
                ctx.obj.logger.info("sql_insert_hierarchy_level_01 DONE")

                connection.commit()

                ctx.obj.logger.info("Level 02 INSERT - QUERY - START")
                ctx.obj.logger.info(utils_cdp_data_ctl.sql_insert_hierarchy_level_02_rows)
                ctx.obj.logger.info("hierarchy_version_id: "    + str(hierarchy_version_id))
                ctx.obj.logger.info("source_data_load_id: "     + str(source_data_load_id))
                ctx.obj.logger.info("current_data_load_id: "    + str(current_data_load_id))
                cursor.execute(utils_cdp_data_ctl.sql_insert_hierarchy_level_02_rows, (source_data_load_id, current_data_load_id, hierarchy_version_id))
                level_02_rows = cursor.fetchall()
                level_02_df = pd.DataFrame(level_02_rows, columns=[desc[0] for desc in cursor.description])
                ctx.obj.logger.info("Level 02 DataFrame:")
                ctx.obj.logger.info(level_02_df)

                ctx.obj.logger.info("Level 02 INSERT - START")
                ctx.obj.logger.info(utils_cdp_data_ctl.sql_insert_hierarchy_level_02)
                ctx.obj.logger.info("hierarchy_version_id: "    + str(hierarchy_version_id))
                ctx.obj.logger.info("source_data_load_id: "     + str(source_data_load_id))
                ctx.obj.logger.info("current_data_load_id: "    + str(current_data_load_id))
                cursor.execute(utils_cdp_data_ctl.sql_insert_hierarchy_level_02, (source_data_load_id, current_data_load_id, hierarchy_version_id))
                level_data = cursor.fetchall()
                ctx.obj.logger.info("Inserted level 02 nodes:")
                for row in level_data:
                    ctx.obj.logger.info(f"Inserted row: {row}")
                ctx.obj.logger.info("sql_insert_hierarchy_level_02 DONE")

                connection.commit()

                ctx.obj.logger.info("Level 03 INSERT - QUERY - START")
                ctx.obj.logger.info(utils_cdp_data_ctl.sql_insert_hierarchy_level_03_rows)
                ctx.obj.logger.info("hierarchy_version_id: "    + str(hierarchy_version_id))
                ctx.obj.logger.info("source_data_load_id: "     + str(source_data_load_id))
                ctx.obj.logger.info("current_data_load_id: "    + str(current_data_load_id))
                cursor.execute(utils_cdp_data_ctl.sql_insert_hierarchy_level_03_rows, (source_data_load_id, current_data_load_id, hierarchy_version_id))
                level_rows = cursor.fetchall()
                level_df = pd.DataFrame(level_rows, columns=[desc[0] for desc in cursor.description])
                ctx.obj.logger.info("Level 03 DataFrame:")
                ctx.obj.logger.info(level_df)

                ctx.obj.logger.info("Level 03 INSERT - START")
                ctx.obj.logger.info(utils_cdp_data_ctl.sql_insert_hierarchy_level_03)
                ctx.obj.logger.info("hierarchy_version_id: "    + str(hierarchy_version_id))
                ctx.obj.logger.info("source_data_load_id: "     + str(source_data_load_id))
                ctx.obj.logger.info("current_data_load_id: "    + str(current_data_load_id))
                cursor.execute(utils_cdp_data_ctl.sql_insert_hierarchy_level_03, (source_data_load_id, current_data_load_id, hierarchy_version_id))
                level_data = cursor.fetchall()
                ctx.obj.logger.info("Inserted level 03 nodes:")
                for row in level_data:
                    ctx.obj.logger.info(f"Inserted row: {row}")
                ctx.obj.logger.info("sql_insert_hierarchy_level_03 DONE")

                connection.commit()

            except Exception as e:
                    print("error inserting hierarchy nodes")
                    ctx.obj.logger.error(f"Error inserting hierarchy nodes: {e}")
                    connection.rollback()   

    # insert row into scc_hierarchy

    connection.close()

def test_db_operations(ctx):

    print("=============================================================")
    print("Enter test_db_operations")
    print("=============================================================")

    # Get database connection instance# get connection    
    connection = utils_psycopb.connect(ctx.obj.logger)
    print("test_db_operations: " + str(connection.connection.closed))

    utils_psycopb.print_query_results(connection, "SELECT * FROM marsol_dev_01.cdp_data_stg_taxonomy;")

    connection.close()

