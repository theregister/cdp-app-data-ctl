

def get_prompt_text(prompt_type, prompt_name, input_text="", taxonomy_df=None, asset_classification_df=None):

    hierarchy_str = ""
    if taxonomy_df is not None:
        hierarchy_str = taxonomy_df.to_string(index=False)

    input_classifications_str = ""
    if asset_classification_df is not None:
        input_classifications_str = "\n".join(asset_classification_df)

    prompt_classification_no_taxonomy_01 = f"""
        Role:
        You are an expert in the enterprise technology domain.
        
        Task:
        Extract unique technology related keywords from the provided input text.
        For each technology keyword, determine the type of the keyword or term.
        The values for the type will be
        1) a technology name - use the value technology_name
        2) a technology vendor - use the value technology_vendor
        3) a technology product - use the value technology_product
        If you don't think the keyword fits into any of the above types, define the type enclosed within ** either side the type you defined.
        Calculate a score of 1 to 10 on how relevant the keyword is within the context of the provided text.
        Once the keywords have been identified and their type established, for each keyword define the technology category it fits into.
        Finally for the values you have identified, calculate a confidence level you have for each set of values on a scale of 1 to 10.
        
        Output:
        The output will be a set of values for keyword_name, keyword_type, relevance_score, keyword_category, confidence_score
        Do not include the column headings.
        The output will be separated by the "|" character.
        So there should be  4 "|" in each row separating the 5 values.
        The output will sort the entries by descending relevance_score.
        Input:
        {input_text}
        """

    prompt_classification_taxonomy_01 = f"""
        Role:
        You are an expert in the enterprise technology domain.
        
        Task:
        Extract unique technology related keywords from the provided input text.
        Calculate a score of 1 to 10 on how relevant the keyword is within the context of the provided text.
        For each technology keyword, associate one or more values from the Capability level of the input technology hierarchy.
        Calculate a confidence level you have for each set of values on a scale of 1 to 10.
        
        Output:
        The output will be a set of values for keyword_name, <empty_column>, relevance_score, capability, confidence_score
        Do not include the column headings.
        The output will be separated by the "|" character.
        So there should be  4 "|" in each row separating the 5 values.
        The output will sort the entries by descending relevance_score.
        
        Input:
        Input Text:
        {input_text}
        Input Technology Hierarchy: {hierarchy_str}
        """

    prompt_classification_taxonomy_02 = f"""
        Role:
        You are an expert in the enterprise technology domain.
        You will help to maintain a technology keyword taxonomy.
        This taxonomy is used to classify technology-related content.
        The initial taxonomy is defined as the Input_Technology_Hierarchy and input is provided in the form of a CSV file.
        The taxonomy is hierarchical, with 5 levels.  Each level node having a parent node at the previous level.
        Each level node can have only one parent (there can be no multiple parent relationships).
        Broadly the taxonomy has the following structure
        Domain (level 01),Category (level 02),Function (level 03),Capability (level 04),Technology (level 05)
        
        Task:
        For the Input_Text provided, extract unique technology related keywords.
        These technology related keywords will be considered at level 05 in the Input_Technology_Hierarchy taxonomy.
        Calculate a score of 1 to 10 (integers only) on how relevant the keyword is within the context of the provided text.
        Calculate a confidence level you have for each set of values on a scale of 1 to 10 (integers only)
        Determine if the technology related keywords fit into the existing taxonomy.
        If the technology related keywords fit into the existing taxonomy, associate the technology related keywords with the appropriate node in the taxonomy.
        If the technology related keywords do not fit into the existing taxonomy, suggest a new node that should be added to the taxonomy.
        Remember that the taxonomy must NOT HAVE multiple parent relationships.

        Output:
        The output will be a set of values for the following attributes (every row must have 12 output columns)
        column 01 - Domain (level 01),
        column 02 - Domain Exists (true or false),
        column 03 - Category (level 02),
        column 04 - Category Exists (true or false),
        column 05 - Function (level 03),
        column 06 - Function Exists (true or false),
        column 07 - Capability (level 04),
        column 08 - Capability Exists (true or false),
        column 09 - Technology (level 05),
        column 10 - Technology Exists (true or false),
        column 11 - relevance_score,
        column 12 - confidence_score
        Do not include the column headings.
        The output will be separated by the "|" character.
        So there should be  13 "|" in each row separating the 12 values.
        The output will sort the entries by descending relevance_score.
        
        NOTE: every row out output data must have the 13 columns specified above.  ALWAYS.

        Input:
        Input_Text: {input_text}
        Input_Technology_Hierarchy: {hierarchy_str}
        """

    prompt_classification_taxonomy_03 = f"""
        I need you to analyze the following content for technology keywords and match them to the taxonomy I've provided.
        INPUT_CONTENT: {input_text}
        Instructions:
        1. Identify key technology-related terms and concepts in the article.
        2. For each identified term, find the best matching Level 5 keyword in the taxonomy
        3. If a term doesn't match existing taxonomy entries, suggest a new path with all 5 levels
        4. Your output MUST follow this exact TSV (tab-separated values) format:
        5. Output your analysis as TSV data (using tabs as separators instead of commas)
            - Include a header row with these EXACT column names: technology_term,relevance_score,confidence_score,matching_taxonomy_path,match_type,notes
            - Technology Term (from article)
            - Relevance score (1-10)
            - Confidence score (1-10)
            - Matching Taxonomy Path - 5 levels separated by > (Level 1 to Level 5)
            - Match Type (Exact, Partial, New Suggestion)
            - Notes - context for match type and any new suggestions
        7. Each term on its own line
        8. Do not include any explanatory text before or after the TSV data.  Include explanatory text in the Notes column.
        9. Start your response with ONLY the header row followed immediately by the data rows.

        INITIAL_FULL_TAXONOMY:
        This initial taxonomy is hierarchical, with 5 levels. Each level node has a parent node at the previous level.
        Each level node can have only one parent (there can be no multiple parent relationships).
        Broadly the taxonomy has the following structure:
        Technology Domain (level 1),Technology Area (level 2), Technology Category (level 3), Technology SubCategory (level 4), Technology Keywords (level 5), Technology Keyword Description
        Here is the taxonomy:
        {hierarchy_str}"""

    prompt_classification_taxonomy_version_02 = f"""
        I need you to analyze the following content for technology keywords and match them to the taxonomy I've provided.
        INPUT_CONTENT: {input_text}
        Instructions:
        1. Identify key technology-related terms and concepts in the article.
        2. Ientify only the top 10 most relevant terms.
        2. For each identified term, find the best matching Level 4 keyword in the taxonomy
        3. If a term doesn't match existing taxonomy entries, suggest a new path with all 5 levels
        4. Your output MUST follow this exact TSV (tab-separated values) format:
        5. Output your analysis as TSV data (using tabs as separators instead of commas)
            - Include a header row with these EXACT column names: technology_term,relevance_score,confidence_score,matching_taxonomy_path,match_type,notes
            - Technology Term (from article)
            - Relevance score (1-10)
            - Confidence score (1-10)
            - Matching Taxonomy Path - 5 levels separated by > (Level 1 to Level 5)
            - Match Type (Exact, Partial, New Suggestion)
            - No more than 10 terms
            - Notes - context for match type and any new suggestions
        7. Each term on its own line
        8. Do not include any explanatory text before or after the TSV data.  Include explanatory text in the Notes column.
        9. Start your response with ONLY the header row followed immediately by the data rows.

        INITIAL_FULL_TAXONOMY:
        This initial taxonomy is hierarchical, with 5 levels. Each level node has a parent node at the previous level.
        Each level node can have only one parent (there can be no multiple parent relationships).
        Broadly the taxonomy has the following structure:
        Technology Domain (level 1),Technology Area (level 2), Technology Category (level 3), Technology SubCategory (level 4), Technology Keywords (level 5), Technology Keyword Description
        Here is the taxonomy:
        {hierarchy_str}"""

    prompt_classification_taxonomy_version_02_01 = f"""
        I need you to analyze the following content for technology keywords and match them to the taxonomy I've provided.
        INPUT_CONTENT: {input_text}
        Instructions:
        1. Identify key technology-related terms and concepts in the article.
        2. Ientify only the top 10 most relevant terms.
        3.  For each term identified attempt to match it will a level 5 keyword in the taxonomy.
            If the term matches a value in level 5 of the taxonomy, mark the row as "exact_level_05", and populate the llm_matched_int_taxonomy_level_05_id with the id from the matched level 5 taxonomy value.  If there is no match, enter a -9 value.
            If the term doesn't match a level 5 value in the taxonomy, match the term to an appropriate level 04 parent value that will have the term as a child. Mark the row as "new_level_05_suggestion", and populate the llm_matched_int_taxonomy_level_04_id with the id from the matched level 4 taxonomy value. If there is no match, enter a -9 value.
        4. Your output MUST follow this exact TSV (tab-separated values) format:
        5. Output your analysis as TSV data (using tabs as separators instead of commas)
            - Include a header row with these EXACT column names: technology_term,relevance_score,confidence_score,matching_taxonomy_path,match_type,llm_matched_int_taxonomy_level_05_id,llm_matched_int_taxonomy_level_04_id,notes
            - Technology Term (from article)
            - Relevance score (1-10)
            - Confidence score (1-10)
            - Matching Taxonomy Path - 4 levels separated by > (Level 1 to Level 4).  Remember match must be at level 5.
            - llm_matched_int_taxonomy_level_05_id - this is the id of the level 5 matched node.  If there is no match, enter a -9 value.
            - llm_matched_int_taxonomy_level_04_id - this is the id of the level 4 node in the taxonomy that the term should be added to.  If there is no match, enter a -9 value.
            - Match Type (exact_level_05, new_level_05_suggestion))
            - No more than 10 terms
            - Notes - context for match type and any new suggestions
        6. Each term on its own line
        7. Do not include any explanatory text before or after the TSV data.  Include explanatory text in the Notes column.
        8. Start your response with ONLY the header row followed immediately by the data rows.

        INITIAL_FULL_TAXONOMY:
        This initial taxonomy is hierarchical, with 5 levels. Each level node has a parent node at the previous level.
        Each level node can have only one parent (there can be no multiple parent relationships).
        Broadly the taxonomy has the following structure:
        Here is the taxonomy:
        {hierarchy_str}"""

    prompt_classification_taxonomy_version_02_03 = f"""
        You are an expert in the technology domain and I want you to analyze the following content for technology keywords and match them to the INITIAL_FULL_TAXONOMY I've provided.
        This initial taxonomy is hierarchical, with 5 levels. Each level node has a parent node at the previous level.
        Each level node has only one parent (there can be no multiple parent relationships).
        Here is the taxonomy
        INITIAL_FULL_TAXONOMY:
        {hierarchy_str}

        Instructions:
        1. Identify key technology-related terms and concepts in the INPUT_CONTENT.
        2. Ientify only the top 10 most relevant terms.
        3. All the technology terms identified must either be matched to an existing level 5 keyword in the taxonomy or a new level 5 keyword should be suggested.
        4. Hence you should perform the following.
           For each technology term identified in the article you can do 1 of the following 3 things.
            a) If the technology terms matches to an existing level_05_name do the following:
                  1. mark the row as "exact_level_05"
                  2. populate the llm_matched_int_taxonomy_level_05_id with the id from the matched level_05_name taxonomy value.
                  3. If there is no exact match, enter a -9 value in llm_matched_int_taxonomy_level_05_id.
                  4. Do not match to level_05_name values that start with "Dummy.Level05.Node" as these are placeholders.
            b) if there is no match to an existing level_05_name value then do the following.
                    1. match the term to an appropriate level_04_name parent value that will have the current term as a child.
                    2. Mark the row as "new_level_05_suggestion"
                    3. populate the llm_matched_int_taxonomy_level_04_id with the id from the matched level_4_name taxonomy value.
            c) Finally, if you are unable to perform either of a) or b) then mark the row as "no_match" and populate the llm_matched_int_taxonomy_level_05_id and llm_matched_int_taxonomy_level_04_id with -9 values.
        4. Your output MUST follow this exact TSV (tab-separated values) format:
        5. Output your analysis as TSV data (using tabs as separators instead of commas)
            - Include a header row with these EXACT column names: technology_term,relevance_score,confidence_score,matching_taxonomy_path,match_type,llm_matched_int_taxonomy_level_05_id,llm_matched_int_taxonomy_level_04_id,notes
            - Technology Term (from article)
            - Relevance score (1-10)
            - Confidence score (1-10)
            - Matching Taxonomy Path - 4 levels separated by > (Level 1 to Level 4).  Remember match must be at level 5.
            - llm_matched_int_taxonomy_level_05_id - this is the id of the level 5 matched node.  If there is no match, enter a -9 value.
            - llm_matched_int_taxonomy_level_04_id - this is the id of the level 4 node in the taxonomy that the term should be added to.  If there is no match, enter a -9 value.
            - Match Type (exact_level_05, new_level_05_suggestion))
            - No more than 10 terms
            - Notes - context for match type and any new suggestions
        6. Each term on its own line
        7. Do not include any explanatory text before or after the TSV data.  Include explanatory text in the Notes column.
        8. Start your response with ONLY the header row followed immediately by the data rows.

        Here is the input content.
        INPUT_CONTENT: {input_text}
        """

    prompt_classification_taxonomy_version_02_03 = f"""
        You are an expert in the technology domain and I want you to analyze the following content for technology keywords and match them to the INITIAL_FULL_TAXONOMY I've provided.
        This initial taxonomy is hierarchical, with 5 levels. Each level node has a parent node at the previous level.
        Each level node has only one parent (there can be no multiple parent relationships).
        Here is the taxonomy
        INITIAL_FULL_TAXONOMY:
        {hierarchy_str}

        Instructions:
        1. Identify key technology-related terms and concepts in the INPUT_CONTENT.
        2. Ientify only the top 10 most relevant terms.
        3. All the technology terms identified must either be matched to an existing level 5 keyword in the taxonomy or a new level 5 keyword should be suggested.
        4. Hence you should perform the following.
           For each technology term identified in the article you can do 1 of the following 3 things.
            a) If the technology term matches to an existing level_05_name do the following:
                  1. mark the row as "exact_level_05"
                  2. populate the llm_matched_int_taxonomy_level_05_id with the id from the matched level_05_name taxonomy value.
                  3. If there is no exact match, enter a -9 value in llm_matched_int_taxonomy_level_05_id.
                  4. Do not match to level_05_name values that start with "Dummy.Level05.Node" as these are placeholders.
            b) if there is no match to an existing level_05_name value then do the following.
                    1. match the term to an appropriate level_04_name parent value that will have the current term as a child.
                    2. Mark the row as "new_level_05_suggestion"
                    3. populate the llm_matched_int_taxonomy_level_04_id with the id from the matched level_4_name taxonomy value.
            c) Finally, if you are unable to perform either of a) or b) then mark the row as "no_match" and populate the llm_matched_int_taxonomy_level_05_id and llm_matched_int_taxonomy_level_04_id with -9 values.
        4. Your output MUST follow this exact TSV (tab-separated values) format:
        5. Output your analysis as TSV data (using tabs as separators instead of commas)
            - Include a header row with these EXACT column names: technology_term,relevance_score,confidence_score,matching_taxonomy_path,match_type,llm_matched_int_taxonomy_level_05_id,llm_matched_int_taxonomy_level_04_id,notes
            - Technology Term (from article)
            - Relevance score (1-10)
            - Confidence score (1-10)
            - Matching Taxonomy Path - 4 levels separated by > (Level 1 to Level 4).  Remember match must be at level 5.
            - llm_matched_int_taxonomy_level_05_id - this is the id of the level 5 matched node.  If there is no match, enter a -9 value.
            - llm_matched_int_taxonomy_level_04_id - this is the id of the level 4 node in the taxonomy that the term should be added to.  If there is no match, enter a -9 value.
            - Match Type (exact_level_05, new_level_05_suggestion))
            - No more than 10 terms
            - Notes - context for match type and any new suggestions
        6. Each term on its own line
        7. Do not include any explanatory text before or after the TSV data.  Include explanatory text in the Notes column.
        8. Start your response with ONLY the header row followed immediately by the data rows.

        Here is the input content.
        INPUT_CONTENT: {input_text}
        """

    # perform only keyword extraction
    # version 02_05 - more explicit instructions in notes column
    prompt_keyword_identification_20250428 = f"""
        You are an expert in the technology domain and I want you to analyze the following content for technology keywords that highlight the key aspects of the article.
        Ultimately these keywords will be used to classify the article as a way of identifying what readers of the article are interested in.
        1. Ientify the top 10 most relevant terms.
        2. Output your analysis as TSV data (using tabs as separators instead of commas)
            - Technology Term - use column name technology_term(from article)
            - Relevance score - use column name relevance_score (1-10)
            - Confidence score - use column name confidence_score (1-10)
            - Notes - use column name notes - this should contain an explaination of why the term has been selected and why the relevance and confidence scores have been assigned.
        3. Each term on its own line

        Here is the input content.
        INPUT_CONTENT: {input_text}
        """

    prompt_taxonomy_match_20250428 = f"""
        You are an expert in the technology domain.
        I will give you an article and a set of technology keywords that are relevant to the article.
        Each keyword has a relevance score and a confidence score.
        I will also give you a hierarchical taxonomy of technology keywords.
        You will match the article keywords to the taxonomy at the lowest (most specific) level possible.
        You can suggest new nodes in the taxonomy if you think they are needed.
        What I want you to do is the following
        For each keyword idenfified in that article, provided in the INPUT_ASSET_KEYWORDS input, I want you to do the following.
        1. attempt to identify the best matching node in the taxonomy.  In this case the match should be equivalent to the selected node.
            - set the classification_match_type column value to exact_match_direct
            - and populate the taxonomy_node_name, taxonomy_node_level, taxonomy_node_id column with the id of the node in the taxonomy.
            - do not match to nodes with values "Dummy.LevelXX.Node.XXXXX" as these are placeholders.
        2. If there is no exact, direct match and a new node can be suggested.  In this case the match will be to an existing node with the intent that the new node will be added as a child of the existing node.
            -- set the classification_match_type column value to exact_match_new_suggestion
            -- populate the taxonomy_node_name, taxonomy_node_level, taxonomy_node_id column with the id of the parent node in the taxonomy that the new node should be added to.
            -- do not match to nodes with values "Dummy.LevelXX.Node.XXXXX" as these are placeholders.
        3. If there is no appropriate match and a new node cannot be suggested
            -- set the classification_match_type column value to no_match
            -- populate the taxonomy_node_name, taxonomy_node_level, taxonomy_node_id column with values -1, -1, -1
        4. Output your analysis as TSV data (using tabs as separators instead of commas)
            - The output should have X columns and the same number of rows as the INPUT_ASSET_KEYWORDS input.
            - The output columns should be as follows:
            - int_asset_classification_result_id - use the same value from the column asset_classification_id in INPUT_ASSET_KEYWORDS
            - classification_match_type - has an existing node been identified (exact_match_direct, exact_match_new_suggestion, no_match)
            - taxonomy_node_name - use the name of the match in the INPUT_TAXONOMY or a new suggested name
            - taxonomy_node_level - use the level of the node in the INPUT_TAXONOMY - ONLY USE INTEGER VALUES IN THIS COLUMN.
            - taxonomy_node_id - use the node_id from the INPUT_TAXONOMY
            - cls_relevance_score - integer relevance score (1-10)
            - cls_confidence_score - integer confidence score (1-10)
            - cls_notes - this column should contain an explaination of why the term has been selected and why the relevance and confidence scores have been assigned.
        3. Each term on its own line
        4. NEVER match to nodes with values "Dummy.LevelXX.Node.XXXXX" as these are placeholders.
        4. There should only be the same number of rows in the output as there are in INPUT_ASSET_KEYWORDS
        5. Do not include any explanatory text before or after the TSV data.  Include explanatory text in the Notes column.

        Here is the input content.
        INPUT_ASSET: {input_text}
        INPUT_ASSET_KEYWORDS: {asset_classification_df}
        INPUT_TAXONOMY: {hierarchy_str}
    """
    
    prompts = {
        "prompt_classification_no_taxonomy_01":             prompt_classification_no_taxonomy_01,
        "prompt_taxonomy_match_20250428":                   prompt_taxonomy_match_20250428,
        "prompt_keyword_identification_20250428":           prompt_keyword_identification_20250428,
        "prompt_classification_taxonomy_version_02_03":     prompt_classification_taxonomy_version_02_03,
        "prompt_classification_taxonomy_version_02_01":     prompt_classification_taxonomy_version_02_01,
        "prompt_classification_taxonomy_version_02":        prompt_classification_taxonomy_version_02,
        "prompt_classification_taxonomy_03":                prompt_classification_taxonomy_03
    }

    # Add other prompts here as needed, incorporating taxonomy_df and input_classifications where applicable.

    try:
        return prompts.get(prompt_name, "Invalid prompt name")
    except Exception as e:
        print("looking to catch key error to understand better")
        print(f"Error: {e}")
        return f"Error: {e}"

def get_prompt_text_campaign_insights(prompt_type, prompt_name, project_leads_df):
    """
    For now separate function for campaign insights generation.
    Get the prompt text based on the prompt type and name.
    This function is similar to get_prompt_text but specifically for campaign insights.
    """

    campaign_insights_20250528_01 = f"""
        You are an expert in lead generation and marketing campaigns.
        I want you to analyze the output of a marketing campaign and identify key insights that can help improve future campaigns.
        The input data I will provide will be a set of leads generated, that consumed particular content.

        You will structure the content into specific sections.  The sections will be as follows:
        1. Executive Summary - a brief overview of the campaign performance, highlighting key metrics and outcomes.
        2. An analysis of the assets that were most effective in generating leads.
        3. An analysis of the job functions that responded
        4. An analysis of any correlations between the assets and the job functions that responded.
        5. A strategic analysis of the content prefernces by job role and how this can be used to inform future campaigns.
        6. Provide a set of future targetting recommendations based on the analysis.

        Output your analysis as TSV data (using tabs as separators instead of commas)
        The output will specifically be a 2 column table with the following columns
        - section_name - the name of the section
        - section_content - the content of the section
        INPUT_CONTENT: {project_leads_df}
    """

    prompts = {
        "campaign_insights_20250528_01":             campaign_insights_20250528_01,
    }

    # Add other prompts here as needed, incorporating taxonomy_df and input_classifications where applicable.

    try:
        return prompts.get(prompt_name, "Invalid prompt name")
    except Exception as e:
        print("looking to catch key error to understand better")
        print(f"Error: {e}")
        return f"Error: {e}"