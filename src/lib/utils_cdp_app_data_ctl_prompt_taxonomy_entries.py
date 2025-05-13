def get_prompt_text(prompt_name, keywords_df=None, taxonomy_df=None):

    taxonomy_str = taxonomy_df.to_string() if taxonomy_df is not None else ""
    keywords_str = keywords_df.to_string() if keywords_df is not None else ""

    prompt_suggest_new_taxonomy_entries_01 = f"""
        You are an expert in the technology domain.
        I am providing you a technology keyword taxonomy.
        I am also providing you a list of technology related keywords that were extracted from a set of articles.
        I want you to analyze the technology related keywords and for each line item indicate whether the keyword matches an existing level_05 taxonomy entry.
        If the keyword does not match an existing level_05 taxonomy entry, I want you to suggest a new path with all 5 levels.

        INPUT_TECHNOLOGY_TAXANOMY: {taxonomy_str}
        INPUT_TECHNOLOGY_KEYWORDS: {keywords_str}

        Instructions:
        1. For each line item in the technology keywords list, indicate whether the keyword matches an existing level_05 taxonomy entry.
        2. If the keyword does not match an existing level_05 taxonomy entry, indicate the level 04 parent it should be added to.
        2. For each identified term, find the best matching Level 5 keyword in the taxonomy.  The match MUST be at level 5.
        3. If a term doesn't match to an existing level 5 taxonomy entry, suggest a new path with all 5 levels
        4. Your output MUST follow this exact TSV (tab-separated values) format:
        5. Output your analysis as TSV data (using tabs as separators instead of commas)
            - Include a header row with these EXACT column names: input_technology_term, input_matching_taxonomy_path, input_notes, output_entry_type, output_level_05_entry, output_level_04_parent, output_notes
            - input_technology_term - this should be the data from the input file
            - input_matching_taxonomy_path
            - input_notes
            - output_entry_type - exact_match or new_suggestion
            - output_level_05_entry - the technology term to be inserted at level 5
            - output_level_04_parent_name - the level 4 parent to insert the new term under
            - output_level_04_parent_id - the level 4 id from the input taxonomy to insert the new term under
            - output_notes - notes on the match type and any new suggestions
            - Confidence score (1-10)
        8. Do not include any explanatory text before or after the TSV data.  Include explanatory text in the Notes column.

        INITIAL_FULL_TAXONOMY:
        This initial taxonomy is hierarchical, with 5 levels. Each level node has a parent node at the previous level.
        Each level node can have only one parent (there can be no multiple parent relationships).
        """

    prompts = {
        "prompt_suggest_new_taxonomy_entries_01": prompt_suggest_new_taxonomy_entries_01,
    }

    try:
        return prompts.get(prompt_name, "Invalid prompt name")
    except Exception as e:
        print("looking to catch key error to understand better")
        print(f"Error: {e}")
        return f"Error: {e}"
