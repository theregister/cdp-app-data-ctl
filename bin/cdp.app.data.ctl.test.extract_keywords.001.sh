
source $CDP_APP_DATA_CTL_HOME/env/shell/env.set.sh
source $CDP_APP_DATA_CTL_HOME/env/shell/env.set.db.alloy.sitpub.marsol_dev_01.sh

# load taxonomies to stage
#cdp.app.data.ctl.sh     load    to-stage    taxonomy

# load taxonomies from stage
#cdp.app.data.ctl.sh     load    from-stage  taxonomy

# extract from assets - this should become a run of and LLM against an asset and classify it (identity technology_terms or buy cycle info etc)
cdp.app.data.ctl.sh process extract-keywords-from-assets

#-- this will take an instance of the published asset classification and attempt a match to a taxonomy
#-- will support matching at any level of hierarhy
#-- row should have match_type - exact_match, no_match_new_suggestion
#cdp.app.data.ctl.sh process match_classification_to_taxonomy

# for a set of published asset classifications, this will take a taxonomy and match it to the asset classification.
## for each asset classification find the bast match to a taxonomy node
## store the taxonomy node with the asset classification.

# now make sure ONE version of the extraction results are made PUBLISHED in CDP_APP_DATA_asset_classification

# now take published asset classification and match the keywords with a taxanomy and then populate the asset classification/taxonmy map table
# there will only be one taxonomy per asset classification which will be one used to score transactions.
# cdp.app.data.ctl.sh build asset_classifiction_taxonomy_map
# inputs
# - classification_version

# now take asset_taxonomy_map and score transactions on it.
#cdp.app.data.ctl.sh score_transactions - input asset_taxonomy_map


