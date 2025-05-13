
sql_get_assets_for_taxonomy_matching = """
-- REFRESH MATERIALIZED VIEW cdp_data_pub_asset_01_mv;
-- REFRESH MATERIALIZED VIEW cdp_data_pub_asset_02_mv;
select      asset.asset_id             	as id,
            asset.title               	as title,
            asset.asset_text          	as asset_text
from        cdp_data_pub_asset_01_mv     asset
where       has_cls_rows = 1
order by    asset.asset_id
"""

sql_get_asset_classifications_for_matching = """
WITH params AS (
    SELECT  %s::uuid as asset_id
)
select      asset.asset_id                                  as asset_id,
            result.int_asset_cls_result_id                  as asset_cls_result_id,
            result.cls_result                               as keyword,
            result.cls_relevance_score                      as relevance_score,
            result.cls_confidence_score                     as confidence_score
from        cdp_data_asset_cls_result_02_v                  as result
join        cdp_data_asset_02_v                             as asset
    on      result.asset_id = asset.asset_id
JOIN    	params                                  		AS p
ON      	asset.asset_id = p.asset_id
--where       source_asset_id between 1 and 10
where       asset.asset_id = p.asset_id
and         result.int_cls_result_latest = 1
"""

sql_get_keywords = """
select 	technology_term					        as input_technology_term,
	matching_taxonomy_path		                as input_matching_taxonomy_path,
	notes										as input_notes,
	null										as output_entry_type,
	null 										as output_level_05_entry,
	null										as output_level_04_parent,
	null										as output_level_04_id,	
	null 										as output_notes
from 	cdp_data_asset_cls_result_01_v 	        result
where 	prompt                          = 'prompt_classification_taxonomy_version_02_01'
and   	classification_taxonomy         = 'taxonomy.version.02.for.update.csv'
and	    relevance_score 	        >= 7
and  	confidence_score 	        >= 7
"""

sql_get_assets_old = """
SELECT  paper.id                  	as id,
        paper.title               	as title,
        paper.asset_text          	as asset_text
FROM cdp_data_paper_01_v AS paper
WHERE exists_in_leadgen_asset = 1
--AND has_cls_result = 0
AND not EXISTS (
    SELECT 1
    FROM cdp_data_asset_cls_result_01_v         as  result
    WHERE result.asset_id = paper.id
    AND result.classification_taxonomy = 'taxonomy.version.02.for.update.csv'
)
AND length(asset_text) > 1000
AND language_code = 'en'
ORDER BY id ASC
limit 100
"""

# this will fix batch numbers and sizes.
# use the batch number to process different batches of assets
sql_get_assets = """
WITH parameters 		AS (
    SELECT  100::int 	as batches_total,
            1::int 		as batch_number
),
BatchedRecords AS (
SELECT 		papers.*,
       		MOD(papers.id, p.batches_total) AS batch_id
FROM 		cdp_data_paper_01_v papers
CROSS JOIN 	parameters p
where 		exists_in_leadgen_asset = 1
AND         length(asset_text) > 1000
AND         language_code = 'en'
)
select      asset.asset_id              as asset_id,
            asset.title               	as title,
            asset.asset_text          	as asset_text
from        cdp_data_asset_02_v   asset
where       source_asset_id between 1 and 10
"""

sql_get_assets_latest="""
-- drive classifications off of leadgen transaction assets
select      asset.asset_id             	as id,
            asset.title               	as title,
            asset.asset_text          	as asset_text
from        cdp_data_pub_asset_01_v     asset
where       asset.source_asset_id in    (
                                        select      distinct leadgen.research_paper_id               as research_paper_id
                                        from        cdp_data_leadgen_lead_trx_merged_02_v   as leadgen
                                        where       time_year_transaction_at in (2025, 2024, 2023)
                                        )
and         asset.source_asset_type = 'sitpub.research.paper'
and         asset_status = 'valid'
and         has_cls_rows = 0
order by    asset.asset_id desc
"""

sql_get_assets_02_last_="""
select      asset.asset_id             	as id,
            asset.title               	as title,
            asset.asset_text          	as asset_text
from        cdp_data_asset_02_v   asset
where       asset.source_asset_id between 1 and 10
and         asset.source_asset_type = 'sitpub.research.paper'
order by    asset.title
"""

sql_get_assets_02_orig = """
WITH parameters 		AS (
    SELECT  1::int 	as batches_total,
            1::int 		as batch_number
),
BatchedRecords AS (
--SELECT 		papers.*,
--       		MOD(papers.id, p.batches_total) AS batch_id
--
select		distinct asset.id								as  id,
			asset.title										as  title,
			asset.asset_text								as  asset_text,
       		MOD(asset.id, p.batches_total) 					AS batch_id
from 		leadgen.lead_transaction 						lead_trx
join		leadgen.foreign_keys							fk
	on		lead_trx.id = fk.lead_transaction_id
join		cdp_data_paper_02_v								asset
	on 		asset.id = fk.research_paper_id
CROSS JOIN 	parameters 										p
where 		extract(year from transaction_at) in (2025)
--and      	extract(month from transaction_at) = 3
AND         length(asset.asset_text) > 1000
AND         asset.language_code = 'en'
and         asset.has_classification_rows is false
)
SELECT 	paper_batches.id                  	as id,
        paper_batches.title               	as title,
        paper_batches.asset_text          	as asset_text
FROM 	BatchedRecords paper_batches
WHERE 	batch_id = (SELECT batch_number - 1 FROM parameters)
ORDER 	BY id;
"""

sql_get_taxonomy = """
WITH params AS (
    SELECT  %s::int as int_taxonomy_version_id
)
select  level_01_name,
        level_02_name,
        level_03_name,
        level_04_name,
        level_04_id,
        level_05_name,
        level_05_id,
        level_05_description
from    cdp_data_taxonomy_star_02_v AS taxonomy
JOIN    params                                  AS p
ON      taxonomy.int_taxonomy_version_id = p.int_taxonomy_version_id
"""

sql_get_taxonomy_tree = """
WITH params AS (
    SELECT  %s::int as int_taxonomy_version_id
)
SELECT 	taxonomy.name			 					as node_name,
        taxonomy.int_taxonomy_node_id				as node_id,
		taxonomy.name_qualifier						as node_name_qualifier,
		taxonomy.description						as node_description,
		taxonomy.level								as node_level,
		taxonomy.path								as node_path,
		taxonomy.depth								as node_depth
from    cdp_data_taxonomy_tree_01_v AS taxonomy
JOIN    params                                  AS p
ON      taxonomy.int_taxonomy_version_id = p.int_taxonomy_version_id
WHERE   taxonomy.level != 5
"""

sql_get_taxonomy_star = """
WITH params AS (
    SELECT  %s::int as int_taxonomy_version_id
)
select  level_01_name,
        level_02_name,
        level_03_name,
        level_04_name,
        level_04_id,
        level_05_name,
        level_05_id,
        level_05_description
from    cdp_data_taxonomy_star_02_v AS taxonomy
JOIN    params                                  AS p
ON      taxonomy.int_taxonomy_version_id = p.int_taxonomy_version_id
"""

sql_get_taxonomy_version_id = """
select  int_taxonomy_version_id
from    cdp_data_taxonomy_version_summary_01_v
where   taxonomy_name = %s
and     taxonomy_version = %s
"""
