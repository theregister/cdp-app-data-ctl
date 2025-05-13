
sql_test = """
select 		distinct 	hier_import.level_02					as 	name,
						hier_level.int_current_batch_id	    	as 	int_current_batch_id,
						hier_import.int_current_batch_id		as 	int_source_batch_id,
			            hier_import.int_source_system   		as 	int_source_system,
			            hier_import.int_source_object   		as 	int_source_object,
			            hier_import.int_source_id	    		as 	int_source_id,
			            hier_level.int_taxonomy_level_01_id	as 	int_taxonomy_level_01_id,  			-- parent id already inserted	
						hier_version.int_taxonomy_version_id 	as 	int_taxonomy_version_id,
			            count(*)		    					as 	unique_row_count
from 		cdp_data_stg_taxonomy 								as	hier_import
join 		cdp_data_taxonomy_level_01 						as	hier_level
			on 	hier_level.name 								= hier_import.level_01 
JOIN 		cdp_data_taxonomy_version 						as	hier_version
			on 	hier_version.int_taxonomy_version_id 			= hier_level.int_taxonomy_version_id 
where 		hier_level.int_current_batch_id 					= hier_version.int_current_batch_id 
GROUP BY 	hier_import.level_02,
			hier_level.int_current_batch_id,
			hier_import.int_current_batch_id,
			hier_import.int_source_system,
			hier_import.int_source_object,
			hier_import.int_source_id,
			hier_level.int_taxonomy_level_01_id,
			hier_version.int_taxonomy_version_id
order by    hier_import.level_02							desc,
			hier_level.int_current_batch_id					desc,
			hier_import.int_current_batch_id 				desc,
			hier_import.int_source_system					desc,
			hier_import.int_source_object					desc,
			hier_import.int_source_id						desc,
			hier_level.int_taxonomy_level_01_id			desc,
			hier_version.int_taxonomy_version_id			desc
"""

sql_select_star_cdp_data_taxonomy = """
select *
from cdp_data_taxonomy
"""

sql_select_star_cdp_data_taxonomy_version = """
select *
from cdp_data_taxonomy_version
"""

sql_select_star_cdp_data_taxonomy_level_01 = """
select *
from cdp_data_taxonomy_level_01
"""

sql_select_star_cdp_data_taxonomy_level_01_v = """
select *
from cdp_data_taxonomy_level_01_v
"""

sql_select_star_cdp_data_stg_taxonomy_level_02_v = """
select *
from cdp_data_stg_taxonomy_level_02_01_v
"""

sql_insert_taxonomy = """
INSERT INTO cdp_data_taxonomy (
name,
description,
data_type,
int_source_system,
int_source_object,
int_source_id,
int_source_batch_id,
int_current_batch_id,
int_run_id)
VALUES (%s,     -- 01 - name
        %s,     -- 02 - description
        %s,     -- 03 - data_type
        %s,     -- 04 - int_source_system
        %s,     -- 05 - int_source_object
        %s,     -- 06 - int_source_id
        %s,     -- 07 - int_source_batch_id
        %s,     -- 08 - int_current_batch_id
        %s)     -- 09 - int_run_id
returning int_taxonomy_id
"""

# select all hierarchies to import
sql_select_taxonomies_to_import = """
select      *
from        cdp_data_stg_taxonomy_summary_01_v
order by    int_current_batch_id asc
"""

# select the data for specific taxonomy to import
sql_select_taxonomy_to_import_by_load_id = """
select      *
from        cdp_data_stg_taxonomy_01_v
where       int_current_batch_id = %s
"""

# check if a taxonomy already exists (names must be unique)
sql_check_taxonomy_exists = """
select      int_taxonomy_id
from        cdp_data_taxonomy_01_v
where       name = %s
"""

sql_get_next_taxonomy_version = """
select      coalesce(max(version), 0) + 1
from        cdp_data_taxonomy_version
where       int_taxonomy_id = %s
"""

sql_select_taxonomy_level_01 = """
select      *
from        cdp_data_taxonomy_level_01
"""

sql_insert_taxonomy_level_01 = """
-- sql_insert_taxonomy_level_01
WITH params AS (
    SELECT  %s::int as int_run_id,
            %s::int as int_current_batch_id,
            %s::int as int_source_batch_id,
            %s::int as int_taxonomy_version_id
)
INSERT INTO cdp_data_taxonomy_level_01 (
int_run_id,
int_taxonomy_version_id,
name,
description,
int_source_system,
int_source_object,
int_source_id,
int_source_batch_id,
int_current_batch_id
)
SELECT      p.int_run_id                            as int_run_id,                          -- int_run_id
            p.int_taxonomy_version_id               as int_taxonomy_version_id,             -- int_taxonomy_version_id
            name                                    as name,                                -- name
            ''                                      as description,                         -- description                       
            'cdp_data_ctl'                          as int_source_system,                   -- int_source_system
            'get taxonomy name'                     as int_source_object,                   -- int_source_object
            'id'                                    as int_source_id,                       -- int_source_id
            p.int_source_batch_id                   as int_source_batch_id,                 -- int_source_batch_id
            p.int_current_batch_id                  as int_current_batch_id                 -- int_current_batch_id
FROM        cdp_data_stg_taxonomy_level_01_01_v     as  stg_level                           -- STAGING VIEW
JOIN        params                                  AS  p
            ON stg_level.int_current_batch_id = p.int_source_batch_id    -- stage table current batch should equal current source batch
returning   int_taxonomy_version_id,
            int_taxonomy_level_01_id,
            name
"""

sql_insert_taxonomy_level_01_rows = """
-- sql_insert_taxonomy_level_01_rows
WITH params AS (
    SELECT  %s::int as int_run_id,
            %s::int as int_current_batch_id,
            %s::int as int_source_batch_id,
            %s::int as int_taxonomy_version_id
)
SELECT      p.int_run_id                            as int_run_id,                          -- int_run_id
            p.int_taxonomy_version_id               as int_taxonomy_version_id,             -- int_taxonomy_version_id
            name                                    as name,                                -- name
            ''                                      as description,                         -- description                       
            'cdp_data_ctl'                          as int_source_system,                   -- int_source_system
            'get taxonomy name'                     as int_source_object,                   -- int_source_object
            'id'                                    as int_source_id,                       -- int_source_id
            p.int_source_batch_id                   as int_source_batch_id,                 -- int_source_batch_id
            p.int_current_batch_id                  as int_current_batch_id                 -- int_current_batch_id
FROM        cdp_data_stg_taxonomy_level_01_01_v     as  stg_level                           -- STAGING VIEW
JOIN        params                                  AS  p
            ON stg_level.int_current_batch_id = p.int_source_batch_id    -- stage table current batch should equal current source batch
"""

sql_insert_taxonomy_level_02 = """
WITH params AS (
    SELECT  %s::int as int_run_id,
            %s::int as int_current_batch_id,
            %s::int as int_source_batch_id,
            %s::int as int_taxonomy_version_id
)
INSERT INTO cdp_data_taxonomy_level_02 (
int_run_id,
int_taxonomy_version_id,
name,
description,
int_source_system,
int_source_object,
int_source_id,
int_source_batch_id,
int_current_batch_id,
int_taxonomy_level_01_id
)
SELECT      p.int_run_id,                               -- int_run_id
            p.int_taxonomy_version_id,                  -- int_taxonomy_version_id
            name,                                       -- name
            '',                                         -- description     
            'cdp_data_ctl',                             -- int_source_system
            'get taxonomy name',                        -- int_source_object
            'id',                                       -- int_source_id
            p.int_source_batch_id,                      -- int_source_batch_id
            p.int_current_batch_id,                     -- int_current_batch_id
            stg_level.int_taxonomy_level_01_id          -- int_taxonomy_level_01_id
FROM        cdp_data_stg_taxonomy_level_02_01_v         AS  stg_level
JOIN        params                                      AS  p
            ON stg_level.int_current_batch_id           = p.int_source_batch_id    -- stage table current batch should equal current source batch
WHERE       stg_level.int_taxonomy_version_id           = p.int_taxonomy_version_id
RETURNING   int_taxonomy_version_id,
            int_taxonomy_level_02_id,
            name
"""

sql_insert_taxonomy_level_02_rows = """
-- sql_insert_taxonomy_level_02_rows
WITH params AS (
    SELECT  %s::int as int_run_id,
            %s::int as int_current_batch_id,
            %s::int as int_source_batch_id,
            %s::int as int_taxonomy_version_id
)
SELECT      p.int_run_id,                               -- int_run_id
            p.int_taxonomy_version_id,                  -- int_taxonomy_version_id
            name,                                       -- name
            '',                                         -- description     
            'cdp_data_ctl',                             -- int_source_system
            'get taxonomy name',                        -- int_source_object
            'id',                                       -- int_source_id
            p.int_source_batch_id,                      -- int_source_batch_id
            p.int_current_batch_id,                     -- int_current_batch_id
            stg_level.int_taxonomy_level_01_id          -- int_taxonomy_level_01_id
FROM        cdp_data_stg_taxonomy_level_02_01_v         AS  stg_level
JOIN        params                                      AS  p
            ON stg_level.int_current_batch_id           = p.int_source_batch_id    -- stage table current batch should equal current source batch
WHERE       stg_level.int_taxonomy_version_id           = p.int_taxonomy_version_id
"""

sql_insert_taxonomy_level_03 = """
WITH params AS (
    SELECT  %s::int as int_run_id,
            %s::int as int_current_batch_id,
            %s::int as int_source_batch_id,
            %s::int as int_taxonomy_version_id
)
INSERT INTO cdp_data_taxonomy_level_03 (
int_run_id,
int_taxonomy_version_id,
name,
description,
int_source_system,
int_source_object,
int_source_id,
int_source_batch_id,
int_current_batch_id,
int_taxonomy_level_02_id
)
SELECT      p.int_run_id,                               -- int_run_id
            p.int_taxonomy_version_id,                 -- int_taxonomy_version_id
            name,                                       -- name
            '',                                         -- description     
            'cdp_data_ctl',                            -- int_source_system
            'get taxonomy name',                       -- int_source_object
            'id',                                       -- int_source_id
            p.int_source_batch_id,                      -- int_source_batch_id
            p.int_current_batch_id,                     -- int_current_batch_id
            stg_level.int_taxonomy_level_02_id         -- int_taxonomy_level_02_id
FROM        cdp_data_stg_taxonomy_level_03_01_v   AS  stg_level
JOIN        params                          AS  p
            ON stg_level.int_current_batch_id = p.int_source_batch_id    -- stage table current batch should equal current source batch
WHERE       stg_level.int_taxonomy_version_id           = p.int_taxonomy_version_id
RETURNING   int_taxonomy_version_id,
            int_taxonomy_level_02_id,
            name
"""

sql_insert_taxonomy_level_03_rows = """
WITH params AS (
    SELECT  %s::int as int_run_id,
            %s::int as int_current_batch_id,
            %s::int as int_source_batch_id,
            %s::int as int_taxonomy_version_id
)
SELECT      p.int_run_id,                               -- int_run_id
            p.int_taxonomy_version_id,                 -- int_taxonomy_version_id
            name,                                       -- name
            '',                                         -- description     
            'cdp_data_ctl',                            -- int_source_system
            'get taxonomy name',                       -- int_source_object
            'id',                                       -- int_source_id
            p.int_source_batch_id,                      -- int_source_batch_id
            p.int_current_batch_id,                     -- int_current_batch_id
            stg_level.int_taxonomy_level_02_id         -- int_taxonomy_level_02_id
FROM        cdp_data_stg_taxonomy_level_03_01_v   AS  stg_level
JOIN        params                          AS  p
            ON stg_level.int_current_batch_id = p.int_source_batch_id    -- stage table current batch should equal current source batch
WHERE       stg_level.int_taxonomy_version_id           = p.int_taxonomy_version_id
"""

sql_insert_taxonomy_level_04 = """
WITH params AS (
    SELECT  %s::int as int_run_id,
            %s::int as int_current_batch_id,
            %s::int as int_source_batch_id,
            %s::int as int_taxonomy_version_id
)
INSERT INTO cdp_data_taxonomy_level_04 (
int_run_id,
int_taxonomy_version_id,
name,
description,
int_source_system,
int_source_object,
int_source_id,
int_source_batch_id,
int_current_batch_id,
int_taxonomy_level_03_id
)
SELECT      p.int_run_id,                               -- int_run_id
            p.int_taxonomy_version_id,                 -- int_taxonomy_version_id
            name,                                       -- name
            '',                                         -- description     
            'cdp_data_ctl',                            -- int_source_system
            'get taxonomy name',                       -- int_source_object
            'id',                                       -- int_source_id
            p.int_source_batch_id,                      -- int_source_batch_id
            p.int_current_batch_id,                     -- int_current_batch_id
            stg_level.int_taxonomy_level_03_id         -- int_taxonomy_level_02_id
FROM        cdp_data_stg_taxonomy_level_04_01_v   AS  stg_level
JOIN        params                          AS  p
            ON stg_level.int_current_batch_id = p.int_source_batch_id    -- stage table current batch should equal current source batch
WHERE       stg_level.int_taxonomy_version_id           = p.int_taxonomy_version_id
RETURNING   int_taxonomy_version_id,
            int_taxonomy_level_03_id,
            name
"""

sql_insert_taxonomy_level_04_rows = """
WITH params AS (
    SELECT  %s::int as int_run_id,
            %s::int as int_current_batch_id,
            %s::int as int_source_batch_id,
            %s::int as int_taxonomy_version_id
)
SELECT      p.int_run_id,                               -- int_run_id
            p.int_taxonomy_version_id,                 -- int_taxonomy_version_id
            name,                                       -- name
            '',                                         -- description     
            'cdp_data_ctl',                            -- int_source_system
            'get taxonomy name',                       -- int_source_object
            'id',                                       -- int_source_id
            p.int_source_batch_id,                      -- int_source_batch_id
            p.int_current_batch_id,                     -- int_current_batch_id
            stg_level.int_taxonomy_level_03_id         -- int_taxonomy_level_02_id
FROM        cdp_data_stg_taxonomy_level_04_01_v   AS  stg_level
JOIN        params                          AS  p
            ON stg_level.int_current_batch_id = p.int_source_batch_id    -- stage table current batch should equal current source batch
WHERE       stg_level.int_taxonomy_version_id           = p.int_taxonomy_version_id
"""

sql_insert_taxonomy_level_05 = """
WITH params AS (
    SELECT  %s::int as int_run_id,
            %s::int as int_current_batch_id,
            %s::int as int_source_batch_id,
            %s::int as int_taxonomy_version_id
)
INSERT INTO cdp_data_taxonomy_level_05 (
int_run_id,
int_taxonomy_version_id,
name,
description,
int_source_system,
int_source_object,
int_source_id,
int_source_batch_id,
int_current_batch_id,
int_taxonomy_level_04_id
)
SELECT      p.int_run_id,                               -- int_run_id
            p.int_taxonomy_version_id,                 -- int_taxonomy_version_id
            name,                                       -- name
            stg_level.description,                          -- description     
            'cdp_data_ctl',                            -- int_source_system
            'get taxonomy name',                       -- int_source_object
            'id',                                       -- int_source_id
            p.int_source_batch_id,                      -- int_source_batch_id
            p.int_current_batch_id,                     -- int_current_batch_id
            stg_level.int_taxonomy_level_04_id         -- int_taxonomy_level_02_id
FROM        cdp_data_stg_taxonomy_level_05_01_v   AS  stg_level
JOIN        params                          AS  p
            ON stg_level.int_current_batch_id = p.int_source_batch_id    -- stage table current batch should equal current source batch
WHERE       stg_level.int_taxonomy_version_id           = p.int_taxonomy_version_id
RETURNING   int_taxonomy_version_id,
            int_taxonomy_level_04_id,
            name
"""

sql_insert_taxonomy_level_05_rows = """
WITH params AS (
    SELECT  %s::int as int_run_id,
            %s::int as int_current_batch_id,
            %s::int as int_source_batch_id,
            %s::int as int_taxonomy_version_id
)
SELECT      p.int_run_id,                                   -- int_run_id
            p.int_taxonomy_version_id,                      -- int_taxonomy_version_id
            name,                                           -- name
            stg_level.description,                          -- description     
            'cdp_data_ctl',                                 -- int_source_system
            'get taxonomy name',                            -- int_source_object
            'id',                                           -- int_source_id
            p.int_source_batch_id,                          -- int_source_batch_id
            p.int_current_batch_id,                         -- int_current_batch_id
            stg_level.int_taxonomy_level_04_id              -- int_taxonomy_level_02_id
FROM        cdp_data_stg_taxonomy_level_05_01_v   AS    stg_level
JOIN        params                          AS  p
            ON stg_level.int_current_batch_id = p.int_source_batch_id    -- stage table current batch should equal current source batch
WHERE       stg_level.int_taxonomy_version_id           = p.int_taxonomy_version_id
"""

sql_insert_taxonomy_version = """
INSERT INTO cdp_data_taxonomy_version (
int_taxonomy_id,
version,
int_source_system,
int_source_object,
int_source_id,
int_source_batch_id,
int_current_batch_id,
int_run_id)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
returning int_taxonomy_version_id
"""

