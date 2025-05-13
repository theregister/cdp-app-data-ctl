
# LEVEL 01 SELECT (basic view over table)
sql_select_hierarchy_import = """
select *
from cdp_data_stg_taxonomy_summary_01_v
"""

sql_select_hierarchy_import_by_load_id = """
select *
from cdp_data_stg_taxonomy_01_v
where current_data_load_id = %s
"""

sql_check_hierarchy_exists = """
select *
from cdp_data_taxonomy_01_v
where name = %s
"""

sql_get_next_hierarchy_version = """
select coalesce(max(version), 0) + 1
from cdp_data_taxonomy_version_01_v
where hierarchy_id = %s
"""

# load level 01 hierarchy
# hierarchy_version_id,
# src_data_load_id,
# this_data_load_id
# src_data_load_id
sql_insert_hierarchy_level_01 = """
INSERT INTO cdp_data_taxonomy_level_01 (
hierarchy_version_id,
name,
description,
source_system,
source_object,
source_id,
source_data_load_id,
current_data_load_id)
SELECT      %s,
            name,
            '',
            'scc_data_ctl',
            'get hierarchy name',
            'id',
            source_data_load_id,
            %s
FROM        cdp_data_taxonomy_import_level_01_v
where       current_data_load_id = %s
returning   hierarchy_version_id, hierarchy_level_01_id, name
"""

sql_insert_hierarchy_level_01_rows = """
SELECT      %s,
            name,
            '',
            'scc_data_ctl',
            'get hierarchy name',
            'id',
            current_data_load_id,
            %s
FROM        cdp_data_taxonomy_import_level_01_v
where       current_data_load_id = %s
"""

# hierarchy_version_id,
# src_data_load_id,
# this_data_load_id
# src_data_load_id
sql_insert_hierarchy_level_02 = """
INSERT INTO cdp_data_taxonomy_level_02 (
hierarchy_version_id,
name,
description,
source_system,
source_object,
source_id,
source_data_load_id,
current_data_load_id,
hierarchy_level_01_id
)
SELECT      hierarchy_version_id,
            name,
            '',
            'scc_data_ctl',
            'get hierarchy name',
            'id',
            source_data_load_id,
            current_data_load_id,
            hierarchy_level_01_id
FROM        cdp_data_taxonomy_import_level_02_v
where       source_data_load_id = %s
and         current_data_load_id = %s
and         hierarchy_version_id = %s
returning hierarchy_version_id, hierarchy_level_02_id, name
"""

sql_insert_hierarchy_level_02_rows = """
SELECT      hierarchy_version_id,
            name,
            '',
            'scc_data_ctl',
            'get hierarchy name',
            'id',
            source_data_load_id,
            current_data_load_id,
            hierarchy_level_01_id
FROM        cdp_data_taxonomy_import_level_02_v
where       source_data_load_id = %s
and         current_data_load_id = %s
and         hierarchy_version_id = %s
"""

sql_insert_hierarchy_level_03 = """
INSERT INTO cdp_data_taxonomy_level_03 (
hierarchy_version_id,
name,
description,
source_system,
source_object,
source_id,
source_data_load_id,
current_data_load_id,
hierarchy_level_02_id
)
SELECT      hierarchy_version_id,
            name,
            '',
            'scc_data_ctl',
            'get hierarchy name',
            'id',
            source_data_load_id,
            current_data_load_id,
            hierarchy_level_02_id
FROM        cdp_data_taxonomy_import_level_03_v
where       source_data_load_id = %s
and         current_data_load_id = %s
and         hierarchy_version_id = %s
returning hierarchy_version_id, hierarchy_level_03_id, name
"""

sql_insert_hierarchy_level_03_rows = """
SELECT      hierarchy_version_id            as hierachy_version_id,
            name                            as name,
            ''                              as description,
            'scc_data_ctl'                  as source_system,
            'get hierarchy name'            as source_object,
            'id'                            as source_id,   
            source_data_load_id             as source_data_load_id,
            current_data_load_id            as current_data_load_id,
            hierarchy_level_02_id           as hierarchy_level_02_id
FROM        cdp_data_taxonomy_import_level_03_v
where       source_data_load_id = %s
and         current_data_load_id = %s
and         hierarchy_version_id = %s
"""

sql_insert_hierarchy_level_04 = """
INSERT INTO cdp_data_taxonomy_level_04 (
hierarchy_version_id,
name,
description,
source_system,
source_object,
source_id,
source_data_load_id,
current_data_load_id,
hierarchy_level_03_id
)
SELECT      %s,
            name,
            '',
            'scc_data_ctl',
            'get hierarchy name',
            'id',
            %s,
            %s,
            hierarchy_level_03_id
FROM cdp_data_taxonomy_import_level_04_v
where       source_data_load_id = %s
and         current_data_load_id    = %s
returning hierarchy_version_id, hierarchy_level_04_id, name
"""

sql_insert_hierarchy_level_04_rows = """
SELECT      %s,
            name,
            '',
            'scc_data_ctl',
            'get hierarchy name',
            'id',
            %s,
            %s,
            hierarchy_level_03_id
FROM cdp_data_taxonomy_import_level_04_v
where       source_data_load_id = %s
and         current_data_load_id    = %s
"""

sql_insert_hierarchy_level_05 = """
INSERT INTO cdp_data_taxonomy_level_05 (
hierarchy_version_id,
name,
description,
source_system,
source_object,
source_id,
source_data_load_id,
current_data_load_id,
hierarchy_level_04_id
)
SELECT      %s,
            name,
            '',
            'scc_data_ctl',
            'get hierarchy name',
            'id',
            %s,
            %s,
            hierarchy_level_04_id
FROM cdp_data_taxonomy_import_level_05_v
where       source_data_load_id = %s
and         current_data_load_id    = %s
returning hierarchy_version_id, hierarchy_level_05_id, name
"""

sql_insert_hierarchy_level_04_rows = """
SELECT      %s,
            name,
            '',
            'scc_data_ctl',
            'get hierarchy name',
            'id',
            %s,
            %s,
            hierarchy_level_04_id
FROM cdp_data_taxonomy_import_level_05_v
where       source_data_load_id = %s
and         current_data_load_id    = %s
"""

sql_insert_hierarchy = """
INSERT INTO scc_hierarchy (
name,
description,
data_type,
source_system,
source_object,
source_id,
source_data_load_id,
current_data_load_id)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
returning hierarchy_id, name
"""

sql_insert_hierarchy_version = """
INSERT INTO cdp_data_taxonomy_version (
hierarchy_id,
version,
source_system,
source_object,
source_id,
source_data_load_id,
current_data_load_id)
VALUES (%s, %s, %s, %s, %s, %s, %s)
returning hierarchy_version_id, version
"""

sql_insert_hierarchy_node_level_05 = """
INSERT INTO cdp_data_taxonomy_node (
hierarchy_version_id,
name,
description,
source_system,
source_object,
source_id,
source_data_load_id,
current_data_load_id)
)
VALUES  (%s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s)
returning hierarchy_node_id, name
"""

sql_insert_hierarchy_node_level_04 = """
INSERT INTO cdp_data_taxonomy_node (
hierarchy_version_id,
name,
description,
source_system,
source_object,
source_id
)
VALUES  (%s,
        %s,
        %s,
        %s,
        %s,
        %s)
returning hierarchy_node_id, name
"""

sql_insert_hierarchy_node_level_03 = """
INSERT INTO cdp_data_taxonomy_node (
hierarchy_version_id,
name,
description,
source_system,
source_object,
source_id
)
VALUES  (%s,
        %s,
        %s,
        %s,
        %s,
        %s)
returning hierarchy_node_id, name
"""

sql_insert_hierarchy_node_level_02 = """
INSERT INTO cdp_data_taxonomy_node (
hierarchy_version_id,
name,
description,
source_system,
source_object,
source_id
)
VALUES  (%s,
        %s,
        %s,
        %s,
        %s,
        %s)
returning hierarchy_node_id, name
"""

sql_insert_hierarchy_node_level_01 = """
INSERT INTO cdp_data_taxonomy_node (
hierarchy_version_id,
name,
description,
source_system,
source_object,
source_id
)
VALUES  (%s,
        %s,
        %s,
        %s,
        %s,
        %s)
returning hierarchy_node_id, name
"""

