
data_version_insert_new = """
insert into cdp_data_data_version (
int_type,
int_source_type,
int_run_id,
int_source_batch_id,
int_current_batch_id
)
VALUES (
'test_type',
'test_source_type',
%s,     -- int run id
-1,     -- int source batch id
%s)     -- int current batch id
returning int_data_version_id
"""

data_version_leads_delivered_insert = """
INSERT INTO cdp_data_data_version_leads_delivered_01_01_v
SELECT  %s::int as int_data_version_id,
        *
FROM    cdp_data_pub_leads_delivered_01_v
"""

data_version_ggladmanager_insert = """
INSERT INTO cdp_data_data_version_ggladmanager_detail_01_01_v
SELECT  %s::int as int_data_version_id,
        *
FROM    cdp_data_pub_ggladmanager_detail_01_v
"""
