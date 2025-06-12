
# get the set of projects to have insights generated for them
# project_type = 'campaign_insight'
# add flag to project as to whether insight generation is turned on
# use this flag for now to determine test projects
# data returned should be 
get_projects_for_insight_generation = """
SELECT  project.project_id,
        project.name
FROM    cdp_app_project_01_v						as project
WHERE   generate_insight        = true
"""

get_project_leads_data = """
select 		opportunity_number						as  opportunity_number,
			lead_trx_paper_title					as  asset_title,
			lead_trx_organization_name				as  organization,
			lead_trx_job_sector  					as  job_sector,
			lead_trx_job_function					as  job_function
from 		cdp_app_delivery_lead_latest_project_01_v
where       project_id = %s
"""