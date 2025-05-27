
# want a set of projects to have insights generated for them
# project_type = 'campaign_insight'
# add flag to project as to whether insight generation is turned on
# use this flag for now to determine test projects
# data returned should be 
get_projects_for_insight_generation = """
SELECT  project_id,
        project_name
FROM    cdp_app_project_01_v
WHERE   generate_insight        = true
AND     type                    = 'campaign_insight'
"""

