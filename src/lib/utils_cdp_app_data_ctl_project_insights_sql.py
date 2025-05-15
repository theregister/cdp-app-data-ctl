
# want a set of projects to have insights generated for them
# project_type = 'campaign_insight'
# add flag to project as to whether insight generation is turned on
# use this flag for now to determine test projects
# data returned should be 
get_projects_for_insight_generation = """
SELECT  count(*)
FROM    cdp_app_data_project
WHERE   generate_insights       = 1
AND     project_type            = 'campaign_insight'

"""