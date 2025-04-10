{{ config(
    tags=['partnerships'],
    schema='intermediate'
) }}

with 
    deduped_cte as (
    {{ dbt_utils.deduplicate(
        relation=source('kobo_partnerships','cl_civic_action_record_partnership'),
        partition_by='_id',
        order_by='_submission_time desc',
    )}}
    ),

civic_action_data as (
SELECT 
    -- Civic Action Data
    (clca.data ->> '_id')::bigint as id,
    clca.data ->> '_status' as status,
    (clca.data ->> '_submission_time')::timestamp as submission_time,
    (clca.data ->> 'Civic_Action_Date')::date as monitoring_date,
    clca.data ->> 'Name_FA_FO' as fa_fo_name,
    COALESCE(clca.data->>'Cluster_Name',clca.data->>'Cluster_Name_001', clca.data->>'Cluster_Name_002') AS cluster_name, 
    COALESCE(clca.data->>'Group_Name_Vipla',clca.data->>'Group_Name_HALWA', clca.data->>'Group_Name_Sparsh') AS group_name, 
    clca.data ->> '_submitted_by' as submitted_by,
    clca.data ->> 'Name_of_the_partner' as partner_name,
    clca.data ->> 'Civic_Action_Stage_' as civic_action_stage,
    clca.data ->> 'group_zu7uw03/id' as group_id,
    clca.data ->> 'group_zu7uw03/year_' as group_year,
    clca.data ->> 'group_zu7uw03/Action' as action_taken,
    clca.data ->> 'group_zu7uw03/gr_name_' as group_name_internal,
    clca.data ->> 'group_zu7uw03/civic_action_code' as civic_action_code,
    clca.data ->> 'Details_Civic_Action/Activity' as civic_activity,
    clca.data ->> 'Details_Civic_Action/Place_of_submission' as submission_place,
    clca.data ->> 'Details_Civic_Action/Method_of_submission' as submission_method,
    clca.data ->> 'Details_Civic_Action/Name_of_the_government_department' as department_name,
    (clca.data ->> 'Number_of_families_supposed_to_be_benefited')::int as number_of_families_benefited,
    (clca.data ->> 'Details_Participants/Number_Volunteers_Who_Contributed_Participated')::int as volunteer_count,
    (clca.data ->> 'Details_Participants/Number_Community_Members_Who_Contributed_Participated')::int as community_member_count,
    clca.data ->> 'Details_Participants/Swatantra_group_member' as group_members

FROM deduped_cte as clca
)

SELECT * from civic_action_data 
   

