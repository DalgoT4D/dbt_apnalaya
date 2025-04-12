{{ config(
    tags=['partnerships'],
    schema='intermediate'
) }}

with 
    deduped_cte as (
    {{ dbt_utils.deduplicate(
        relation=source('kobo_partnerships','hl_civic_action_record_partnership'),
        partition_by='_id',
        order_by='_submission_time desc',
    )}}
    ),
    deduped_cte_new as (
    {{ dbt_utils.deduplicate(
        relation=source('kobo_partnerships','hl_civic_action_record_partnership_new'),
        partition_by='_id',
        order_by='_submission_time desc',
    )}}
    ),

civic_action_data as (
SELECT 
    -- Civic Action Data
    (hlca.data ->> '_id')::bigint as id,
    hlca.data ->> '_uuid' as uuid,
    hlca.data ->> '_status' as status,
    (hlca.data ->> '_submission_time')::timestamp as submission_time,
    (hlca.data ->> 'Civic_Action_Date')::date as monitoring_date,
    hlca.data ->> 'Name_FA_FO' as fa_fo_name,
    COALESCE(hlca.data->>'Cluster_Name',hlca.data->>'Cluster_Name_001', hlca.data->>'Cluster_Name_002') AS cluster_name, 
    COALESCE(hlca.data->>'Group_Name_Vipla',hlca.data->>'Group_Name_HALWA', hlca.data->>'Group_Name_Sparsha') AS group_name, 
    hlca.data ->> '_submitted_by' as submitted_by,
    hlca.data ->> 'Name_of_the_partner' as partner_name,
    hlca.data ->> 'Civic_Action_Stage_' as civic_action_stage,
    hlca.data ->> 'group_zu7uw03/Purpose_of_civic_action' as action,
    hlca.data ->> 'group_zu7uw03/id' as group_id,
    hlca.data ->> 'group_zu7uw03/First_Name_001' as first_name,
    hlca.data ->> 'group_zu7uw03/Middle_Name_001' as middle_name,
    hlca.data ->> 'group_zu7uw03/Last_Name_001' as last_name,
    hlca.data ->> 'group_zu7uw03/Full_name' as full_name,
    hlca.data ->> 'group_zu7uw03/Sex' as sex,
    (hlca.data ->> 'group_zu7uw03/Age')::int as age,
    hlca.data ->> 'group_zu7uw03/year_' as group_year,
    hlca.data ->> 'group_zu7uw03/Action' as civic_action,
    hlca.data ->> 'group_zu7uw03/gr_name_' as group_internal_name,
    hlca.data ->> 'group_zu7uw03/civic_action_code' as civic_action_code,
    hlca.data ->> 'group_zu7uw03/Unique_ID' as unique_id,
    hlca.data ->> 'group_zu7uw03/Purpose_of_civic_action' as purpose_civic_action,
    hlca.data ->> 'group_zu7uw03/Register_or_not' as is_registered,
    hlca.data ->> 'Details_Civic_Action/Activity' as civic_activity,
    hlca.data ->> 'Details_Civic_Action/Place_of_submission' as submission_place,
    hlca.data ->> 'Details_Civic_Action/Method_of_submission' as submission_method,
    hlca.data ->> 'Details_Civic_Action/Name_of_the_government_department' as department_name,

    hlca.data ->> 'Details_Participants/Ashayein_group_member' as ashayein_group_member,
    (hlca.data ->> 'Details_Participants/Number_Volunteers_Who_Contributed_Participated')::int as volunteer_count,
    (hlca.data ->> 'Details_Participants/Number_Community_Members_Who_Contributed_Participated')::int as community_member_count

FROM deduped_cte as hlca
),

civic_action_data_new as (
SELECT 
    -- Civic Action Data
    (hlca.data ->> '_id')::bigint as id,
    hlca.data ->> '_uuid' as uuid,
    hlca.data ->> '_status' as status,
    (hlca.data ->> '_submission_time')::timestamp as submission_time,
    (hlca.data ->> 'Civic_Action_Date')::date as monitoring_date,
    hlca.data ->> 'Name_FA_FO' as fa_fo_name,
    hlca.data->>'Cluster_Name' AS cluster_name, 
    hlca.data->>'group_name' AS group_name, 
    hlca.data ->> '_submitted_by' as submitted_by,
    hlca.data ->> 'name_of_the_partner' as partner_name,
    hlca.data ->> 'Civic_Action_Stage_' as civic_action_stage,
    hlca.data ->> 'group_zu7uw03/Purpose_of_civic_action' as action,
    hlca.data ->> 'group_zu7uw03/id' as group_id,
    hlca.data ->> 'group_zu7uw03/First_Name_001' as first_name,
    hlca.data ->> 'group_zu7uw03/Middle_Name_001' as middle_name,
    hlca.data ->> 'group_zu7uw03/Last_Name_001' as last_name,
    hlca.data ->> 'group_zu7uw03/Full_name' as full_name,
    hlca.data ->> 'group_zu7uw03/Sex' as sex,
    (hlca.data ->> 'group_zu7uw03/Age')::int as age,
    hlca.data ->> 'group_zu7uw03/year_' as group_year,
    hlca.data ->> 'group_zu7uw03/Action' as civic_action,
    hlca.data ->> 'group_zu7uw03/gr_name_' as group_internal_name,
    hlca.data ->> 'group_zu7uw03/civic_action_code' as civic_action_code,
    hlca.data ->> 'group_zu7uw03/Unique_ID' as unique_id,
    hlca.data ->> 'group_zu7uw03/Purpose_of_civic_action' as purpose_civic_action,
    hlca.data ->> 'group_zu7uw03/Register_or_not' as is_registered,
    hlca.data ->> 'Details_Civic_Action/Activity' as civic_activity,
    hlca.data ->> 'Details_Civic_Action/Place_of_submission' as submission_place,
    hlca.data ->> 'Details_Civic_Action/Method_of_submission' as submission_method,
    hlca.data ->> 'Details_Civic_Action/Name_of_the_government_department' as department_name,

    hlca.data ->> 'Details_Participants/Ashayein_group_member' as ashayein_group_member,
    (hlca.data ->> 'Details_Participants/Number_Volunteers_Who_Contributed_Participated')::int as volunteer_count,
    (hlca.data ->> 'Details_Participants/Number_Community_Members_Who_Contributed_Participated')::int as community_member_count

FROM deduped_cte_new as hlca
)

SELECT * from civic_action_data 
UNION SELECT * from civic_action_data_new
   

