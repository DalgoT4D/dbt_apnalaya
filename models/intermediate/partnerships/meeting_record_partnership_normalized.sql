{{ config(
    tags=['partnerships'],
    schema='intermediate'
) }}

with 
    deduped_cte as (
    {{ dbt_utils.deduplicate(
        relation=source('kobo_partnerships','meeting_record_partnership'),
        partition_by='_id',
        order_by='_submission_time desc',
    )}}
    ),

    deduped_cte_new as (
    {{ dbt_utils.deduplicate(
        relation=source('kobo_partnerships','meeting_record_partnership_new'),
        partition_by='_id',
        order_by='_submission_time desc',
    )}}
    ),

meeting_data as (
SELECT 
    -- Meeting Data
    (md.data ->> '_id')::int as id,
   (md.data ->> 'Date')::date as monitoring_date,
    md.data ->> '_status' as status,
    md.data ->> 'Venue_Name' as venue_name,
    md.data ->> '_submitted_by' as submitted_by,
    md.data ->> 'Name_of_Staffs' as name_of_staffs,
    (md.data ->> 'Date_Next_Visit')::date as date_next_visit,
    md.data ->> 'Partcipant_type' as participant_type,
    (md.data ->> '_submission_time')::timestamp as submission_time,
    md.data ->> 'Name_Data_Collector' as data_collector,
    md.data ->> 'Name_of_the_partner' as partner_name,
    TRIM(BOTH ',' FROM REPLACE(REPLACE(md.data->>'Name_of_the_partner', 'Apnalaya_NGO', ''), ' ', ',')) AS n_partner_name,
    (md.data ->> 'Total_Staff_Present')::int as total_staff_present,
    (md.data ->> 'Total_Staff_Present_Other')::int as total_staff_present_other,
    (md.data ->> 'Number_of_Facilitators')::int as number_of_facilitators,
    (md.data ->> 'Total_Community_Members_Present')::int as total_community_members_present,
    md.data ->> 'Meeting_Details/Meeting_Type' as meeting_type,
    md.data ->> 'Meeting_Details/Action_Points' as action_points,
    md.data ->> 'Meeting_Details/Meeting_Purpose' as meeting_purpose,
    md.data ->> 'Meeting_Details/Topics_discussed_during_the_meeting' as topics_discussed
FROM deduped_cte as md
),

meeting_data_new as (
SELECT 
    -- Meeting Data
    (md.data ->> '_id')::int as id,
   (md.data ->> 'Date')::date as monitoring_date,
    md.data ->> '_status' as status,
    md.data ->> 'Venue_Name' as venue_name,
    md.data ->> '_submitted_by' as submitted_by,
    md.data ->> 'Name_of_Staffs' as name_of_staffs,
    (md.data ->> 'Date_Next_Visit')::date as date_next_visit,
    md.data ->> 'Partcipant_type' as participant_type,
    (md.data ->> '_submission_time')::timestamp as submission_time,
    md.data ->> 'Name_Data_Collector' as data_collector,
    md.data ->> 'Name_of_the_partner' as partner_name,
    TRIM(BOTH ',' FROM REPLACE(REPLACE(md.data->>'Name_of_the_partner', 'Apnalaya_NGO', ''), ' ', ',')) AS n_partner_name,
    (md.data ->> 'Total_Staff_Present')::int as total_staff_present,
    (md.data ->> 'Total_Staff_Present_Other')::int as total_staff_present_other,
    (md.data ->> 'Number_of_Facilitators')::int as number_of_facilitators,
    (md.data ->> 'Total_Community_Members_Present')::int as total_community_members_present,
    md.data ->> 'Meeting_Details/Meeting_Type' as meeting_type,
    md.data ->> 'Meeting_Details/Action_Points' as action_points,
    md.data ->> 'Meeting_Details/Meeting_Purpose' as meeting_purpose,
    md.data ->> 'Meeting_Details/Topics_discussed_during_the_meeting' as topics_discussed
FROM deduped_cte_new as md
),

meeting_data_unioned as (SELECT * from meeting_data 
UNION SELECT * from meeting_data_new)

SELECT *, DATE_TRUNC('month', monitoring_date)::date AS monitoring__month from meeting_data_unioned 
   

