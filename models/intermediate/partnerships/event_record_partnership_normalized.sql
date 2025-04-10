WITH 
    deduped_cte AS (
    {{ dbt_utils.deduplicate(
        relation=source('kobo_partnerships','event_record_partnership'),
        partition_by='_id',
        order_by='_submission_time desc'
    )}}
    ),

event_data AS (
SELECT 
    -- Event Data
    (ed.data->>'_id')::int AS id,
    (ed.data->>'Date')::date AS monitoring_date,
    ed.data->>'Event_Name' AS event_name,
    ed.data->>'Event_Purpose' AS event_purpose,
    ed.data->>'Outcomes_of_Event' AS outcomes_of_event,
    ed.data->>'Name_Data_Collector' AS data_collector,
    (ed.data->>'Number_Event_Activities')::int AS number_of_activities,
    (ed.data->>'Event_Activity_Details_count')::int AS activity_count,
    ed.data->>'_submitted_by' AS submitted_by,
    (ed.data->>'_submission_time')::timestamp AS submission_time,

    -- Activity-Level Data
    activity->>'Event_Activity_Details/Activity_Name' AS activity_name,
    (activity->>'Event_Activity_Details/Indirect_contact')::int AS indirect_contact,
    activity->>'Event_Activity_Details/Participant_type' AS participant_type,
    COALESCE(activity->>'Event_Activity_Details/Cluster_Name_HALWA',activity->>'Event_Activity_Details/Cluster_Name_Sparsha',activity->>'Event_Activity_Details/Cluster_Name_Vipla') AS cluster_name,
    TRIM(BOTH ',' FROM REPLACE(REPLACE(activity->>'Event_Activity_Details/Name_of_the_partner', 'Apnalaya_NGO', ''), ' ', ',')) AS n_partner_name,
    TRIM(REPLACE(REPLACE(activity->>'Event_Activity_Details/Name_of_the_partner', 'Apnalaya_NGO', ''), '  ', ' ')) AS partner_name,
    (activity->>'Event_Activity_Details/Total_Community_Members_Present')::int AS total_community_members_present
FROM deduped_cte AS ed, 
    LATERAL jsonb_array_elements(ed.data->'Event_Activity_Details') AS activity
)

-- Normalize partner name when blank separated strings are equivalent
SELECT e.*, DATE_TRUNC('month', e.monitoring_date)::date AS monitoring__month
FROM event_data AS e