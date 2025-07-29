{{ config(
    tags=['partnerships'],
    schema='intermediate'
) }}

with 
    deduped_cte as (
    {{ dbt_utils.deduplicate(
        relation=source('kobo_partnerships','interaction_record_partnership'),
        partition_by='_id',
        order_by='_submission_time desc',
    )}}
    ),
        deduped_cte_new as (
    {{ dbt_utils.deduplicate(
        relation=source('kobo_partnerships','interaction_record_partnership_new'),
        partition_by='_id',
        order_by='_submission_time desc',
    )}}
    ),

interaction_data as (
SELECT 
    -- Interaction Data
    (md.data ->> '_id')::bigint as id,
    md.data ->> '_status' as status,
    (md.data ->> '_submission_time')::timestamp as submission_time,
    (md.data ->> 'Date_Visit')::date as monitoring_date,
    md.data ->> 'highlights' as highlights,
    md.data ->> 'action_taken' as action_taken,
    md.data ->> '_submitted_by' as submitted_by,
    COALESCE(md.data->>'Cluster_Name',md.data->>'Cluster_Name_001', md.data->>'Cluster_Name_002') AS cluster_name, 
    md.data ->> 'Interaction_Type_' as interaction_type,
    md.data ->> 'Interaction_Purpose_' as interaction_purpose,
    case 
    when (md.data ->> 'Interaction_Purpose_') LIKE '%Monitoring%' then 'Monitoring Visit'
    when (md.data ->> 'Interaction_Purpose_') NOT LIKE '%Absent%' then 'Program Visit'
    else 'Other'
    end as visit_type,
    md.data ->> 'monitoring_purpose' as monitoring_purpose,
    (md.data ->> 'monitoring_staff_count')::int as monitoring_staff_count,
    (md.data ->> 'Number_of_Staff_Visit_for_monitoring')::int as number_of_staff_visits,
    md.data ->> 'Name_Data_Collector' as data_collector,
    TRIM(BOTH ',' FROM REPLACE(REPLACE(md.data->>'Name_of_the_partner', 'Apnalaya_NGO', ''), ' ', ',')) AS partner_name

FROM deduped_cte as md
),

interaction_data_new as (
SELECT 
    -- Interaction Data
    (md.data ->> '_id')::bigint as id,
    md.data ->> '_status' as status,
    (md.data ->> '_submission_time')::timestamp as submission_time,
    (md.data ->> 'Date_Visit')::date as monitoring_date,
    md.data ->> 'highlights' as highlights,
    md.data ->> 'action_taken' as action_taken,
    md.data ->> '_submitted_by' as submitted_by,
    md.data->> 'Cluster_Name_' AS cluster_name, 
    md.data ->> 'Interaction_Type_' as interaction_type,
    md.data ->> 'Interaction_Purpose_' as interaction_purpose,
    case 
    when (md.data ->> 'Interaction_Purpose_') LIKE '%Monitoring%' then 'Monitoring Visit'
    when (md.data ->> 'Interaction_Purpose_') NOT LIKE '%Absent%' then 'Program Visit'
    else 'Other'
    end as visit_type,
    md.data ->> 'monitoring_purpose' as monitoring_purpose,
    (md.data ->> 'monitoring_staff_count')::int as monitoring_staff_count,
    (md.data ->> 'Number_of_Staff_Visit_for_monitoring')::int as number_of_staff_visits,
    md.data ->> 'Name_Data_Collector' as data_collector,
    TRIM(BOTH ',' FROM REPLACE(REPLACE(md.data->>'Name_of_the_partner', 'Apnalaya_NGO', ''), ' ', ',')) AS partner_name

FROM deduped_cte_new as md
),

interaction_data_unioned as (SELECT * from interaction_data 
UNION SELECT * from interaction_data_new)

SELECT *,DATE_TRUNC('month', monitoring_date)::date AS monitoring__month from interaction_data_unioned 
   

