{{ config(
    tags=['partnerships'],
    schema='intermediate'
) }}

with 
    deduped_cte as (
    {{ dbt_utils.deduplicate(
        relation=source('kobo_partnerships','stakeholder_visit_record_partnership'),
        partition_by='_id',
        order_by='_submission_time desc',
    )}}
    ),

visit_data as (
SELECT 
    -- Visit Data
    (vd.data ->> '_id')::bigint as id,
    vd.data ->> '_status' as status,
    (vd.data ->> '_submission_time')::timestamp as submission_time,
    (vd.data ->> 'Date')::date as monitoring_date,

    vd.data ->> 'Name_FA_FO' as fa_fo_name,
    vd.data ->> '_submitted_by' as submitted_by,
    TRIM(REPLACE(REPLACE(vd.data->>'Name_of_the_partner', 'Apnalaya_NGO', ''), '  ', ' ')) AS partner_name,
    COALESCE(vd.data->>'Cluster_Name_Sparsha',vd.data->>'Cluster_Name_HALWA', vd.data->>'Cluster_Name_Vipla') AS cluster_name, 
    vd.data ->> 'Partcipant_type' as participant_type,
    (vd.data ->> 'Total_Staff_Present')::int as total_staff_present,
    (vd.data ->> 'Total_Staff_Present_Other')::int as total_staff_present_other,
    (vd.data ->> 'Staff_Details_count')::int as staff_details_count,
    vd.data ->> 'Name_of_Staffs' as name_of_staffs,
    vd.data ->> 'Details_Of_the_Visit/Name_Stakeholder' as stakeholder_name,
    vd.data ->> 'Details_Of_the_Visit/Designation_Stakeholder' as stakeholder_designation,
    vd.data ->> 'Details_Of_the_Visit/Level_Stakeholder' as stakeholder_level,
    vd.data ->> 'Details_Of_the_Visit/Place_of_the_visit' as place_of_visit,
    vd.data ->> 'Details_Of_the_Visit/Highlights_of_Visit' as highlights_of_visit,
    vd.data ->> 'Details_Of_the_Visit/Action_Points' as action_points

FROM deduped_cte as vd
)

SELECT * from visit_data 
   

