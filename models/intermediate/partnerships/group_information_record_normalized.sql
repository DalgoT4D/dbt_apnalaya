WITH 
deduped_cte AS (
    {{ dbt_utils.deduplicate(
        relation=source('kobo_partnerships', 'group_record_partnership'), 
        partition_by='_id',
        order_by='_submission_time desc'
    )}}
),
base_data AS (
    SELECT 
        (data->>'_id')::bigint AS id,
        (data->>'Date')::date AS monitoring_date,
        (data->>'Name_of_the_partner')::text AS name_of_the_partner,
        (data->>'Name_of_the_partner_Other')::text AS name_of_the_partner_other,

        COALESCE(
            data->>'Group_Name_Sparsha', 
            data->>'Group_Name_HALWA', 
            data->>'Group_Name_Vipla', 
            data->>'Group_Name_Abhyudaya'
        )::text AS group_name,
        COALESCE(
            data->>'Cluster_Name', 
            data->>'Cluster_Name_001', 
            data->>'Cluster_Name_002', 
            data->>'Cluster_Name_003'
        )::text AS cluster_name,

        (data->>'individual_or_group')::text AS individual_or_group,
        (data->>'Group_Detail')::text AS group_detail,
        (data->>'Group_Formation_Date')::date AS group_formation_date,
        (data->>'Number_of_Members')::integer AS number_of_members,

        (data->'Details_of_Group_Member')::jsonb AS details_of_group_members,

        (data->>'Group_Discontinuation_Date')::date AS group_discontinuation_date,
        (data->>'Reason_for_Discontinuation')::text AS reason_for_discontinuation,
        (data->>'Specifiy_Other_Reas_for_Discon')::text AS specify_other_reason_for_discontinuation,
        (data->>'Reason_for_filling_a_record')::text AS reason_for_filling_record,

        (data->'Details_of_Group_Member_001')::jsonb AS details_of_discontinued_members,

        (data->>'Reason_for_leaving_the_group')::text AS reason_for_leaving_group,
        (data->>'Reason_for_leaving')::text AS reason_for_leaving_other,

        COALESCE(
            data->>'Name_FA_FO', 
            data->>'Name_FA_FO_001', 
            data->>'Name_FA_FO_002', 
            data->>'Name_FA_FO_003'
        )::text AS name_fa_fo
    FROM 
        deduped_cte
)
SELECT
    bd.id,
    bd.monitoring_date,
    bd.name_of_the_partner,
    bd.name_of_the_partner_other,
    bd.group_name,
    bd.cluster_name,
    bd.individual_or_group,
    bd.group_detail,
    bd.group_formation_date,
    bd.number_of_members,
    member.value->>'Details_of_Group_Member/First_Name' AS member_first_name,
    member.value->>'Details_of_Group_Member/Middle_Name' AS member_middle_name,
    member.value->>'Details_of_Group_Member/Last_Name' AS member_last_name,
    member.value->>'Details_of_Group_Member/Unique_ID' AS member_unique_id,
    member.value->>'Details_of_Group_Member/Sex' AS member_sex,
    (member.value->>'Details_of_Group_Member/Age')::int AS member_age,
    bd.group_discontinuation_date,
    bd.reason_for_discontinuation,
    bd.specify_other_reason_for_discontinuation,
    bd.reason_for_filling_record,
    bd.details_of_discontinued_members,
    bd.reason_for_leaving_group,
    bd.reason_for_leaving_other,
    bd.name_fa_fo
FROM base_data bd
LEFT JOIN LATERAL jsonb_array_elements(bd.details_of_group_members) AS member(value) ON TRUE