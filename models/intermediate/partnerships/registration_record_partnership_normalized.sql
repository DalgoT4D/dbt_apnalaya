{{ config(
    tags=['partnerships'],
    schema='intermediate'
) }}

with 
    deduped_cte as (
    {{ dbt_utils.deduplicate(
        relation=source('kobo_partnerships','registration_record_partnership'),
        partition_by='_id',
        order_by='_submission_time desc',
    )}}
    ),

family_data as (
SELECT 
    -- Household-Level Data
    (fd.data->>'_id')::int AS household_id,
    (fd.data->>'Date')::date AS monitoring_date,
    fd.data->>'Name_FA_FO_002' AS primary_contact_name,
    COALESCE(fd.data->>'Cluster_Name',fd.data->>'Cluster_Name_001', fd.data->>'Cluster_Name_002') AS cluster_name, 
    (fd.data->>'Contact_Number_Y')::bigint AS contact_number,
    fd.data->>'Name_of_the_partner' AS partner_name,
    fd.data->>'Details_Family/Religion' AS religion,
    fd.data->>'Details_Family/Social_Category' AS social_category,
    (fd.data->>'Number_of_Family_Members')::int AS number_of_family_members,
    fd.data->>'Address_Landmark' AS address_landmark,
    fd.data->>'Address_Lane_Number' AS address_lane_number,
    fd.data->>'Address_Room_House_Number' AS address_room_house_number,
    fd.data->>'Details_Family/have_rationcard' AS has_ration_card,
    fd.data->>'Details_Family/Type_of_Bathroom' AS type_of_bathroom,
    fd.data->>'Details_Family/Type_of_Residence' AS type_of_residence,
    fd.data->>'Details_Family/Type_of_Toilet_Facility' AS type_of_toilet_facility,
    fd.data->>'Details_Family/Community_Toilet_Facility' AS community_toilet_facility,
    fd.data->>'Details_Family/Main_Source_Drinking_Water' AS main_source_drinking_water,
    fd.data->>'Details_Family/Main_Water_Purification_Method' AS main_water_purification_method,
    fd.data->>'Details_Family/Living_in_Shivaji_Nagar_Since_When' AS living_in_shivaji_nagar_since,
    fd.data->>'media_consent' AS media_consent,
    fd.data->>'calculation' AS calculation,
    fd.data->>'_status' AS submission_status,
    (fd.data->>'_submission_time')::timestamp AS submission_time,
    fd.data->>'_submitted_by' AS submitted_by,

    -- Family Member-Level Data
    fm->>'Family_Members_Details/First_Name' AS first_name,
    fm->>'Family_Members_Details/Middle_Name' AS middle_name,
    fm->>'Family_Members_Details/Last_Name' AS last_name,
    (fm->>'Family_Members_Details/Age')::int AS age,
    fm->>'Family_Members_Details/Sex' AS sex,
    fm->>'Family_Members_Details/Marital_Status' AS marital_status,
    fm->>'Family_Members_Details/year_at_Marriage' AS year_at_marriage,
    fm->>'Family_Members_Details/Educational_Level' AS education_level,
    fm->>'Family_Members_Details/Occupation_Status' AS occupation_status,
    fm->>'Family_Members_Details/Type_of_Occupation' AS type_of_occupation,
    (fm->>'Family_Members_Details/Monthly_Income')::int AS monthly_income,
    fm->>'Family_Members_Details/pan_card' AS has_pan_card,
    fm->>'Family_Members_Details/voter_id' AS has_voter_id,
    fm->>'Family_Members_Details/aadhar_card' AS has_aadhar_card,
    fm->>'Family_Members_Details/Volunteer' AS is_volunteer,
    fm->>'Family_Members_Details/Presence_of_Disability' AS has_disability
FROM deduped_cte as fd, 
    LATERAL jsonb_array_elements(fd.data->'Family_Members_Details') AS fm
)

SELECT *, CONCAT(household_id,CONCAT('_',first_name)) as unique_id from family_data