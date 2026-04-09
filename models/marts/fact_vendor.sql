{{
  config(
    materialized='incremental',
    schema='public',
    unique_key='vendor_id_poc_id',
    incremental_strategy='merge',
    merge_update_columns=[
      'client_company_id', 'vendor_name', 'vendor_email', 'primary_contact_phone', 'gst_no', 'city_name', 'country_name',
      'address', 'vendor_created_at', 'vendor_type', 'joining_status_label', 'category_names',
      'invited_by', 'invited_by_name', 'invited_by_email', 'poc_name', 'poc_email', 'poc_phone',
      'tag_names', 'preferred_item_ids', 'score_value', 'vendor_code', 'network_joined_date', 'source', 
      'is_verified', 'misc', 'archived_at', 'updated_at'
    ]
  )
}}

WITH final_vendor_data AS (
  -- Combine all vendor information with each POC as a separate row
  SELECT 
    vc.id,
    vc.name,
    vc.email,
    vc.phone,
    vc.gst_no,
    vc.address,
    vc.created_at,
    vc.category,
    vc.created_by,
    vc.is_verified,
    vc.misc,
    vc.city_name,
    vc.country_name,
    vc.vendor_code,
    vc.mapping_status,
    vc.network_joined_date,
    vc.source,
    vc.invited_by,
    vpc.category_names,
    vt.tag_names,
    pvm.preferred_item_ids,
    vs.score_value,
    ibu.invited_by_name,
    ibu.invited_by_email,
    -- POC details (one per row)
    vap.poc_id,
    vap.poc_name,
    vap.poc_email,
    vap.poc_phone,
    vap.poc_created_at,
    vap.poc_sequence,
    -- Calculate joining status label
    CASE 
      WHEN vc.mapping_status = 0 THEN 'Not Initiated'
      WHEN vc.mapping_status = 1 THEN 'In Progress'
      WHEN vc.mapping_status = 2 THEN 'Review Pending'
      WHEN vc.mapping_status = 3 THEN 'Approval Pending'
      WHEN vc.mapping_status = 4 THEN 'Rejected'
      WHEN vc.mapping_status = 5 THEN 'Joined'
      ELSE 'Unknown'
    END as joining_status_label,
    -- Determine vendor type
    CASE 
      WHEN vc.created_by IS NOT NULL THEN 'Broker Created Firm'
      ELSE CASE vc.category
        WHEN 1 THEN 'Selling Firm'
        WHEN 2 THEN 'Brokerage'
        WHEN 4 THEN 'Subsidiary'
        ELSE 'Unknown'
      END
    END as vendor_type,
    -- Get primary contact phone (company phone or first POC phone)
    COALESCE(vc.phone, vap.poc_phone) as primary_contact_phone,
    -- Add updated_at for incremental processing
    GREATEST(
      COALESCE(vc.updated_at, vc.created_at),
      COALESCE(vap.poc_created_at, vc.created_at)
    ) as updated_at,
    -- Add client_company_id (hardcoded as per your original query)
    12855 as client_company_id,
    -- Add archived_at (NULL for now, can be updated later)
    NULL as archived_at
  FROM {{ ref('int_vendor_companies') }} vc
  LEFT JOIN {{ ref('int_vendor_product_categories') }} vpc ON vc.id = vpc.company_id
  LEFT JOIN {{ ref('int_vendor_all_pocs') }} vap ON vc.id = vap.company_id
  LEFT JOIN {{ ref('int_vendor_tags') }} vt ON vc.id = vt.company_id
  LEFT JOIN {{ ref('int_preferred_vendor_mappings') }} pvm ON vc.id = pvm.company_id
  LEFT JOIN {{ ref('int_vendor_scores') }} vs ON vc.id = vs.company_id
  LEFT JOIN {{ ref('int_invited_by_users') }} ibu ON vc.invited_by = ibu.user_id
)

SELECT 
  client_company_id,
  id as vendor_id,
  name as vendor_name,
  email as vendor_email,
  primary_contact_phone,
  gst_no,
  city_name,
  country_name,
  address,
  DATE_PART(EPOCH_SECOND, created_at)::INTEGER as vendor_created_at,
  vendor_type,
  joining_status_label,
  category_names,
  invited_by,
  invited_by_name,
  invited_by_email,
  -- POC specific columns
  poc_id,
  poc_name,
  poc_email,
  poc_phone,
  -- Other vendor details
  tag_names,
  preferred_item_ids,
  score_value,
  vendor_code,
  DATE_PART(EPOCH_SECOND, network_joined_date)::INTEGER as network_joined_date,
  source,
  is_verified,
  misc,
  archived_at,
  updated_at,
  -- Create composite key for incremental processing
  id || '_' || COALESCE(poc_id::VARCHAR, 'no_poc') as vendor_id_poc_id
FROM final_vendor_data
{% if is_incremental() %}
WHERE updated_at >= (SELECT MAX(updated_at) FROM {{ this }}) - INTERVAL '30 minutes'
{% endif %}
ORDER BY vendor_name ASC, poc_sequence ASC
