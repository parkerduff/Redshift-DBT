{{
  config(
    materialized='ephemeral'
  )
}}

SELECT 
  bsm.dealing_with_company_id,
  bsm.vendor_code,
  bsm.status as mapping_status,
  bsm.created_at as network_joined_date,
  bsm.source,
  bsm.invited_by
FROM {{ ref('stg_buyer_seller_company_mappings') }} bsm
WHERE bsm.client_company_id = 12855  -- Current workspace ID
  AND bsm.status IN (1, 2, 7)                 -- Allowed statuses
  AND bsm.source IN (0, 1)
  AND bsm.invited_by IS NOT NULL  -- Filter for vendors with invited_by
