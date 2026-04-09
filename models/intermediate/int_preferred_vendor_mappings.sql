{{
  config(
    materialized='ephemeral'
  )
}}

SELECT 
  pvim.company_id,
  LISTAGG(pvim.item_id::VARCHAR, ', ') WITHIN GROUP (ORDER BY pvim.item_id) as preferred_item_ids
FROM {{ ref('stg_preferred_vendor_item_mappings') }} pvim
WHERE pvim.company_id IN (SELECT id FROM {{ ref('int_vendor_companies') }})
  AND pvim.status = 1
GROUP BY pvim.company_id
