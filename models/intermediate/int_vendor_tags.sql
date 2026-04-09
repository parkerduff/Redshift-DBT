{{
  config(
    materialized='ephemeral'
  )
}}

SELECT 
  t.taggable_id as company_id,
  LISTAGG(tags.name, ', ') WITHIN GROUP (ORDER BY tags.name) as tag_names
FROM {{ ref('stg_taggings') }} t
INNER JOIN {{ ref('stg_tags') }} tags ON t.tag_id = tags.id
WHERE t.taggable_type = 'Company'
  AND t.taggable_id IN (SELECT id FROM {{ ref('int_vendor_companies') }})
  AND t.status = 1
GROUP BY t.taggable_id
