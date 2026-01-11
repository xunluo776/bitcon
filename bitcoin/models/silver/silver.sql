{{ config(
    materialized = 'incremental',
    unique_key = 'tx_hash'
) }}

select *
from {{ source('bronze', 'BRONZE_TRANSACTIONS') }}
where 1=1
{% if is_incremental() %}
  and ingestion_timestamp >
      coalesce(
        (select max(ingestion_timestamp) from {{ this }}),
        '1970-01-01'
      )
{% endif %}