{{ config(materialized = 'table') }}

select
    block_date,

    count(*)                                as tx_count,
    sum(fee_btc)                            as total_fee_btc,
    avg(fee_per_vbyte)                      as avg_fee_per_vbyte,

    sum(total_input_value_btc)              as total_input_btc,
    sum(total_output_value_btc)             as total_output_btc,

    sum(input_count)                        as total_inputs,
    sum(output_count)                       as total_outputs,

    sum(size_bytes)                         as total_size_bytes,
    sum(virtual_size_bytes)                 as total_virtual_size_bytes,

    sum(case when is_coinbase then 1 else 0 end)
                                           as coinbase_tx_count
from {{ ref('silver') }}
group by block_date
