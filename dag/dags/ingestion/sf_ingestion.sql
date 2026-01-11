use role dbt_role;
use database bitcoin;
use warehouse large_wh;


-- create stage
create stage if not exists BITCOIN.BRONZE.ingestion_stage
url = 's3://aws-public-blockchain/v1.0/btc/'
file_format=(type=parquet);

list @BITCOIN.BRONZE.ingestion_stage;


-- create a table for extracting raw data
CREATE TABLE IF NOT EXISTS BITCOIN.BRONZE.bronze_transactions (
    tx_hash string,
    tx_index_in_block int,
    block_hash string,
    block_number int,
    block_timestamp timestamp,
    block_date date,
    fee_btc float,
    total_input_value_btc float,
    total_output_value_btc float,
    input_count int,
    output_count int,
    size_bytes int,
    virtual_size_bytes int,
    fee_per_vbyte float,
    is_coinbase boolean,
    tx_version int,
    lock_time int,
    inputs variant,
    outputs variant,
    source_last_modified timestamp,
    ingestion_timestamp timestamp default current_timestamp(),
    
    CONSTRAINT pk_bronze_transactions PRIMARY KEY (tx_hash)
   );
   



copy into BITCOIN.BRONZE.bronze_transactions from (
    SELECT
        -- Transaction identity
        t.$1:hash::STRING AS tx_hash,
        t.$1:index::INT AS tx_index_in_block,
    
        -- Block context
        t.$1:block_hash::STRING AS block_hash,
        t.$1:block_number::INT AS block_number,
        t.$1:block_timestamp::TIMESTAMP AS block_timestamp,
        t.$1:date::DATE AS block_date,
    
        -- Transaction economics
        t.$1:fee::FLOAT AS fee_btc,
        t.$1:input_value::FLOAT AS total_input_value_btc,
        t.$1:output_value::FLOAT AS total_output_value_btc,
    
        -- Counts (important for analytics)
        t.$1:input_count::INT AS input_count,
        t.$1:output_count::INT AS output_count,
    
        -- Size & efficiency
        t.$1:size::INT AS size_bytes,
        t.$1:virtual_size::INT AS virtual_size_bytes,
        ROUND(
            t.$1:fee::FLOAT / NULLIF(t.$1:virtual_size::FLOAT, 0),
            12
        ) AS fee_per_vbyte,
    
        -- Transaction behavior flags
        t.$1:is_coinbase::BOOLEAN AS is_coinbase,
        t.$1:version::INT AS tx_version,
        t.$1:lock_time::INT AS lock_time,
    
        -- Semi-structured (keep for Silver flattening)
        t.$1:inputs AS inputs,
        t.$1:outputs AS outputs,
    
        -- Metadata
        t.$1:last_modified::TIMESTAMP AS source_last_modified,
        current_timestamp() as ingestion_timestamp

    FROM @BITCOIN.BRONZE.ingestion_stage/transactions AS t

) 
PATTERN = '.*/[0-9]{6,7}[.]snappy[.]parquet';

SELECT * FROM BITCOIN.BRONZE.BRONZE_TRANSACTIONS;



