{{ config(
    materialized='incremental',
    unique_key= ['txndetailsid'],
    on_schema_change='fail'
)}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'transactions_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

SELECT
    md5(
        random()::text || '-' || 
        COALESCE(_airbyte_data->>'txndetailsid', '') || '-' || 
        COALESCE(_airbyte_data->>'walletdetailsid', '') || '-' || 
        COALESCE((_airbyte_data->>'lastmodified')::text, '') || '-' || 
        now()::text
    ) AS id,
    'insert' AS operation,
    true AS currentflag,
    null::timestamptz AS expdate,
    _airbyte_data->>'txndetailsid' AS txndetailsid,
    _airbyte_data->>'walletdetailsid' AS walletdetailsid,
    _airbyte_data->>'clientdetails' AS clientdetails,
    ((_airbyte_data->>'createdat')::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS transaction_createdat_utc2,
    ((_airbyte_data->>'lastmodified')::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS transaction_modifiedat_utc2,
    _airbyte_data->>'txntype' AS txntype,
    _airbyte_data->>'transactionstatus' AS transactionstatus,
    _airbyte_data->>'transactionchannel' AS transactionchannel,
    _airbyte_data->>'transactiondomain' AS transactiondomain,
    _airbyte_data->>'transactionaction' AS transactionaction,
    _airbyte_data->>'interchangeaction' AS interchangeaction,
    (_airbyte_data->>'interchangeamount')::float AS interchange_amount,
    (_airbyte_data->>'servicefees_aibyte_transform')::float AS service_fees,
    (_airbyte_data->>'txnrequestedamount_aibyte_transform')::float AS amount,
    (_airbyte_data->>'walletbalancebefore_aibyte_transform')::float AS balance_before,
    (_airbyte_data->>'walletbalanceafter_aibyte_transform')::float AS balance_after,
    _airbyte_data->>'walletactualbalancebefore_aibyte_transform' AS actual_balance_before,
    _airbyte_data->>'walletactualbalanceafter_aibyte_transform' AS actual_balance_after,
    _airbyte_data->>'hasservicefees' AS hasservicefees,
    _airbyte_data->>'transactionreference' AS transactionreference,
    _airbyte_data->>'isreversedflag' AS isreversedflag,
    (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS loaddate,
    CASE
        WHEN _airbyte_data->>'txntype' NOT LIKE '%FEES' OR _airbyte_data->>'txntype' = 'TransactionTypes_CREATE-VCN_FEES' THEN false
        ELSE true
    END AS is_fees
FROM
    {{ source('axis_core', '_airbyte_raw_transactiondetails') }} src
{% if is_incremental() and table_exists %}
    WHERE src._airbyte_emitted_at > COALESCE((SELECT max(loaddate::timestamptz) FROM {{ source('dbt-dimensions', 'transactions_dimension') }}), '1900-01-01'::timestamp)
{% endif %}
