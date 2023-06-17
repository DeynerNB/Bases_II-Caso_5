{{ config(materialized='view') }}

SELECT * FROM {{ ref('Pages') }}