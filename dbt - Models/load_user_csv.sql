{{ config(materialized='view') }}

SELECT * FROM {{ ref('Users') }}