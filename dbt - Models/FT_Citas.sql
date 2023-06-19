{{ config(materialized='view') }}

SELECT
    DS2.id,
    DS2.idServicio AS IdServicio,
    DS2.id AS IdUsuario,
    DS2.FechaSolicitada AS FechaSolicitada,
    DS2.Estado,
    DS1.Costo
FROM {{ source('Data_Datasets', 'DATASET_1') }} AS DS1
INNER JOIN {{ source('Data_Datasets', 'DS_Usuarios') }} AS DS2
    ON DS2.idServicio = DS1.id

SELECT *
FROM @tabla t
INNER JOIN dbo.DS_Usuarios U
	ON DATEPART(year, U.FechaSolicitud) = t.anho
ORDER BY t.nombre