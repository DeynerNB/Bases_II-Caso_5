{{ config(materialized='view') }}

SELECT
    DS_U.id,
    DS_U.IdServicio,
    DS_U.id AS IdUsuario,
    DS_U.FechaSolicitud,
    DS_U.EstadoSolicitud,
    serv.precioCita AS Costo
FROM {{ source('Data_Datasets', 'DS_Usuarios') }} AS DS_U
INNER JOIN (
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS idServ, precioCita
    FROM {{ source('Services', 'ServiciosCOSEVI') }}
) AS serv
    ON serv.idServ = DS_U.IdServicio
