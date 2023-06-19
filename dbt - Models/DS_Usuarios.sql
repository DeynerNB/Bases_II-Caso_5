-- DATASET Usuarios
{{ config(materialized='table') }}

-- CORREGIR EL ID_SERVICIO
WITH users_raw_data AS (
    SELECT U.id,
        CONCAT(firstName, ' ', lastName) AS Nombre,
        I.identificacion AS Cedula,
        I.edad AS Edad,
        serv.ServicioID AS IdServicio,
        U.createdAt AS FechaSolicitud,
        CASE
            WHEN U.isActive = 1 THEN 'Completo'
            ELSE 'Incompleto'
        END AS EstadoSolicitud
    FROM {{ ref('Users') }} AS U
    INNER JOIN {{ ref('Identificaciones') }} AS I
        ON I.id = U.id
    CROSS JOIN  (
		SELECT ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS ServicioID
		FROM {{ source('Services', 'ServiciosCOSEVI') }} 
	) AS serv    
    WHERE U.createdAt IS NOT NULL
)

-- Materializar la tabla con las siguientes columnas
SELECT id, Nombre, Cedula, Edad, IdServicio, FechaSolicitud, EstadoSolicitud
FROM users_raw_data;