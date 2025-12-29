{{ config(
    materialized='table',
    post_hook=[
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_test
         ON public_dwh.test (etat_compteur_id, LOWER(etat_compteur))"
    ]
) }}

-- Select only current rows and remove SCD2 columns
SELECT DISTINCT ON (etat_compteur_id, LOWER(etat_compteur))
    etat_compteur_id,
    INITCAP(TRIM(etat_compteur)) AS etat_compteur
FROM {{ ref('dim_type_etat_compteur') }}
WHERE is_current = true
ORDER BY etat_compteur_id, LOWER(etat_compteur)
