
Preparazione file con duckdb

CREATE TABLE temp_indir AS
  SELECT * FROM read_csv_auto('INDIR_ITA_20251010.csv', all_varchar=true);
  COPY temp_indir TO 'INDIR_ITA_20251010.parquet' (FORMAT PARQUET, COMPRESSION UNCOMPRESSED);
  
  DROP TABLE temp_indir;
  
  COPY 'STRAD_ITA_20251010.csv' TO 'STRAD_ITA_20251010.parquet' (FORMAT parquet, COMPRESSION uncompressed);
  
  
Prerequisiti

  uv venv
  source .venv/bin/activate
  
  uv pip install pyarrow pandas



Computa statistiche

uv run stats.py 20251010 --chunk-size 5000 --output report


Confronto fra versioni

uv run confronta_versioni.py 20250128 20251010



Altre note

DESCRIBE SELECT * FROM 'INDIR_ITA_20251010.parquet'

COPY (
-- Carica il file dei civici (Parquet) e filtra quelli con coordinate
WITH comuni_con_civici_geo AS (
SELECT 
    CODICE_COMUNE,
    CODICE_ISTAT,
    COUNT(*) AS civici_georeferenziati
FROM 'INDIR_ITA_20251010.parquet'
WHERE COORD_X_COMUNE IS NOT NULL 
  AND COORD_Y_COMUNE IS NOT NULL
  AND TRIM(COALESCE(COORD_X_COMUNE, '')) != ''
  AND TRIM(COALESCE(COORD_Y_COMUNE, '')) != ''
GROUP BY CODICE_COMUNE, CODICE_ISTAT
ORDER BY civici_georeferenziati DESC
)

-- Uniscilo al file dei comuni per ottenere i nomi
SELECT 
    c.DENOMINAZIONE_IT AS comune,
    c.SIGLAPROVINCIA AS provincia,
    c.CODISTAT,
    g.CODICE_ISTAT, g.civici_georeferenziati
FROM comuni_con_civici_geo g
JOIN read_csv_auto('comuniANPR_ISTAT.csv') c
  ON g.CODICE_ISTAT = c.CODISTAT
ORDER BY civici_georeferenziati DESC;
) TO 'report/comuni_con_civici_georeferenziati.csv'
WITH (HEADER 1, DELIMITER ',');