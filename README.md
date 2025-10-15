
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