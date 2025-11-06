#!/usr/bin/env bash

set -e  # Esce in caso di errore

if [ $# -ne 1 ]; then
    echo "Uso: $0 <data>"
    echo "Esempio: $0 20251104"
    exit 1
fi

DATA="$1"

# Verifica che i file CSV esistano
if [ ! -f "INDIR_ITA_${DATA}.csv" ] || [ ! -f "STRAD_ITA_${DATA}.csv" ]; then
    echo "❌ Errore: mancano i file CSV per la data ${DATA}"
    exit 1
fi

echo "📅 Data elaborazione: ${DATA}"

# Creazione ambiente virtuale con uv (se non esiste)
if [ ! -d ".venv" ]; then
    echo "🔧 Creazione ambiente virtuale con uv..."
    uv venv
fi

# Attivazione ambiente
source .venv/bin/activate

# Installazione dipendenze
echo "📦 Installazione/aggiornamento dipendenze..."
uv pip install pyarrow pandas

# -------------------------------------------------
# 1. Conversione CSV → Parquet con DuckDB
# -------------------------------------------------
echo "🦆 Conversione CSV in Parquet con DuckDB..."

# Civici (INDIR)
duckdb <<EOF
CREATE TABLE temp_indir AS
  SELECT * FROM read_csv_auto('INDIR_ITA_${DATA}.csv', all_varchar=true);
COPY temp_indir TO 'INDIR_ITA_${DATA}.parquet' (FORMAT PARQUET, COMPRESSION UNCOMPRESSED);
DROP TABLE temp_indir;
EOF

# Strade (STRAD)
duckdb <<EOF
COPY 'STRAD_ITA_${DATA}.csv' TO 'STRAD_ITA_${DATA}.parquet' (FORMAT parquet, COMPRESSION uncompressed);
EOF

echo "✅ Parquet generati."

# -------------------------------------------------
# 2. Generazione report statistico
# -------------------------------------------------
echo "📊 Generazione report statistico..."
mkdir -p report
uv run stats.py "${DATA}" --chunk-size 5000 --output report

# -------------------------------------------------
# 3. Generazione comuni con civici georeferenziati (se comuniANPR_ISTAT.csv esiste)
# -------------------------------------------------
if [ -f "comuniANPR_ISTAT.csv" ]; then
    echo "🗺️  Generazione comuni con civici georeferenziati..."
    duckdb <<EOF
COPY (
WITH comuni_con_civici_geo AS (
    SELECT 
        CODICE_COMUNE,
        CODICE_ISTAT,
        COUNT(*) AS civici_georeferenziati
    FROM 'INDIR_ITA_${DATA}.parquet'
    WHERE COORD_X_COMUNE IS NOT NULL 
      AND COORD_Y_COMUNE IS NOT NULL
      AND TRIM(COALESCE(COORD_X_COMUNE, '')) != ''
      AND TRIM(COALESCE(COORD_Y_COMUNE, '')) != ''
    GROUP BY CODICE_COMUNE, CODICE_ISTAT
)
SELECT 
    c.DENOMINAZIONE_IT AS comune,
    c.SIGLAPROVINCIA AS provincia,
    c.CODISTAT,
    g.CODICE_ISTAT,
    g.civici_georeferenziati
FROM comuni_con_civici_geo g
JOIN read_csv_auto('comuniANPR_ISTAT.csv') c
  ON g.CODICE_ISTAT = c.CODISTAT
ORDER BY civici_georeferenziati DESC
) TO 'report/comuni_con_civici_georeferenziati_${DATA}.csv'
WITH (HEADER 1, DELIMITER ',');
EOF
    echo "✅ File comuni georeferenziati salvato."
else
    echo "⚠️  File comuniANPR_ISTAT.csv non trovato. Salto generazione comuni georeferenziati."
fi

# -------------------------------------------------
# 4. Confronto con ultima versione disponibile
# -------------------------------------------------
echo "🔍 Ricerca ultima versione precedente per il confronto..."

# Estrai tutte le date dai file report_comuni_*.csv (escludendo la data corrente)
# Usa solo i nomi di file che corrispondono al pattern
PREV_DATA=""
if compgen -G "report/report_comuni_*.csv" > /dev/null; then
    # Estrai le date, ordina in ordine cronologico (numerico), prendi l'ultima < DATA
    PREV_DATA=$(ls report/report_comuni_*.csv 2>/dev/null | \
        sed -n 's/.*report_comuni_\([0-9]\{8\}\)\.csv$/\1/p' | \
        grep -v "^${DATA}$" | \
        sort -n | \
        tail -n 1)
fi

if [ -n "$PREV_DATA" ]; then
    echo "🔄 Confronto con ultima versione disponibile: ${PREV_DATA}"
    # Verifica che lo script esista
    if [ -f "confronta_versioni.py" ]; then
        uv run confronta_versioni.py "${PREV_DATA}" "${DATA}"
        echo "✅ Confronto completato. Risultati generati."
    else
        echo "⚠️  Script confronta_versioni.py non trovato. Salto confronto."
    fi
else
    echo "ℹ️  Nessuna versione precedente trovata nella cartella 'report/'. Salto confronto."
fi

echo "✅ Processo completato per la data ${DATA}!"
echo "📁 Controlla la cartella 'report/' per tutti i risultati."