#!/usr/bin/env python3
import argparse
import pandas as pd
import os
import json
from collections import defaultdict
import pyarrow.parquet as pq
from datetime import datetime

def salva_statistiche_globali(args, stats_dict):
    """Salva le statistiche globali in un file JSON nella cartella metadata."""
    os.makedirs("metadata", exist_ok=True)
    json_path = os.path.join("metadata", f"stats_{args.data}.json")
    
    output = {
        "data_rilascio": args.data,
        "data_elaborazione": datetime.now().isoformat(),
        "chunk_size_usato": int(args.chunk_size),  # Assicura tipo nativo
        "file_input": {
            "indirizzi": f"INDIR_ITA_{args.data}.parquet",
            "strade": f"STRAD_ITA_{args.data}.parquet"
        },
        "statistiche": stats_dict
    }
    
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"💾 Statistiche globali salvate in: {json_path}")

def main():
    parser = argparse.ArgumentParser(description="Calcola statistiche dettagliate per comune (memoria efficiente + archivio).")
    parser.add_argument("data", help="Data del rilascio nel formato YYYYMMDD (es. 20251010)")
    parser.add_argument("--input-dir", default=".", help="Cartella di input (dove sono i file .parquet)")
    parser.add_argument("--output", default=None, help="Nome del file CSV di output (default: report_comuni_YYYYMMDD.csv)")
    parser.add_argument("--chunk-size", type=int, default=50_000, help="Numero di righe per blocco (default: 50000)")

    args = parser.parse_args()

    # Determina il percorso di output
    if args.output is None:
        # Default: cartella corrente + nome file standard
        output_path = f"report_comuni_{args.data}.csv"
    else:
        # Espandi il percorso e uniscilo all'input_dir se relativo
        output_path = args.output if os.path.isabs(args.output) else os.path.join(args.input_dir, args.output)

    # Verifica se output_path è una directory esistente
    if os.path.isdir(output_path):
        # Se sì, salva dentro di essa con il nome file predefinito
        output_file = f"report_comuni_{args.data}.csv"
        output_path = os.path.join(output_path, output_file)
    elif os.path.dirname(output_path) and not os.path.isdir(os.path.dirname(output_path)):
        # Crea la directory padre se non esiste
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

    strad_file = os.path.join(args.input_dir, f"STRAD_ITA_{args.data}.parquet")
    indir_file = os.path.join(args.input_dir, f"INDIR_ITA_{args.data}.parquet")

    # --- 1. Carica strade (piccolo, va in memoria) ---
    print(f"📥 Caricamento strade da {strad_file}...")
    strad_df = pd.read_parquet(strad_file)
    strade_per_comune = strad_df.groupby("CODICE_COMUNE").size()
    codice_istat_per_comune = strad_df.drop_duplicates("CODICE_COMUNE").set_index("CODICE_COMUNE")["CODICE_ISTAT"]

    # --- 2. Elabora INDIR a blocchi ---
    print(f"📥 Elaborazione civici da {indir_file} a blocchi di {args.chunk_size:,} righe...")
    table = pq.ParquetFile(indir_file)
    total_rows = table.metadata.num_rows
    print(f"📁 Totale righe civici: {total_rows:,}")

    num_civici = defaultdict(int)
    num_civici_con_coord = defaultdict(int)

    for i, batch in enumerate(table.iter_batches(batch_size=args.chunk_size)):
        chunk = batch.to_pandas()
        if i % 20 == 0:
            processed = min((i + 1) * args.chunk_size, total_rows)
            print(f"  Processati ~{processed:,} / {total_rows:,} righe...", end="\r")

        # Conta civici per comune
        counts = chunk["CODICE_COMUNE"].value_counts()
        for comune, cnt in counts.items():
            num_civici[comune] += cnt

        # Conta civici con coordinate valide
        mask = (
            chunk["COORD_X_COMUNE"].notna() &
            chunk["COORD_Y_COMUNE"].notna() &
            (chunk["COORD_X_COMUNE"] != "") &
            (chunk["COORD_Y_COMUNE"] != "")
        )
        coord_counts = chunk[mask]["CODICE_COMUNE"].value_counts()
        for comune, cnt in coord_counts.items():
            num_civici_con_coord[comune] += cnt

    print(f"\n✅ Elaborazione civici completata.")

    # --- 3. Costruisci report per comune ---
    all_comuni = set(strade_per_comune.index) | set(num_civici.keys())
    report_data = []
    for comune in sorted(all_comuni):
        report_data.append({
            "CODICE_COMUNE": comune,
            "num_strade": int(strade_per_comune.get(comune, 0)),
            "num_civici": num_civici.get(comune, 0),
            "num_civici_con_coord": num_civici_con_coord.get(comune, 0),
            "CODICE_ISTAT": str(codice_istat_per_comune.get(comune, ""))
        })

    report_df = pd.DataFrame(report_data)
    report_df.to_csv(output_path, index=False)
    print(f"✅ Report per comune salvato in: {output_path}")

    # --- 4. Calcola statistiche globali (con conversione a tipi nativi) ---
    tot_comuni = int(len(report_df))
    tot_strade = int(report_df["num_strade"].sum())
    tot_civici = int(report_df["num_civici"].sum())
    tot_civici_coord = int(report_df["num_civici_con_coord"].sum())
    comuni_con_coord = int((report_df["num_civici_con_coord"] > 0).sum())

    # Serie per calcoli statistici
    strade_series = report_df[report_df["num_strade"] > 0]["num_strade"]
    civici_series = report_df[report_df["num_civici"] > 0]["num_civici"]

    # Calcola e converti TUTTI i valori in tipi Python nativi (int/float)
    media_strade = float(strade_series.mean()) if not strade_series.empty else 0.0
    mediana_strade = float(strade_series.median()) if not strade_series.empty else 0.0
    min_strade = int(strade_series.min()) if not strade_series.empty else 0
    max_strade = int(strade_series.max()) if not strade_series.empty else 0

    media_civici = float(civici_series.mean()) if not civici_series.empty else 0.0
    mediana_civici = float(civici_series.median()) if not civici_series.empty else 0.0
    min_civici = int(civici_series.min()) if not civici_series.empty else 0
    max_civici = int(civici_series.max()) if not civici_series.empty else 0

    # Dizionario con soli tipi JSON serializzabili
    stats_dict = {
        "tot_comuni": tot_comuni,
        "tot_strade": tot_strade,
        "tot_civici": tot_civici,
        "tot_civici_con_coord": tot_civici_coord,
        "comuni_con_civici_georef": comuni_con_coord,
        "strade_per_comune": {
            "media": media_strade,
            "mediana": mediana_strade,
            "min": min_strade,
            "max": max_strade
        },
        "civici_per_comune": {
            "media": media_civici,
            "mediana": mediana_civici,
            "min": min_civici,
            "max": max_civici
        }
    }

    # Salva archivio JSON
    salva_statistiche_globali(args, stats_dict)

    # --- 5. Stampa statistiche a video ---
    print("\n" + "="*60)
    print("📊 STATISTICHE COMPLETE - ARCHIVIO NUMERI CIVICI ITALIANI")
    print("="*60)
    print(f"{'Numero di comuni:':<35} {tot_comuni:>12,}")
    print(f"{'Numero di strade totali:':<35} {tot_strade:>12,}")
    print(f"{'Numero di civici totali:':<35} {tot_civici:>12,}")
    if tot_civici > 0:
        perc_coord = 100 * tot_civici_coord / tot_civici
        print(f"{'Civici con coordinate:':<35} {tot_civici_coord:>12,} ({perc_coord:.2f}%)")
    print()
    if tot_comuni > 0:
        perc_comuni_geo = 100 * comuni_con_coord / tot_comuni
        print(f"{'Comuni con civici georeferenziati:':<35} {comuni_con_coord:>12,} ({perc_comuni_geo:.2f}%)")
    print()
    print("📈 STRADE PER COMUNE:")
    print(f"  Media:   {media_strade:>10.2f}")
    print(f"  Mediana: {mediana_strade:>10.0f}")
    print(f"  Min:     {min_strade:>10,}")
    print(f"  Max:     {max_strade:>10,}")
    print()
    print("🏘️  CIVICI PER COMUNE:")
    print(f"  Media:   {media_civici:>10.2f}")
    print(f"  Mediana: {mediana_civici:>10.0f}")
    print(f"  Min:     {min_civici:>10,}")
    print(f"  Max:     {max_civici:>10,}")
    print("="*60)

if __name__ == "__main__":
    main()