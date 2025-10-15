#!/usr/bin/env python3
import argparse
import json
import os

def load_stats(data):
    path = f"metadata/stats_{data}.json"
    if not os.path.exists(path):
        raise FileNotFoundError(f"File non trovato: {path}")
    with open(path) as f:
        return json.load(f)

def confronta(a, b):
    sa = a["statistiche"]
    sb = b["statistiche"]

    print(f"\n🔍 Confronto: {a['data_rilascio']} → {b['data_rilascio']}")
    print("="*60)

    def diff(key, label, fmt="{:,}"):
        val_a = sa[key]
        val_b = sb[key]
        delta = val_b - val_a
        sign = "+" if delta >= 0 else ""
        print(f"{label:<30} {fmt.format(val_a)} → {fmt.format(val_b)} ({sign}{fmt.format(delta)})")

    diff("tot_comuni", "Comuni totali")
    diff("tot_strade", "Strade totali")
    diff("tot_civici", "Civici totali")
    diff("tot_civici_con_coord", "Civici con coordinate")
    diff("comuni_con_civici_georef", "Comuni con georef")

    print("\n📈 Strade per comune (media):")
    print(f"  {sa['strade_per_comune']['media']:.2f} → {sb['strade_per_comune']['media']:.2f}")
    
    print("🏘️  Civici per comune (media):")
    print(f"  {sa['civici_per_comune']['media']:.2f} → {sb['civici_per_comune']['media']:.2f}")

    # Opzionale: salva confronto in CSV per analisi futura
    import csv
    out_csv = f"comparisons/diff_{b['data_rilascio']}_vs_{a['data_rilascio']}.csv"
    os.makedirs("comparisons", exist_ok=True)
    with open(out_csv, "w") as f:
        writer = csv.writer(f)
        writer.writerow(["metrica", "vecchia", "nuova", "delta"])
        for key in ["tot_comuni", "tot_strade", "tot_civici", "tot_civici_con_coord", "comuni_con_civici_georef"]:
            writer.writerow([key, sa[key], sb[key], sb[key] - sa[key]])
    print(f"\n✅ Confronto dettagliato salvato in: {out_csv}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("vecchia_data", help="Data della versione precedente (es. 20250910)")
    parser.add_argument("nuova_data", help="Data della versione nuova (es. 20251010)")
    args = parser.parse_args()

    os.makedirs("comparisons", exist_ok=True)

    vecchia = load_stats(args.vecchia_data)
    nuova = load_stats(args.nuova_data)

    confronta(vecchia, nuova)

if __name__ == "__main__":
    main()