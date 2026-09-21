import csv
import gzip
import json
import tqdm
import time

INPUT_FILE = "data/original/conceptnet-assertions-5.7.0.csv"          # already decompressed
OUTPUT_FILE = "data/preprocessed/conceptnet_english_cleaned.csv"


def extract_weight(metadata_str):
    """Parse the ConceptNet JSON metadata; fall back to 1.0."""
    try:
        return float(json.loads(metadata_str).get("weight", 1.0))
    except Exception:
        return 1.0


def clean_relation(uri):
    """'/r/IsA' -> 'IsA'."""
    parts = uri.split("/")
    return parts[2] if len(parts) >= 3 else uri


print("🚀 Avvio della Pipeline ETL in streaming su ConceptNet...")
start_time = time.time()

open_func = gzip.open if INPUT_FILE.endswith(".gz") else open
mode = "rt" if INPUT_FILE.endswith(".gz") else "r"

processed_rows = 0
saved_rows = 0

with open_func(INPUT_FILE, mode, encoding="utf-8") as f_in, \
     open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as f_out:

    reader = csv.reader(f_in, delimiter="\t")
    writer = csv.writer(f_out)

    # subject / object are now FULL ConceptNet URIs: /c/en/{name}/{pos}
    writer.writerow(["relation", "subject", "object", "weight"])

    for row in tqdm.tqdm(reader, desc="Processing rows"):
        processed_rows += 1

        if len(row) < 5:
            continue

        raw_subj = row[2].strip()
        raw_obj = row[3].strip()

        # --- 1. LANGUAGE FILTER (early rejection) ---
        if not (raw_subj.startswith("/c/en/") and raw_obj.startswith("/c/en/")):
            continue

        # --- 2. EXTRACT AND CLEAN ---
        relation = clean_relation(row[1].strip())
        weight   = extract_weight(row[4])

        # --- 3. WRITE (keep URIs intact) ---
        writer.writerow([relation, raw_subj, raw_obj, weight])
        saved_rows += 1

        if processed_rows % 1_000_000 == 0:
            print(f"🔄 Lette {processed_rows:,} righe... "
                  f"Salvate {saved_rows:,} in inglese.")

end_time = time.time()
print("\n✅ Estrazione completata!")
print(f"📊 Righe totali lette: {processed_rows:,}")
print(f"🎯 Righe in inglese salvate: {saved_rows:,}")
print(f"⏱️ Tempo impiegato: {round(end_time - start_time, 2)} secondi")