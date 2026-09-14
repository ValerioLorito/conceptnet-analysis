import csv
import gzip
import json
import tqdm
import time

INPUT_FILE = "assertions.csv"  # Il file è già stato decompresso
OUTPUT_FILE = "conceptnet_english_cleaned.csv"

def extract_weight(metadata_str):
    """Estrae il peso dal JSON testuale. Se fallisce, restituisce la baseline 1.0."""
    try:
        return float(json.loads(metadata_str).get('weight', 1.0))
    except:
        return 1.0

def clean_uri(uri):
    """Rimuove i prefissi di ConceptNet (es. /c/en/dog -> dog)"""
    parts = uri.split('/')
    return parts[3] if len(parts) >= 4 else uri

def clean_relation(uri):
    """Rimuove i prefissi dalle relazioni (es. /r/IsA -> IsA)"""
    parts = uri.split('/')
    return parts[2] if len(parts) >= 3 else uri

print("🚀 Avvio della Pipeline ETL in streaming su ConceptNet...")
start_time = time.time()

# Usiamo gzip per leggere direttamente il file senza doverlo prima estrarre su disco (risparmia spazio)
open_func = gzip.open if INPUT_FILE.endswith('.gz') else open
mode = 'rt' if INPUT_FILE.endswith('.gz') else 'r'

processed_rows = 0
saved_rows = 0

# Apriamo sia il file in lettura che quello in scrittura contemporaneamente
with open_func(INPUT_FILE, mode, encoding='utf-8') as f_in, \
     open(OUTPUT_FILE, 'w', encoding='utf-8', newline='') as f_out:
    
    reader = csv.reader(f_in, delimiter='\t')
    writer = csv.writer(f_out)
    
    # Scriviamo l'intestazione pulita nel nuovo file
    writer.writerow(['relation', 'subject', 'object', 'weight'])
    
    for row in tqdm(reader, desc="Processing rows"):
        processed_rows += 1
        
        # ConceptNet assertions hanno 5 colonne. Ignoriamo righe corrotte.
        if len(row) < 5:
            continue
            
        raw_subj = row[2]
        raw_obj = row[3]
        
        # --- 1. FILTRO LINGUISTICO (Early Rejection) ---
        # Se non sono entrambi in inglese, scartiamo immediatamente la riga.
        # Questo risparmia tantissima CPU evitando parsing inutili.
        if not (raw_subj.startswith('/c/en/') and raw_obj.startswith('/c/en/')):
            continue
            
        # --- 2. ESTRAZIONE E PULIZIA ---
        # Ci arriviamo solo se la lingua è corretta
        relation = clean_relation(row[1])
        subject = clean_uri(raw_subj)
        obj = clean_uri(raw_obj)
        weight = extract_weight(row[4])
        
        # --- 3. SCRITTURA (Load) ---
        writer.writerow([relation, subject, obj, weight])
        saved_rows += 1
        
        # Feedback visivo ogni milione di righe
        if processed_rows % 1000000 == 0:
            print(f"🔄 Lette {processed_rows:,} righe... Salvate {saved_rows:,} in inglese.")

end_time = time.time()
print("\n✅ Estrazione completata!")
print(f"📊 Righe totali lette: {processed_rows:,}")
print(f"🎯 Righe in inglese salvate: {saved_rows:,}")
print(f"⏱️ Tempo impiegato: {round(end_time - start_time, 2)} secondi")