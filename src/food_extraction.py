import csv
import time

INPUT_FILE = "conceptnet_english_cleaned.csv"
OUTPUT_FILE = "conceptnet_food_2hop.csv"

# Il nostro nodo radice
TARGET_NODE = "food"

# I "Super-Hubs" che non vogliamo espandere al livello 2 per evitare la deriva semantica
SUPER_HUBS = {'person', 'object', 'entity', 'thing', 'location', 'word', 'concept', 'idea', 'human'}

print(f"🚀 Avvio estrazione a 2-Hop per il dominio: '{TARGET_NODE}'")
start_time = time.time()

# In RAM terremo solo le parole estratte e le firme degli archi per la deduplicazione
hop1_nodes = set()
seen_edges = set()

# ==========================================
# PASSAGGIO 1: Identificazione dei vicini diretti (Hop 1)
# ==========================================
print("🔵 PASSAGGIO 1: Ricerca delle relazioni dirette...")
with open(INPUT_FILE, 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    next(reader) # Saltiamo l'intestazione
    
    for row in reader:
        # Se la riga è malformata, saltiamo
        if len(row) < 4: continue
            
        rel, subj, obj, weight = row
        
        # Se troviamo il nostro TARGET_NODE, salviamo l'ALTRO_CONCETTO
        if subj == TARGET_NODE:
            hop1_nodes.add(obj)
        elif obj == TARGET_NODE:
            hop1_nodes.add(subj)

# Pulizia: Rimuoviamo il target stesso se si è auto-referenziato, e rimuoviamo i Super-Hubs
hop1_nodes.discard(TARGET_NODE)
hop1_nodes = hop1_nodes - SUPER_HUBS

print(f"✅ Trovati {len(hop1_nodes)} concetti unici legati a '{TARGET_NODE}'.")

# ==========================================
# PASSAGGIO 2: Estrazione del Grafo (Hop 1 + Hop 2) e Deduplicazione
# ==========================================
print("🟣 PASSAGGIO 2: Estrazione di tutte le relazioni (Hop 1 + Hop 2) senza duplicati...")
saved_rows = 0

with open(INPUT_FILE, 'r', encoding='utf-8') as f_in, \
     open(OUTPUT_FILE, 'w', encoding='utf-8', newline='') as f_out:
     
    reader = csv.reader(f_in)
    writer = csv.writer(f_out)
    
    # Riscriviamo l'intestazione
    writer.writerow(['relation', 'subject', 'object', 'weight'])
    next(reader) # Saltiamo l'intestazione del file in lettura
    
    for row in reader:
        if len(row) < 4: continue
        rel, subj, obj, weight = row
        
        # Una relazione ci interessa se:
        # 1. Coinvolge direttamente 'food'
        # 2. Coinvolge uno dei concetti che abbiamo estratto al Passaggio 1
        is_hop1 = (subj == TARGET_NODE or obj == TARGET_NODE)
        is_hop2 = (subj in hop1_nodes or obj in hop1_nodes)
        
        if is_hop1 or is_hop2:
            # Creiamo una "firma" univoca della relazione per verificare i duplicati
            edge_signature = (rel, subj, obj)
            
            if edge_signature not in seen_edges:
                seen_edges.add(edge_signature)
                writer.writerow(row)
                saved_rows += 1

end_time = time.time()
print("\n🎉 Estrazione completata con successo!")
print(f"📊 Relazioni uniche salvate: {saved_rows:,}")
print(f"⏱️ Tempo impiegato: {round(end_time - start_time, 2)} secondi")