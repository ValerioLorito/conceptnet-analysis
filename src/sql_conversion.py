import pandas as pd
import time

INPUT_FILE = 'conceptnet_english_cleaned.csv'

print(f"🚀 Avvio normalizzazione del dataset per MySQL...")
start_time = time.time()

# 1. LETTURA DEL DATASET PULITO
# Usiamo il normale read_csv perché il nostro file ETL precedente è separato da virgola
# Le colonne sappiamo già essere: ['relation', 'subject', 'object', 'weight']
print("📂 Lettura del file CSV in memoria (potrebbe richiedere qualche secondo per 3.5M di righe)...")
df = pd.read_csv(INPUT_FILE)

# 2. CREAZIONE DEL DIZIONARIO DEI CONCETTI (Nodi)
print("🧠 Estrazione dei concetti unici (Nodi)...")
# Uniamo la colonna dei soggetti e degli oggetti, rimuoviamo i duplicati
unique_concepts = pd.concat([df['subject'], df['object']]).unique()
# Creiamo il dizionario per la mappatura { 'nome_concetto': ID_numerico }
concept_map = {name: i+1 for i, name in enumerate(unique_concepts)}

# 3. CREAZIONE DEL DIZIONARIO DELLE RELAZIONI (Tipi di arco)
print("🔗 Estrazione dei tipi di relazione unici...")
unique_relations = df['relation'].unique()
relation_map = {name: i+1 for i, name in enumerate(unique_relations)}

# 4. ESPORTAZIONE DELLE TABELLE DI "LOOKUP"
print("💾 Salvataggio di concepts.csv e relations.csv...")
# Creiamo e salviamo i DataFrame dei dizionari
df_concepts = pd.DataFrame({'concept_id': list(concept_map.values()), 'name': list(concept_map.keys())})
df_concepts.to_csv('concepts.csv', index=False)

df_relations = pd.DataFrame({'relation_id': list(relation_map.values()), 'name': list(relation_map.keys())})
df_relations.to_csv('relations.csv', index=False)

# 5. SOSTITUZIONE DELLE STRINGHE CON GLI ID (Normalizzazione della Fact Table)
print("🔄 Sostituzione delle stringhe con le Foreign Keys nella tabella principale...")
df['subject_id'] = df['subject'].map(concept_map)
df['object_id'] = df['object'].map(concept_map)
df['relation_id'] = df['relation'].map(relation_map)

# 6. ESPORTAZIONE DELLA TABELLA DELLE ASSERZIONI (Archi)
print("💾 Salvataggio di assertions.csv (La tabella degli archi)...")
# Selezioniamo solo le colonne numeriche (Foreign Keys) e il peso
df_edges = df[['relation_id', 'subject_id', 'object_id', 'weight']]
df_edges.to_csv('assertions.csv', index=False)

end_time = time.time()
print(f"\n✅ Normalizzazione completata in {round(end_time - start_time, 2)} secondi!")
print(f"📊 Totale Concetti unici: {len(unique_concepts):,}")
print(f"📊 Totale Tipi Relazione unici: {len(unique_relations):,}")