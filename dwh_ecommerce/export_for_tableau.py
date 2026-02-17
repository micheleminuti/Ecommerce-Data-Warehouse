import duckdb
import os

# --- CONFIGURAZIONE ---
db_path = 'dev.duckdb'  # Sostituisci col nome reale del file
output_dir = 'export_tableau_csv'
os.makedirs(output_dir, exist_ok=True)

# Lista completa basata sui tuoi modelli dbt
tables = [
    'main_marts.dim_customer',
    'main_marts.dim_date',
    'main_marts.dim_device',
    'main_marts.dim_location',
    'main_marts.dim_payment',
    'main_marts.dim_product',
    'main_marts.dim_promo',
    'main_marts.dim_time',
    'main_marts.fact_sales' 
]

# Connessione a DuckDB
con = duckdb.connect(db_path)

print(f"Inizio export in formato CSV...")
print(f"Cartella di destinazione: {output_dir}\n")

for table in tables:
    try:
        # Nome file (es. dim_product.csv)
        file_name = f"{table.split('.')[-1]}.csv"
        file_path = os.path.join(output_dir, file_name)
        
        print(f"Esportando {table}...", end=" ", flush=True)

        # Comando COPY per CSV
        # HEADER: include i nomi delle colonne
        # DELIMITER: usa la virgola
        # FORCE_QUOTE: mette i doppi apici a tutto per evitare errori con virgole nei testi
        con.execute(f"""
            COPY {table} TO '{file_path}' 
            (FORMAT 'CSV', HEADER TRUE, DELIMITER ',', FORCE_QUOTE *)
        """)
        
        size_mb = os.path.getsize(file_path) / (1024 * 1024)
        print(f"Fatto! ({size_mb:.2f} MB)")
        
    except Exception as e:
        print(f"Errore su {table}: {e}")

print(f"\n Tutti i file sono pronti nella cartella: {output_dir}")
con.close()