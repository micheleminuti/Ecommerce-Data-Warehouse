import duckdb
import os

# --- CONFIGURAZIONE ---
db_path = 'dev.duckdb' 
output_dir = 'export_tableau_csv'
os.makedirs(output_dir, exist_ok=True)

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

con = duckdb.connect(db_path)

print(f"Inizio export ottimizzato per Tableau...")

for table in tables:
    try:
        file_name = f"{table.split('.')[-1]}.csv"
        file_path = os.path.join(output_dir, file_name)
        
        print(f"Esportando {table}...", end=" ", flush=True)

        # MODIFICA QUI: 
        # Rimosso FORCE_QUOTE *. DuckDB metterà le virgolette solo dove necessario 
        # (es. testi con virgole interne), lasciando i numeri "nudi" per Tableau.
        con.execute(f"""
            COPY {table} TO '{file_path}' 
            (FORMAT 'CSV', HEADER TRUE, DELIMITER ',')
        """)
        
        size_mb = os.path.getsize(file_path) / (1024 * 1024)
        print(f"Fatto! ({size_mb:.2f} MB)")
        
    except Exception as e:
        print(f"Errore su {table}: {e}")

con.close()