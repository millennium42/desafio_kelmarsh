import pandas as pd

def check_iec_categories():
    caminho_arquivo = "data/processed/kelmarsh_consolidated.csv"
    
    print("A carregar os dados para análise de categorias...")
    df = pd.read_csv(caminho_arquivo)
    
    # Verifica se a coluna existe para evitar erros
    if 'IEC category' in df.columns:
        # Obtém os valores únicos e remove possíveis valores nulos
        categorias = df['IEC category'].dropna().unique()
        
        print("\n--- Categorias IEC Encontradas no Dataset ---")
        for cat in categorias:
            print(f"- {cat}")
        print("---------------------------------------------\n")
    else:
        print("A coluna 'IEC category' não foi encontrada no dataset.")

if __name__ == "__main__":
    check_iec_categories()