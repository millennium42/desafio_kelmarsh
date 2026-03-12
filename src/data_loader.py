import os
import glob
import pandas as pd

def load_and_combine_data(raw_path="data/raw", processed_path="data/processed"):
    """
    Lê todos os ficheiros CSV de turbinas, combina-os, realiza a limpeza central
    (conversão de datas, cálculo de duração e padronização de alarmes) e 
    guarda o resultado otimizado em formato Parquet.
    """
    # Define o padrão de busca para encontrar os ficheiros CSV das turbinas
    pattern = os.path.join(raw_path, "Status_Kelmarsh_*.csv")
    file_list = glob.glob(pattern)
    dataframes = []
    
    print(f"Encontrados {len(file_list)} ficheiros. A iniciar a leitura...")
    
    # Inicia a iteração sobre cada ficheiro encontrado
    for file in file_list:
        filename = os.path.basename(file)
        # Extrai o número da turbina (ex: "1" de "Status_Kelmarsh_1_...")
        turbine_num = filename.split('_')[2]
        turbine_id = f"T{turbine_num}"
        
        # Lê o ficheiro CSV saltando as linhas de metadados iniciais (padrão Zenodo)
        df = pd.read_csv(file, skiprows=9)
        
        # Adiciona a coluna identificadora da turbina
        df['Turbine'] = turbine_id
        dataframes.append(df)
        
        print(f"Ficheiro {filename} carregado. Identificada: {turbine_id} ({len(df)} registos)")
        
    print("\nA combinar todos os dados num único conjunto...")
    combined_df = pd.concat(dataframes, ignore_index=True)
    
    # ==========================================
    # FASE DE LIMPEZA E PADRONIZAÇÃO CENTRALIZADA
    # ==========================================
    
    # --- 1. Padronização de Datas ---
    time_cols = ['Timestamp', 'Timestamp start', 'Date and time', 'Time', 'Date_Time']
    for col in time_cols:
        if col in combined_df.columns:
            print(f"A converter a coluna '{col}' para datetime...")
            combined_df[col] = pd.to_datetime(combined_df[col], errors='coerce')
            combined_df.rename(columns={col: 'Timestamp'}, inplace=True)
            break
            
    # --- 2. Tratamento Centralizado da Duração ---
    print("A processar e limpar as durações de falha...")
    if 'Duration' in combined_df.columns:
        # Remove registos onde a duração é '-' (apenas mensagens informativas)
        combined_df = combined_df[combined_df['Duration'] != '-'].copy()
        
        # Converte o formato 'HH:MM:SS' para horas decimais (ex: 1.5)
        combined_df['Duration_Hours'] = pd.to_timedelta(combined_df['Duration']).dt.total_seconds() / 3600.0
        
        # Remove linhas que tenham ficado com durações nulas/inválidas
        combined_df = combined_df.dropna(subset=['Duration_Hours', 'Timestamp'])

    # --- 3. Padronização da Coluna de Alarmes ---
    print("A padronizar a nomenclatura dos alarmes...")
    if 'Message' in combined_df.columns:
        combined_df.rename(columns={'Message': 'Alarm_Message'}, inplace=True)
    elif 'Status' in combined_df.columns:
        combined_df.rename(columns={'Status': 'Alarm_Message'}, inplace=True)
            
    # ==========================================
    # EXPORTAÇÃO DOS DADOS OTIMIZADOS
    # ==========================================
    
    # Garante que a pasta de destino existe
    os.makedirs(processed_path, exist_ok=True)
    
    # Alterado para .parquet para preservar a tipagem (datetime, float) e acelerar a leitura
    output_file = os.path.join(processed_path, "kelmarsh_consolidated.parquet")
    print(f"A guardar os dados processados e otimizados em: {output_file}")
    
    combined_df.to_parquet(output_file, index=False)
    
    print("Processo de carga e limpeza concluído com sucesso!")
    return combined_df

if __name__ == "__main__":
    load_and_combine_data()