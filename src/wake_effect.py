import os
import glob
import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt

def get_wind_direction_angle(static_path):
    """Calcula a direção geodésica (azimute) para que T2 fique a montante de T3."""
    df_static = pd.read_csv(static_path)
    t2 = df_static[df_static['Title'] == 'Kelmarsh 2'].iloc[0]
    t3 = df_static[df_static['Title'] == 'Kelmarsh 3'].iloc[0]
    
    # Converter para radianos
    lat_origem, lon_origem = math.radians(t3['Latitude']), math.radians(t3['Longitude'])
    lat_destino, lon_destino = math.radians(t2['Latitude']), math.radians(t2['Longitude'])
    
    # Cálculo do Azimute (Bearing) exato
    dlon = lon_destino - lon_origem
    x = math.sin(dlon) * math.cos(lat_destino)
    y = math.cos(lat_origem) * math.sin(lat_destino) - (math.sin(lat_origem) * math.cos(lat_destino) * math.cos(dlon))
    
    bearing_deg = (math.degrees(math.atan2(x, y)) + 360) % 360
    return bearing_deg

def get_relevant_columns(file_path):
    """Lê apenas o cabeçalho do CSV para identificar as colunas estritamente necessárias."""
    # Lê apenas a linha de cabeçalho (ignorando as 9 linhas de metadata)
    cols = pd.read_csv(file_path, skiprows=9, nrows=0).columns.tolist()
    
    col_map = {}
    for c in cols:
        if 'Date and time' in c or 'Timestamp' in c: col_map['Timestamp'] = c
        elif 'Wind speed (m/s)' in c: col_map['Wind_Speed'] = c
        elif 'Wind direction' in c and '°' in c: col_map['Wind_Direction'] = c
        elif 'Power' in c and 'kW' in c: col_map['Power'] = c
        elif 'Pitch' in c and '°' in c: col_map['Pitch'] = c
            
    return col_map

def load_optimized_turbine_data(turbine_id, raw_path="data/raw"):
    """Lê os dados SCADA de forma otimizada carregando apenas as colunas vitais."""
    pattern = os.path.join(raw_path, f"Turbine_Data_Kelmarsh_{turbine_id}_*.csv")
    files = glob.glob(pattern)
    
    if not files:
        print(f"Aviso: Ficheiros Turbine_Data para T{turbine_id} não encontrados!")
        return None
        
    dfs = []
    for f in files:
        col_map = get_relevant_columns(f)
        cols_to_use = list(col_map.values())
        
        # OTIMIZAÇÃO: usecols garante que apenas as 4 ou 5 colunas relevantes vão para a memória
        df = pd.read_csv(f, skiprows=9, usecols=cols_to_use)
        
        # Renomeia para o padrão interno baseado no mapa
        inv_map = {v: k for k, v in col_map.items()}
        df.rename(columns=inv_map, inplace=True)
        dfs.append(df)
        
    df_concat = pd.concat(dfs, ignore_index=True)
    
    # OTIMIZAÇÃO: Conversão datetime mais rápida inferindo o formato
    df_concat['Timestamp'] = pd.to_datetime(df_concat['Timestamp'], errors='coerce')
    df_concat.dropna(subset=['Timestamp', 'Wind_Speed', 'Power'], inplace=True)
    
    return df_concat

def process_wake_effect(static_path="data/raw/Kelmarsh_WT_static.csv", output_dir="output/plots"):
    """Filtra os dados geográficos e de SCADA para desenhar a curva binarizada de esteira."""
    print("A calcular o alinhamento geográfico (Azimute) entre T2 e T3...")
    angle_t2_t3 = get_wind_direction_angle(static_path)
    print(f"-> Direção exata do vento para T2 ficar a montante de T3: {angle_t2_t3:.2f}°")
    
    print("\nA carregar os registos Turbine_Data (modo otimizado de memória)...")
    df_t2 = load_optimized_turbine_data(2)
    df_t3 = load_optimized_turbine_data(3)
    
    if df_t2 is None or df_t3 is None:
        print("Erro: Os ficheiros Turbine_Data não estão na pasta data/raw/.")
        return
        
    # OTIMIZAÇÃO: inner join garante que temos exatamente os mesmos timestamps
    df_merged = pd.merge(df_t2, df_t3, on='Timestamp', suffixes=('_T2', '_T3'))
    
    print("\nA aplicar filtros: Direção (±30°), Pitch (<=5°) e Disponibilidade (Potência > 0)...")
    limite_inf = angle_t2_t3 - 30
    limite_sup = angle_t2_t3 + 30
    
    # 1. Filtro Direção do Vento medido na turbina a montante (T2)
    mask_vento = (df_merged['Wind_Direction_T2'] >= limite_inf) & (df_merged['Wind_Direction_T2'] <= limite_sup)
    
    # 2. Filtro de Pitch (se existir no dataset)
    mask_pitch = True
    if 'Pitch_T2' in df_merged.columns and 'Pitch_T3' in df_merged.columns:
        mask_pitch = (df_merged['Pitch_T2'] <= 5) & (df_merged['Pitch_T3'] <= 5)
        
    # 3. Filtro de Paragem (Apenas ambas gerando energia ativa positiva)
    mask_operacao = (df_merged['Power_T2'] > 0) & (df_merged['Power_T3'] > 0)
    
    # Aplicação vetorizada dos filtros
    df_filtered = df_merged[mask_vento & mask_pitch & mask_operacao].copy()
    print(f"-> Filtro concluído: {len(df_filtered)} amostras de 10-min em alinhamento perfeito.")
    
    # ==========================================
    # CURVA DE POTÊNCIA BINARIZADA (Método IEC)
    # ==========================================
    print("\nA gerar Curvas de Potência Binarizadas (intervalos de 0.5 m/s)...")
    
    # OTIMIZAÇÃO: Criação de bins usando o pd.cut de forma nativa e agrupamento numérico direto
    bins = np.arange(0, 25.5, 0.5)
    df_filtered['Wind_Bin'] = pd.cut(df_filtered['Wind_Speed_T2'], bins=bins, labels=bins[:-1] + 0.25)
    
    # Média agregada para a curva
    curve_t2 = df_filtered.groupby('Wind_Bin', observed=False)['Power_T2'].mean()
    curve_t3 = df_filtered.groupby('Wind_Bin', observed=False)['Power_T3'].mean()
    
    # Geração do Gráfico
    os.makedirs(output_dir, exist_ok=True)
    plt.figure(figsize=(12, 6))
    
    plt.plot(curve_t2.index, curve_t2.values, marker='o', color='#2980b9', label='T2 (A montante)', linewidth=2)
    plt.plot(curve_t3.index, curve_t3.values, marker='s', color='#e74c3c', label='T3 (A jusante / Esteira)', linewidth=2)
    plt.fill_between(curve_t2.index, curve_t3.values, curve_t2.values, color='gray', alpha=0.2, label='Défice de Potência')
    
    plt.title(f'Efeito de Esteira: Curva de Potência Binarizada\n(Direção do Vento: {angle_t2_t3:.1f}° ±30°)', fontsize=14, fontweight='bold')
    plt.xlabel('Velocidade do Vento medida no hub da T2 (m/s)', fontsize=12)
    plt.ylabel('Potência Ativa (kW)', fontsize=12)
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.7)
    
    caminho_plot = os.path.join(output_dir, 'curva_potencia_esteira.png')
    plt.savefig(caminho_plot, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Sucesso! Gráfico guardado em: {caminho_plot}")

if __name__ == "__main__":
    process_wake_effect()