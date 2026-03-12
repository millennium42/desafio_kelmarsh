import os
import glob
import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt

def get_wind_direction_angle(static_path):
    df_static = pd.read_csv(static_path)
    t2 = df_static[df_static['Title'] == 'Kelmarsh 2'].iloc[0]
    t3 = df_static[df_static['Title'] == 'Kelmarsh 3'].iloc[0]
    
    lat_origem, lon_origem = math.radians(t3['Latitude']), math.radians(t3['Longitude'])
    lat_destino, lon_destino = math.radians(t2['Latitude']), math.radians(t2['Longitude'])
    
    dlon = lon_destino - lon_origem
    # Calcula-se o cosseno de lat_destino apenas uma vez
    cos_lat_destino = math.cos(lat_destino)

    x = math.sin(dlon) * cos_lat_destino
    y = math.cos(lat_origem) * math.sin(lat_destino) - (math.sin(lat_origem) * cos_lat_destino * math.cos(dlon))
    
    bearing_deg = (math.degrees(math.atan2(x, y)) + 360) % 360
    return bearing_deg

def get_relevant_columns(file_path):
    """Lê o cabeçalho e mapeia as colunas, limpando caracteres especiais como '#'."""
    cols = pd.read_csv(file_path, skiprows=9, nrows=0).columns.tolist()
    
    col_map = {}
    for c in cols:
        # Remove o '#' e espaços em branco do início/fim para comparação
        c_clean = c.replace('#', '').strip()
        
        # Faz a correspondência exata no nome limpo
        if c_clean == 'Date and time' or c_clean == 'Timestamp': 
            col_map['Timestamp'] = c  # Guarda o nome ORIGINAL da coluna 'c'
        elif c_clean == 'Wind speed (m/s)': 
            col_map['Wind_Speed'] = c
        elif c_clean == 'Wind direction (°)': 
            col_map['Wind_Direction'] = c
        elif c_clean == 'Power (kW)': 
            col_map['Power'] = c
        elif c_clean == 'Pitch angle (°)': 
            col_map['Pitch'] = c
            
    return col_map

def load_optimized_turbine_data(turbine_id, raw_path="data/raw"):
    pattern = os.path.join(raw_path, f"Turbine_Data_Kelmarsh_{turbine_id}_*.csv")
    files = glob.glob(pattern)
    
    if not files:
        print(f"Aviso: Ficheiros Turbine_Data para T{turbine_id} não encontrados!")
        return None
        
    dfs = []
    for f in files:
        col_map = get_relevant_columns(f)
        cols_to_use = list(col_map.values())
        
        df = pd.read_csv(f, skiprows=9, usecols=cols_to_use)
        inv_map = {v: k for k, v in col_map.items()}
        df.rename(columns=inv_map, inplace=True)
        dfs.append(df)
        
    df_concat = pd.concat(dfs, ignore_index=True)
    df_concat['Timestamp'] = pd.to_datetime(df_concat['Timestamp'], errors='coerce')
    df_concat.dropna(subset=['Timestamp', 'Wind_Speed', 'Power'], inplace=True)
    
    return df_concat

def process_wake_effect(static_path="data/raw/Kelmarsh_WT_static.csv", output_dir="output/plots"):
    print("A calcular o alinhamento geográfico (Azimute) entre T2 e T3...")
    angle_t2_t3 = get_wind_direction_angle(static_path)
    
    print("\nA carregar os registos Turbine_Data...")
    df_t2 = load_optimized_turbine_data(2)
    df_t3 = load_optimized_turbine_data(3)
    
    if df_t2 is None or df_t3 is None:
        return
        
    df_merged = pd.merge(df_t2, df_t3, on='Timestamp', suffixes=('_T2', '_T3'))
    
    # === TESTE DE INTEGRIDADE DOS DADOS ===
    if df_merged['Power_T2'].equals(df_merged['Power_T3']):
        print("\n" + "!"*60)
        print(" ALERTA CRÍTICO: OS DADOS DA T2 E T3 SÃO EXATAMENTE IGUAIS!")
        print(" Por favor, verifique se os ficheiros Turbine_Data_Kelmarsh_3")
        print(" não são apenas cópias dos ficheiros da Turbina 2.")
        print("!"*60 + "\n")
    
    print("A aplicar filtros de Esteira (±30°), Pitch (<=5°) e Disponibilidade...")
    
    diferenca_angular = (df_merged['Wind_Direction_T2'] - angle_t2_t3 + 180) % 360 - 180
    mask_vento = np.abs(diferenca_angular) <= 30

    mask_pitch = True
    if 'Pitch_T2' in df_merged.columns and 'Pitch_T3' in df_merged.columns:
        mask_pitch = (df_merged['Pitch_T2'] <= 5) & (df_merged['Pitch_T3'] <= 5)
        
    mask_operacao = (df_merged['Power_T2'] > 0) & (df_merged['Power_T3'] > 0)
    
    # Construção dinâmica da query de filtragem
    query_str = "Power_T2 > 0 and Power_T3 > 0"
    
    # Só adiciona o filtro de Pitch se as colunas realmente existirem no dataset
    if 'Pitch_T2' in df_merged.columns and 'Pitch_T3' in df_merged.columns:
        query_str += " and Pitch_T2 <= 5 and Pitch_T3 <= 5"
        
    # Aplica a máscara do vento e a query calculada
    df_filtered = df_merged[mask_vento].query(query_str).copy()
    print("\nA gerar Curvas de Potência Binarizadas (0.5 m/s)...")
    bins = np.arange(0, 25.5, 0.5)
    
    # No momento de cortar em Bins, garanta ordered=True
    df_filtered['Wind_Bin'] = pd.cut(
        df_filtered['Wind_Speed_T2'], 
        bins=bins, 
        labels=bins[:-1] + 0.25,
        ordered=True
    )
    curve_t2 = df_filtered.groupby('Wind_Bin', observed=False)['Power_T2'].mean()
    curve_t3 = df_filtered.groupby('Wind_Bin', observed=False)['Power_T3'].mean()
    
    os.makedirs(output_dir, exist_ok=True)
    plt.figure(figsize=(12, 6))
    
    plt.plot(curve_t2.index, curve_t2.values, marker='o', color='#2980b9', label='T2 (A montante / Vento Livre)', linewidth=2)
    plt.plot(curve_t3.index, curve_t3.values, marker='s', color='#e74c3c', label='T3 (A jusante / Efeito Esteira)', linewidth=2)
    
    # Só preenche de cinza se a T2 for maior que a T3 (evita bugs visuais se os dados estiverem errados)
    plt.fill_between(curve_t2.index, curve_t3.values, curve_t2.values, where=(curve_t2.values > curve_t3.values), color='gray', alpha=0.3, label='Défice de Potência')
    
    plt.title(f'Efeito de Esteira: Curva de Potência Binarizada\n(Direção do Vento: {angle_t2_t3:.1f}° ±30°)', fontsize=14, fontweight='bold')
    plt.xlabel('Velocidade do Vento medida na T2 (m/s)', fontsize=12)
    plt.ylabel('Potência Ativa (kW)', fontsize=12)
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.7)
    
    caminho_plot = os.path.join(output_dir, 'curva_potencia_esteira.png')
    plt.savefig(caminho_plot, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Sucesso! Gráfico guardado em: {caminho_plot}")

if __name__ == "__main__":
    process_wake_effect()