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
    cos_lat_destino = math.cos(lat_destino)

    x = math.sin(dlon) * cos_lat_destino
    y = math.cos(lat_origem) * math.sin(lat_destino) - (math.sin(lat_origem) * cos_lat_destino * math.cos(dlon))
    
    bearing_deg = (math.degrees(math.atan2(x, y)) + 360) % 360
    return bearing_deg

def get_relevant_columns(file_path):
    cols = pd.read_csv(file_path, skiprows=9, nrows=0).columns.tolist()
    
    col_map = {}
    for c in cols:
        c_clean = c.replace('#', '').strip()
        
        if c_clean in ['Date and time', 'Timestamp']: 
            col_map['Timestamp'] = c
        elif c_clean == 'Wind speed (m/s)': 
            col_map['Wind_Speed'] = c
        elif c_clean == 'Wind direction (°)': 
            col_map['Wind_Direction'] = c
        elif c_clean == 'Power (kW)': 
            col_map['Power'] = c
        elif c_clean == 'Pitch angle (°)': 
            col_map['Pitch'] = c
        elif c_clean in ['Nacelle ambient temperature (°C)', 'Ambient temperature (°C)', 'Temperature (°C)']: 
            col_map['Temperature'] = c 
            
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
        
        # [CORREÇÃO]: Removido o engine='pyarrow'. 
        # O motor C padrão lidará perfeitamente com os caracteres especiais do cabeçalho.
        df = pd.read_csv(f, skiprows=9, usecols=cols_to_use)
        
        inv_map = {v: k for k, v in col_map.items()}
        df.rename(columns=inv_map, inplace=True)
        
        # [OTIMIZAÇÃO 2]: Converter as datas arquivo por arquivo antes do concat
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
        
        dfs.append(df)
        
    df_concat = pd.concat(dfs, ignore_index=True)
    df_concat.dropna(subset=['Timestamp', 'Wind_Speed', 'Power'], inplace=True)
    
    return df_concat

def calculate_air_density_correction(df):
    R = 287.05
    P = 101325
    RHO_0 = 1.225
    
    if 'Temperature' not in df.columns:
        print("Aviso: Coluna de Temperatura não encontrada. Assumindo 15°C para correção de densidade.")
        df['Temperature'] = 15.0
        
    temp_kelvin = df['Temperature'] + 273.15
    df['Air_Density'] = P / (R * temp_kelvin)
    df['Wind_Speed_Corrected'] = df['Wind_Speed'] * ((df['Air_Density'] / RHO_0) ** (1/3))
    
    return df

def process_wake_effect(static_path="data/raw/Kelmarsh_WT_static.csv", output_dir="output/plots"):
    print("A calcular o alinhamento geográfico (Azimute) entre T2 e T3...")
    angle_t2_t3 = get_wind_direction_angle(static_path)
    
    print("\nA carregar os registos Turbine_Data...")
    df_t2 = load_optimized_turbine_data(2)
    df_t3 = load_optimized_turbine_data(3)
    
    if df_t2 is None or df_t3 is None:
        return
        
    print("A aplicar correção de densidade do ar (Norma IEC)...")
    df_t2 = calculate_air_density_correction(df_t2)
    df_t3 = calculate_air_density_correction(df_t3)
        
    print("A aplicar pré-filtros de Operação e Pitch...")
    if 'Pitch' in df_t2.columns:
        df_t2 = df_t2[(df_t2['Power'] > 0) & (df_t2['Pitch'] <= 5)]
    else:
        df_t2 = df_t2[df_t2['Power'] > 0]
        
    if 'Pitch' in df_t3.columns:
        df_t3 = df_t3[(df_t3['Power'] > 0) & (df_t3['Pitch'] <= 5)]
    else:
        df_t3 = df_t3[df_t3['Power'] > 0]
        
    # [OTIMIZAÇÃO 3]: Fazer o downcasting para float32 ANTES do merge para evitar picos de RAM
    cols_float_t2 = df_t2.select_dtypes(include=['float64']).columns
    df_t2[cols_float_t2] = df_t2[cols_float_t2].astype('float32')
    
    cols_float_t3 = df_t3.select_dtypes(include=['float64']).columns
    df_t3[cols_float_t3] = df_t3[cols_float_t3].astype('float32')
        
    print("A cruzar os dados (Merge)...")
    df_merged = pd.merge(df_t2, df_t3, on='Timestamp', suffixes=('_T2', '_T3'))
    
    if df_merged['Power_T2'].equals(df_merged['Power_T3']):
        print("\n" + "!"*60)
        print(" ALERTA CRÍTICO: OS DADOS DA T2 E T3 SÃO EXATAMENTE IGUAIS!")
        print("!"*60 + "\n")
    
    print("A aplicar filtro de Esteira (±30°)...")
    diferenca_angular = (df_merged['Wind_Direction_T2'] - angle_t2_t3 + 180) % 360 - 180
    mask_vento = np.abs(diferenca_angular) <= 30
    df_filtered = df_merged[mask_vento].copy()

    print("\nA gerar Curvas de Potência Binarizadas (0.5 m/s)...")
    bins = np.arange(0, 25.5, 0.5)
    
    df_filtered['Wind_Bin'] = pd.cut(
        df_filtered['Wind_Speed_Corrected_T2'], 
        bins=bins, 
        labels=bins[:-1] + 0.25,
        ordered=True
    )
    
    curve_t2 = df_filtered.groupby('Wind_Bin', observed=False)['Power_T2'].mean()
    curve_t3 = df_filtered.groupby('Wind_Bin', observed=False)['Power_T3'].mean()
    
    os.makedirs(output_dir, exist_ok=True)
    plt.figure(figsize=(12, 6))
    
    plt.plot(curve_t2.index, curve_t2.values, marker='o', color='#2980b9', label='T2 (A montante / Vento Corrigido)', linewidth=2)
    plt.plot(curve_t3.index, curve_t3.values, marker='s', color='#e74c3c', label='T3 (A jusante / Efeito Esteira)', linewidth=2)
    
    plt.fill_between(curve_t2.index, curve_t3.values, curve_t2.values, where=(curve_t2.values > curve_t3.values), color='gray', alpha=0.3, label='Défice de Potência')
    
    plt.title(f'Efeito de Esteira: Curva de Potência (Corrigida p/ Densidade do Ar)\n(Direção do Vento: {angle_t2_t3:.1f}° ±30°)', fontsize=14, fontweight='bold')
    plt.xlabel('Velocidade Normalizada do Vento na T2 (m/s)', fontsize=12)
    plt.ylabel('Potência Ativa (kW)', fontsize=12)
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.7)
    
    caminho_plot = os.path.join(output_dir, 'curva_potencia_esteira.png')
    plt.savefig(caminho_plot, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Sucesso! Gráfico guardado em: {caminho_plot}")

if __name__ == "__main__":
    process_wake_effect()