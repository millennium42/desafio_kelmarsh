import os
import glob
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

def get_ml_columns(file_path):
    """Mapeia apenas as colunas de sensores críticos para modelos de Machine Learning."""
    cols = pd.read_csv(file_path, skiprows=9, nrows=0).columns.tolist()
    
    col_map = {}
    for c in cols:
        c_clean = c.replace('#', '').strip()
        
        if c_clean in ['Date and time', 'Timestamp']: 
            col_map['Timestamp'] = c
        elif c_clean == 'Power (kW)': 
            col_map['Power'] = c
        elif c_clean == 'Gear oil temperature (°C)': 
            col_map['Gear_Oil_Temp'] = c
        elif c_clean == 'Generator bearing rear temperature (°C)': 
            col_map['Gen_Bearing_Temp'] = c
        elif c_clean == 'Stator temperature 1 (°C)': 
            col_map['Stator_Temp'] = c
        elif c_clean == 'Drive train acceleration (mm/ss)': 
            col_map['Drive_Train_Acc'] = c
            
    return col_map

def load_all_sensor_data(raw_path="data/raw"):
    """Lê os dados SCADA de todas as turbinas, focando apenas nos sensores térmicos e de vibração."""
    print("A carregar dados avançados dos sensores (Temperaturas e Vibrações)...")
    
    pattern = os.path.join(raw_path, "Turbine_Data_Kelmarsh_*.csv")
    files = glob.glob(pattern)
    
    if not files:
        print("Aviso: Nenhum ficheiro de operação encontrado!")
        return None
        
    dfs = []
    for f in files:
        nome_ficheiro = os.path.basename(f)
        turbine_num = nome_ficheiro.split('_')[3] 
        turbine_id = f"T{turbine_num}"
        
        col_map = get_ml_columns(f)
        cols_to_use = list(col_map.values())
        
        # Leitura com o motor C (pois o PyArrow falha com o '#' no Timestamp deste dataset)
        df = pd.read_csv(f, skiprows=9, usecols=cols_to_use)
        
        # Renomear para padronizar
        inv_map = {v: k for k, v in col_map.items()}
        df.rename(columns=inv_map, inplace=True)
        
        # [OTIMIZAÇÃO 1 e 2]: Limpar NaNs e converter datas antes de juntar à lista
        df.dropna(subset=['Timestamp', 'Power'], inplace=True)
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
        
        # [OTIMIZAÇÃO 3]: Downcasting imediato (ficheiro a ficheiro) para impedir picos de RAM
        cols_float = df.select_dtypes(include=['float64']).columns
        df[cols_float] = df[cols_float].astype('float32')
        
        # Adicionar identificador como tipo 'category'
        df['Turbine'] = pd.Series([turbine_id] * len(df), dtype="category")
        
        dfs.append(df)
        print(f" -> Ficheiro {nome_ficheiro} carregado e pré-processado.")
        
    print("\nA concatenar todos os dados otimizados num único conjunto...")
    df_concat = pd.concat(dfs, ignore_index=True)
    
    return df_concat

def generate_predictive_dashboard(output_dir="output/plots"):
    df_sensors = load_all_sensor_data()
    
    if df_sensors is None or df_sensors.empty:
        return
        
    os.makedirs(output_dir, exist_ok=True)
    
    # Categorizar o estado operacional da turbina
    df_sensors['Estado_Operacional'] = np.where(df_sensors['Power'] > 0, 'Em Operação (Normal)', 'Parada / Falha')
    
    print("\nA gerar o Painel de Manutenção Preditiva (Dashboard)...")
    
    fig, axes = plt.subplots(2, 2, figsize=(18, 12))
    fig.suptitle('Dashboard Preditivo: Análise de Sensores Críticos e Estresse Mecânico', fontsize=18, fontweight='bold')
    
    # Gráfico 1: Estresse Térmico - Caixa de Engrenagens (Gearbox)
    sns.violinplot(data=df_sensors, x='Turbine', y='Gear_Oil_Temp', hue='Estado_Operacional', split=True, ax=axes[0, 0], palette='viridis')
    axes[0, 0].set_title('Distribuição Térmica: Óleo da Caixa de Engrenagens', fontsize=12, fontweight='bold')
    axes[0, 0].set_ylabel('Temperatura (°C)')
    axes[0, 0].grid(axis='y', linestyle='--', alpha=0.6)
    
    # Gráfico 2: Estresse Térmico - Gerador
    sns.violinplot(data=df_sensors, x='Turbine', y='Gen_Bearing_Temp', hue='Estado_Operacional', split=True, ax=axes[0, 1], palette='magma')
    axes[0, 1].set_title('Distribuição Térmica: Rolamento Traseiro do Gerador', fontsize=12, fontweight='bold')
    axes[0, 1].set_ylabel('Temperatura (°C)')
    axes[0, 1].grid(axis='y', linestyle='--', alpha=0.6)
    
    # Gráfico 3: Estresse Mecânico - Vibração do Trem de Força
    limite_vibracao = df_sensors['Drive_Train_Acc'].quantile(0.99)
    sns.boxplot(data=df_sensors, x='Turbine', y='Drive_Train_Acc', hue='Estado_Operacional', ax=axes[1, 0], palette='Set2')
    axes[1, 0].set_title('Vibração Mecânica (Trem de Força) vs Operação', fontsize=12, fontweight='bold')
    axes[1, 0].set_ylabel('Aceleração (mm/ss)')
    axes[1, 0].set_ylim(0, limite_vibracao) 
    axes[1, 0].grid(axis='y', linestyle='--', alpha=0.6)
    
    # Gráfico 4: Matriz de Correlação Global de Sensores
    df_operacao = df_sensors[df_sensors['Estado_Operacional'] == 'Em Operação (Normal)']
    df_corr = df_operacao[['Power', 'Gear_Oil_Temp', 'Gen_Bearing_Temp', 'Stator_Temp', 'Drive_Train_Acc']].corr()
    
    sns.heatmap(df_corr, annot=True, cmap='coolwarm', vmin=-1, vmax=1, ax=axes[1, 1], fmt=".2f", linewidths=0.5)
    axes[1, 1].set_title('Correlação de Sensores (Durante Operação Normal)', fontsize=12, fontweight='bold')
    
    plt.tight_layout(rect=[0, 0.03, 1, 0.96])
    
    caminho_plot = os.path.join(output_dir, 'dashboard_manutencao_preditiva.png')
    plt.savefig(caminho_plot, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"Sucesso! Painel Preditivo guardado em: {caminho_plot}")

if __name__ == "__main__":
    generate_predictive_dashboard()