import glob
import os
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.metrics import average_precision_score, classification_report, roc_auc_score, ConfusionMatrixDisplay
from lightgbm import LGBMClassifier 

# ==============================================================================
# 1. MAPEAMENTO DE DADOS E FILTROS INDUSTRIAIS
# ==============================================================================
TELEMETRY_FEATURES = {
    "Timestamp": ["Date and time", "Timestamp"],
    "Power": ["Power (kW)"],
    "PowerStdDev": ["Power, Standard deviation (kW)"], 
    "WindSpeed": ["Wind speed (m/s)"],
    "AmbientTemp": ["Nacelle ambient temperature (°C)", "Ambient temperature (converter) (°C)"],
    "GearOilTemp": ["Gear oil temperature (°C)"],
    "StatorTemp": ["Stator temperature 1 (°C)"], 
    "GenBearingRearTemp": ["Generator bearing rear temperature (°C)"],
    "DriveTrainAcc": ["Drive train acceleration (mm/ss)"],
    "TowerAccX": ["Tower Acceleration X (mm/ss)"], 
    "TowerAccY": ["Tower Acceleration y (mm/ss)"], 
    "CurrentL1": ["Current L1 / U (A)"], 
    "CurrentL2": ["Current L2 / V (A)"], 
    "CurrentL3": ["Current L3 / W (A)"], 
    "GenRPMStdDev": ["Generator RPM, Standard deviation (RPM)"], 
    "PitchAngleA_Std": ["Blade angle (pitch position) A, Standard deviation (°)"], 
}

STATUS_COLUMNS = {
    "Timestamp": ["Timestamp", "Date and time", "Timestamp start"],
    "IECCategory": ["IEC category"],
    "Message": ["Message", "Alarm message", "Status"]
}

# Filtro 1: Exclui manutenções agendadas, focando apenas em quebras reais
FAILURE_CATEGORIES = {"Forced outage"}

# Filtro 2 (O Segredo): A IA só tenta prever falhas mecânicas e térmicas (ignora software/rede)
TARGET_KEYWORDS = [
    'gear', 'pitch', 'generator', 'bearing', 'temperature', 
    'stator', 'vibration', 'cooling', 'converter', 'rotor', 'slip', 'lubrication'
]

def _find_column(columns: List[str], candidates: List[str]) -> Optional[str]:
    cleaned = {c.replace("#", "").strip(): c for c in columns}
    for candidate in candidates:
        if candidate in cleaned:
            return cleaned[candidate]
    return None

def _resolve_columns(csv_path: str, mapping: Dict[str, List[str]], skiprows: int = 9) -> Dict[str, str]:
    cols = pd.read_csv(csv_path, skiprows=skiprows, nrows=0).columns.tolist()
    resolved = {}
    for alias, candidates in mapping.items():
        col = _find_column(cols, candidates)
        if col:
            resolved[alias] = col
    return resolved


# ==============================================================================
# 2. CARREGAMENTO COM OTIMIZAÇÃO EXTREMA (TODOS OS ANOS)
# ==============================================================================
def load_telemetry(raw_path: str = "data/raw") -> pd.DataFrame:
    # O glob.glob com '*' garante que ELE LÊ TODOS OS ANOS presentes na pasta
    print("A carregar telemetria de TODOS OS ANOS com compressão de memória RAM...")
    files = glob.glob(os.path.join(raw_path, "Turbine_Data_Kelmarsh_*.csv"))
    frames = []

    for file_path in files:
        turbine_num = os.path.basename(file_path).split("_")[3]
        turbine_id = f"T{turbine_num}"

        resolved = _resolve_columns(file_path, TELEMETRY_FEATURES)
        if not {"Timestamp", "Power"}.issubset(resolved.keys()):
            continue

        df = pd.read_csv(file_path, skiprows=9, usecols=list(resolved.values()))
        inverse = {v: k for k, v in resolved.items()}
        df = df.rename(columns=inverse)
        
        df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
        df = df.dropna(subset=["Timestamp", "Power"]).copy()

        # COMPRESSÃO: Converte para float32 para poupar ~60% da RAM
        df["Turbine"] = pd.Series([turbine_id] * len(df), dtype="category")
        numeric_cols = [c for c in df.columns if c not in ["Timestamp", "Turbine"]]
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
        df[numeric_cols] = df[numeric_cols].astype('float32')

        frames.append(df)

    telemetry = pd.concat(frames, ignore_index=True)
    telemetry = telemetry.sort_values(["Turbine", "Timestamp"]).reset_index(drop=True)
    return telemetry

def load_failure_events(raw_path: str = "data/raw") -> pd.DataFrame:
    print("A extrair apenas falhas eletromecânicas puras de TODOS OS ANOS...")
    files = glob.glob(os.path.join(raw_path, "Status_Kelmarsh_*.csv"))
    frames = []

    pattern = '|'.join(TARGET_KEYWORDS)

    for file_path in files:
        turbine_num = os.path.basename(file_path).split("_")[2]
        turbine_id = f"T{turbine_num}"

        resolved = _resolve_columns(file_path, STATUS_COLUMNS)
        if "Timestamp" not in resolved or "IECCategory" not in resolved or "Message" not in resolved:
            continue

        df = pd.read_csv(file_path, skiprows=9, usecols=list(resolved.values()))
        inverse = {v: k for k, v in resolved.items()}
        df = df.rename(columns=inverse)
        df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
        df = df.dropna(subset=["Timestamp", "IECCategory", "Message"]).copy()

        df = df[df["IECCategory"].isin(FAILURE_CATEGORIES)]
        df = df[df['Message'].str.lower().str.contains(pattern, na=False, regex=True)]

        if df.empty:
            continue

        df["Turbine"] = turbine_id
        frames.append(df[["Turbine", "Timestamp", "IECCategory", "Message"]])

    if not frames:
        return pd.DataFrame()

    events = pd.concat(frames, ignore_index=True)
    return events.sort_values(["Turbine", "Timestamp"]).reset_index(drop=True)


# ==============================================================================
# 3. CRIAÇÃO DO ALVO (MÁQUINA DO TEMPO)
# ==============================================================================
def add_future_failure_target(telemetry: pd.DataFrame, events: pd.DataFrame, horizon_hours: int = 8) -> pd.DataFrame:
    print(f"A projetar alvo preditivo no futuro ({horizon_hours} horas)...")
    telemetry = telemetry.copy()
    telemetry["target_failure_in_horizon"] = 0 
    horizon_ns = pd.Timedelta(hours=horizon_hours).value

    for turbine, idx in telemetry.groupby("Turbine", observed=True).groups.items():
        turbine_times = telemetry.loc[idx, "Timestamp"].astype("int64").to_numpy()
        event_times = events.loc[events["Turbine"] == turbine, "Timestamp"].astype("int64").to_numpy()
        
        if len(event_times) == 0:
            continue

        next_pos = np.searchsorted(event_times, turbine_times, side="left")
        valid = next_pos < len(event_times)
        
        deltas = np.full(len(turbine_times), np.inf)
        deltas[valid] = event_times[next_pos[valid]] - turbine_times[valid]
        
        telemetry.loc[idx, "target_failure_in_horizon"] = (deltas <= horizon_ns).astype(int)

    telemetry["target_failure_in_horizon"] = telemetry["target_failure_in_horizon"].astype("uint8")
    return telemetry


# ==============================================================================
# 4. FÍSICA INFORMADA (ESTADO DA ARTE E RAMPAS)
# ==============================================================================
def create_engineered_features(df: pd.DataFrame) -> pd.DataFrame:
    print("A calcular Física da Turbina e Rampas de Degradação Térmica...")
    df = df.sort_values(["Turbine", "Timestamp"]).copy()
    
    # Filtro Operacional
    df['Is_Operating'] = (df['Power'] > 10).astype('uint8')
    grouped = df.groupby("Turbine", observed=True)

    sensor_cols = ["Power", "GearOilTemp", "DriveTrainAcc", "StatorTemp", "TowerAccX"]
    
    for col in sensor_cols:
        if col in df.columns:
            # Janelas de Degradação Rápida e Lenta
            df[f"{col}_roll_1h"] = grouped[col].transform(lambda s: s.rolling(6, min_periods=1).mean())
            df[f"{col}_roll_12h"] = grouped[col].transform(lambda s: s.rolling(72, min_periods=10).mean())
            
            # Rampa de Degradação (Tendência Matemática Explícita)
            df[f"{col}_Trend_Ramp"] = df[f"{col}_roll_1h"] - df[f"{col}_roll_12h"]

    # Assinatura Magnética
    if {"CurrentL1", "CurrentL2", "CurrentL3"}.issubset(df.columns):
        df["Current_Max"] = df[["CurrentL1", "CurrentL2", "CurrentL3"]].max(axis=1)
        df["Current_Min"] = df[["CurrentL1", "CurrentL2", "CurrentL3"]].min(axis=1)
        df["Current_Unbalance"] = df["Current_Max"] - df["Current_Min"]
        
    # Magnitude Vetorial da Torre
    if {"TowerAccX", "TowerAccY"}.issubset(df.columns):
        df["Tower_Vibration_Magnitude"] = np.sqrt(df["TowerAccX"]**2 + df["TowerAccY"]**2)

    # Estresse Térmico Relativo
    if {"GearOilTemp", "AmbientTemp"}.issubset(df.columns):
        df["GearOilTemp_delta_ambient"] = df["GearOilTemp"] - df["AmbientTemp"]

    cols_float = df.select_dtypes(include=['float64']).columns
    df[cols_float] = df[cols_float].astype('float32')

    return df


# ==============================================================================
# 5. TREINO DA IA (MODELO GLOBAL DE FROTA)
# ==============================================================================
def train_failure_model(df: pd.DataFrame) -> Tuple[LGBMClassifier, pd.DataFrame]:
    print("\nA treinar a Inteligência Artificial (Modelo Frota com todos os anos)...")
    model_df = df.dropna(subset=["Timestamp", "target_failure_in_horizon"]).copy()
    
    # Aplicação do Filtro Operacional no momento do treino
    model_df = model_df[model_df['Is_Operating'] == 1].copy()
    model_df = model_df.sort_values("Timestamp")

    target = "target_failure_in_horizon"
    feature_cols = [c for c in model_df.columns if c not in {"Timestamp", target, "IECCategory", "Message", "Turbine", "Is_Operating"}]

    split_idx = int(len(model_df) * 0.8)
    train_df = model_df.iloc[:split_idx]
    test_df = model_df.iloc[split_idx:].copy()

    # Undersampling Estratégico
    falhas_train = train_df[train_df[target] == 1]
    normais_train = train_df[train_df[target] == 0].sample(frac=0.40, random_state=42) 
    train_df_balanced = pd.concat([falhas_train, normais_train]).sample(frac=1.0, random_state=42)
    
    X_train = train_df_balanced[feature_cols]
    y_train = train_df_balanced[target]
    
    X_test = test_df[feature_cols]
    y_test = test_df[target]

    model = LGBMClassifier(
        n_estimators=400,
        learning_rate=0.03, 
        class_weight="balanced",
        random_state=42,
        n_jobs=-1,       
        verbose=-1       
    )

    model.fit(X_train, y_train)

    # Persistência de Alarme: Média móvel suaviza o risco e evita falsos alarmes curtos
    raw_probs = model.predict_proba(X_test)[:, 1]
    test_df['raw_prob'] = raw_probs
    test_df['smoothed_prob'] = test_df.groupby('Turbine')['raw_prob'].transform(lambda x: x.rolling(6, min_periods=1).mean())

    final_preds = (test_df['smoothed_prob'] > 0.50).astype(int)

    print("\n===== Relatório Global (Todas as Turbinas) =====")
    print(classification_report(y_test, final_preds, digits=3))
    
    return model, test_df


# ==============================================================================
# 6. ORQUESTRAÇÃO E GERAÇÃO DE RELATÓRIOS INDIVIDUAIS
# ==============================================================================
def run_predictive_failure_pipeline(raw_path: str = "data/raw", horizon_hours: int = 8) -> None:
    telemetry = load_telemetry(raw_path=raw_path)
    events = load_failure_events(raw_path=raw_path)

    if telemetry.empty or events.empty:
        print("Erro crítico: SCADA insuficiente ou nenhuma falha mecânica validada.")
        return

    dataset = add_future_failure_target(telemetry=telemetry, events=events, horizon_hours=horizon_hours)
    dataset = create_engineered_features(dataset)

    model, test_df = train_failure_model(dataset)
    
    print(f"\nA gerar e exportar Dashboards Cirúrgicos ({horizon_hours}h)...")
    os.makedirs("output/plots", exist_ok=True)
    
    turbinas_no_teste = test_df['Turbine'].unique()
    
    for turbina in turbinas_no_teste:
        df_plot = test_df[test_df['Turbine'] == turbina].sort_values('Timestamp')
        
        if df_plot.empty: continue
            
        # 1. Gráfico de Risco Temporal
        plt.figure(figsize=(14, 6))
        plt.plot(df_plot['Timestamp'], df_plot['smoothed_prob'] * 100, label='Avaliação da IA (Suavizada %)', color='#2c3e50', linewidth=2)
        
        falhas_reais = df_plot[df_plot['target_failure_in_horizon'] == 1]
        if not falhas_reais.empty:
            plt.scatter(falhas_reais['Timestamp'], falhas_reais['smoothed_prob'] * 100, color='#e74c3c', marker='X', label=f'Janela de Risco (<{horizon_hours}h)', zorder=5)
        
        plt.axhline(y=50, color='gray', linestyle='--', label='Limiar de Intervenção (50%)')
        
        plt.title(f'Monitorização Tática - Turbina {turbina}', fontsize=14, fontweight='bold')
        plt.ylabel('Probabilidade Estabilizada (%)')
        plt.xlabel('Eixo Cronológico')
        plt.legend()
        plt.grid(alpha=0.4)
        
        plt.savefig(f'output/plots/risco_preditivo_{turbina}.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        # 2. Matriz de Confusão Isolada (FORÇADA para gerar o 2x2 sempre, corrigindo a T2)
        plt.figure(figsize=(6, 5))
        ConfusionMatrixDisplay.from_predictions(
            df_plot['target_failure_in_horizon'], 
            (df_plot['smoothed_prob'] > 0.50).astype(int), 
            labels=[0, 1], # <--- ESTE PARÂMETRO GARANTE QUE A MATRIZ É GERADA MESMO SEM FALHAS
            display_labels=['Saudável', f'Alarme (<{horizon_hours}h)'],
            cmap='Blues', colorbar=False
        )
        plt.title(f'Desempenho da IA ({horizon_hours}h) - {turbina}')
        plt.savefig(f'output/plots/matriz_confusao_{turbina}.png', dpi=300, bbox_inches='tight')
        plt.close()

    print("\n=======================================================")
    print("PIPELINE INDUSTRIAL CONCLUÍDO COM SUCESSO!")
    print("Verifique os relatórios individuais na pasta 'output/plots/'.")
    print("=======================================================")

if __name__ == "__main__":
    run_predictive_failure_pipeline()