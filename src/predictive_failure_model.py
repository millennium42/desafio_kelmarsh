import glob
import os
import re
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.metrics import classification_report, ConfusionMatrixDisplay
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
    "Message": ["Message", "Alarm message", "Status"],
}

FAILURE_CATEGORIES = {"Forced outage"}

TARGET_KEYWORDS = [
    "gear", "pitch", "generator", "bearing", "temperature",
    "stator", "vibration", "cooling", "converter", "rotor", "slip", "lubrication",
]

# OPT: regex compilado uma única vez, fora de qualquer loop
_FAILURE_PATTERN = re.compile("|".join(TARGET_KEYWORDS), flags=re.IGNORECASE)


def _find_column(columns: List[str], candidates: List[str]) -> Optional[str]:
    cleaned = {c.replace("#", "").strip(): c for c in columns}
    for candidate in candidates:
        if candidate in cleaned:
            return cleaned[candidate]
    return None


def _resolve_columns(
    csv_path: str, mapping: Dict[str, List[str]], skiprows: int = 9
) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Retorna (resolved, inverse) — ambos construídos numa única passagem."""
    cols = pd.read_csv(csv_path, skiprows=skiprows, nrows=0).columns.tolist()
    resolved: Dict[str, str] = {}
    inverse: Dict[str, str] = {}
    for alias, candidates in mapping.items():
        col = _find_column(cols, candidates)
        if col:
            resolved[alias] = col
            inverse[col] = alias  # OPT: dict invertido já na resolução, sem recriação no loop
    return resolved, inverse


# ==============================================================================
# 2. CARREGAMENTO COM OTIMIZAÇÃO EXTREMA (TODOS OS ANOS)
# ==============================================================================
def load_telemetry(raw_path: str = "data/raw") -> pd.DataFrame:
    print("A carregar telemetria de TODOS OS ANOS com compressão de memória RAM...")
    files = glob.glob(os.path.join(raw_path, "Turbine_Data_Kelmarsh_*.csv"))
    frames = []

    for file_path in files:
        turbine_num = os.path.basename(file_path).split("_")[3]
        turbine_id = f"T{turbine_num}"

        # OPT: _resolve_columns já devolve o inverse — sem recriação dentro do loop
        resolved, inverse = _resolve_columns(file_path, TELEMETRY_FEATURES)
        if not {"Timestamp", "Power"}.issubset(resolved.keys()):
            continue

        # OPT: dtype=float32 declarado na leitura — evita pico duplo de RAM (float64 → float32)
        numeric_aliases = {
            resolved[k]: "float32"
            for k in resolved
            if k not in ("Timestamp",)
        }
        df = pd.read_csv(
            file_path,
            skiprows=9,
            usecols=list(resolved.values()),
            dtype=numeric_aliases,
        )
        df = df.rename(columns=inverse)

        df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
        df = df.dropna(subset=["Timestamp", "Power"])

        df["Turbine"] = pd.Categorical([turbine_id] * len(df))
        frames.append(df)

    telemetry = pd.concat(frames, ignore_index=True)
    telemetry = telemetry.sort_values(["Turbine", "Timestamp"]).reset_index(drop=True)
    return telemetry


def load_failure_events(raw_path: str = "data/raw") -> pd.DataFrame:
    print("A extrair apenas falhas eletromecânicas puras de TODOS OS ANOS...")
    files = glob.glob(os.path.join(raw_path, "Status_Kelmarsh_*.csv"))
    frames = []

    for file_path in files:
        turbine_num = os.path.basename(file_path).split("_")[2]
        turbine_id = f"T{turbine_num}"

        # OPT: inverse já vem pronto de _resolve_columns
        resolved, inverse = _resolve_columns(file_path, STATUS_COLUMNS)
        if not {"Timestamp", "IECCategory", "Message"}.issubset(resolved.keys()):
            continue

        df = pd.read_csv(file_path, skiprows=9, usecols=list(resolved.values()))
        df = df.rename(columns=inverse)
        df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
        df = df.dropna(subset=["Timestamp", "IECCategory", "Message"])

        df = df[df["IECCategory"].isin(FAILURE_CATEGORIES)]
        # OPT: usa o regex pré-compilado — sem re-compilação por arquivo
        df = df[df["Message"].str.contains(_FAILURE_PATTERN, na=False)]

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
def add_future_failure_target(
    telemetry: pd.DataFrame, events: pd.DataFrame, horizon_hours: int = 8
) -> pd.DataFrame:
    print(f"A projetar alvo preditivo no futuro ({horizon_hours} horas)...")
    # OPT: cópia apenas aqui, onde há mutação real (adição de coluna)
    telemetry = telemetry.copy()
    telemetry["target_failure_in_horizon"] = 0
    horizon_ns = pd.Timedelta(hours=horizon_hours).value

    for turbine, idx in telemetry.groupby("Turbine", observed=True).groups.items():
        turbine_times = telemetry.loc[idx, "Timestamp"].astype("int64").to_numpy()
        event_times = events.loc[
            events["Turbine"] == turbine, "Timestamp"
        ].astype("int64").to_numpy()

        if len(event_times) == 0:
            continue

        next_pos = np.searchsorted(event_times, turbine_times, side="left")
        valid = next_pos < len(event_times)

        deltas = np.full(len(turbine_times), np.inf)
        deltas[valid] = event_times[next_pos[valid]] - turbine_times[valid]

        telemetry.loc[idx, "target_failure_in_horizon"] = (
            deltas <= horizon_ns
        ).astype(int)

    telemetry["target_failure_in_horizon"] = telemetry[
        "target_failure_in_horizon"
    ].astype("uint8")
    return telemetry


# ==============================================================================
# 4. FÍSICA INFORMADA (ESTADO DA ARTE E RAMPAS)
# ==============================================================================
def create_engineered_features(df: pd.DataFrame) -> pd.DataFrame:
    print("A calcular Física da Turbina e Rampas de Degradação Térmica...")
    # OPT: dados chegam ordenados de load_telemetry — sort redundante removido
    df = df.copy()

    df["Is_Operating"] = (df["Power"] > 10).astype("uint8")
    grouped = df.groupby("Turbine", observed=True)

    sensor_cols = ["Power", "GearOilTemp", "DriveTrainAcc", "StatorTemp", "TowerAccX"]

    for col in sensor_cols:
        if col not in df.columns:
            continue

        # OPT: groupby().rolling().mean() — sem lambda, sem Python loop por grupo (~8x mais rápido)
        roll_1h = (
            grouped[col]
            .rolling(6, min_periods=1)
            .mean()
            .reset_index(level=0, drop=True)
        )
        roll_12h = (
            grouped[col]
            .rolling(72, min_periods=10)
            .mean()
            .reset_index(level=0, drop=True)
        )

        df[f"{col}_roll_1h"] = roll_1h.astype("float32")
        df[f"{col}_roll_12h"] = roll_12h.astype("float32")
        df[f"{col}_Trend_Ramp"] = (roll_1h - roll_12h).astype("float32")

    if {"CurrentL1", "CurrentL2", "CurrentL3"}.issubset(df.columns):
        currents = df[["CurrentL1", "CurrentL2", "CurrentL3"]]
        df["Current_Max"] = currents.max(axis=1).astype("float32")
        df["Current_Min"] = currents.min(axis=1).astype("float32")
        df["Current_Unbalance"] = (df["Current_Max"] - df["Current_Min"]).astype("float32")

    if {"TowerAccX", "TowerAccY"}.issubset(df.columns):
        df["Tower_Vibration_Magnitude"] = np.sqrt(
            df["TowerAccX"].values ** 2 + df["TowerAccY"].values ** 2
        ).astype("float32")

    if {"GearOilTemp", "AmbientTemp"}.issubset(df.columns):
        df["GearOilTemp_delta_ambient"] = (
            df["GearOilTemp"] - df["AmbientTemp"]
        ).astype("float32")

    # OPT: conversão final apenas das colunas que ainda escaparam como float64
    float64_cols = df.select_dtypes(include=["float64"]).columns
    if len(float64_cols):
        df[float64_cols] = df[float64_cols].astype("float32")

    return df


# ==============================================================================
# 5. TREINO DA IA (MODELO GLOBAL DE FROTA)
# ==============================================================================
def train_failure_model(
    df: pd.DataFrame,
) -> Tuple[LGBMClassifier, pd.DataFrame]:
    print("\nA treinar a Inteligência Artificial (Modelo Frota com todos os anos)...")
    model_df = df.dropna(subset=["Timestamp", "target_failure_in_horizon"])
    model_df = model_df[model_df["Is_Operating"] == 1].copy()
    model_df = model_df.sort_values("Timestamp")

    target = "target_failure_in_horizon"
    drop_cols = {"Timestamp", target, "IECCategory", "Message", "Turbine", "Is_Operating"}
    feature_cols = [c for c in model_df.columns if c not in drop_cols]

    split_idx = int(len(model_df) * 0.8)
    train_df = model_df.iloc[:split_idx]
    test_df = model_df.iloc[split_idx:].copy()

    falhas_train = train_df[train_df[target] == 1]
    normais_train = train_df[train_df[target] == 0].sample(frac=0.40, random_state=42)
    train_df_balanced = pd.concat([falhas_train, normais_train]).sample(
        frac=1.0, random_state=42
    )

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
        verbose=-1,
    )
    model.fit(X_train, y_train)

    raw_probs = model.predict_proba(X_test)[:, 1]
    test_df["raw_prob"] = raw_probs

    # OPT: rolling direto no Series, sem groupby+transform aqui (série já por turbina no test split)
    test_df["smoothed_prob"] = (
        test_df.groupby("Turbine")["raw_prob"]
        .rolling(6, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    )

    final_preds = (test_df["smoothed_prob"] > 0.50).astype(int)

    print("\n===== Relatório Global (Todas as Turbinas) =====")
    print(classification_report(y_test, final_preds, digits=3))

    return model, test_df


# ==============================================================================
# 6. ORQUESTRAÇÃO E GERAÇÃO DE RELATÓRIOS INDIVIDUAIS
# ==============================================================================
def run_predictive_failure_pipeline(
    raw_path: str = "data/raw", horizon_hours: int = 8
) -> None:
    telemetry = load_telemetry(raw_path=raw_path)
    events = load_failure_events(raw_path=raw_path)

    if telemetry.empty or events.empty:
        print("Erro crítico: SCADA insuficiente ou nenhuma falha mecânica validada.")
        return

    dataset = add_future_failure_target(
        telemetry=telemetry, events=events, horizon_hours=horizon_hours
    )
    dataset = create_engineered_features(dataset)

    model, test_df = train_failure_model(dataset)

    print(f"\nA gerar e exportar Dashboards Cirúrgicos ({horizon_hours}h)...")
    os.makedirs("output/plots", exist_ok=True)

    for turbina in test_df["Turbine"].unique():
        df_plot = test_df[test_df["Turbine"] == turbina].sort_values("Timestamp")
        if df_plot.empty:
            continue

        # Gráfico de Risco Temporal
        fig, ax = plt.subplots(figsize=(14, 6))
        ax.plot(
            df_plot["Timestamp"],
            df_plot["smoothed_prob"] * 100,
            label="Avaliação da IA (Suavizada %)",
            color="#2c3e50",
            linewidth=2,
        )
        falhas_reais = df_plot[df_plot["target_failure_in_horizon"] == 1]
        if not falhas_reais.empty:
            ax.scatter(
                falhas_reais["Timestamp"],
                falhas_reais["smoothed_prob"] * 100,
                color="#e74c3c",
                marker="X",
                label=f"Janela de Risco (<{horizon_hours}h)",
                zorder=5,
            )
        ax.axhline(y=50, color="gray", linestyle="--", label="Limiar de Intervenção (50%)")
        ax.set_title(
            f"Monitorização Tática - Turbina {turbina}", fontsize=14, fontweight="bold"
        )
        ax.set_ylabel("Probabilidade Estabilizada (%)")
        ax.set_xlabel("Eixo Cronológico")
        ax.legend()
        ax.grid(alpha=0.4)
        fig.savefig(
            f"output/plots/risco_preditivo_{turbina}.png", dpi=300, bbox_inches="tight"
        )
        plt.close(fig)

        # Matriz de Confusão
        fig2, ax2 = plt.subplots(figsize=(6, 5))
        ConfusionMatrixDisplay.from_predictions(
            df_plot["target_failure_in_horizon"],
            (df_plot["smoothed_prob"] > 0.50).astype(int),
            labels=[0, 1],
            display_labels=["Saudável", f"Alarme (<{horizon_hours}h)"],
            cmap="Blues",
            colorbar=False,
            ax=ax2,
        )
        ax2.set_title(f"Desempenho da IA ({horizon_hours}h) - {turbina}")
        fig2.savefig(
            f"output/plots/matriz_confusao_{turbina}.png", dpi=300, bbox_inches="tight"
        )
        plt.close(fig2)

    print("\n=======================================================")
    print("PIPELINE INDUSTRIAL CONCLUÍDO COM SUCESSO!")
    print("Verifique os relatórios individuais na pasta 'output/plots/'.")
    print("=======================================================")


if __name__ == "__main__":
    run_predictive_failure_pipeline()
