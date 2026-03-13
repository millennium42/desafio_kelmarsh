import glob
import os
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import average_precision_score, classification_report, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder


TELEMETRY_FEATURES = {
    "Timestamp": ["Date and time", "Timestamp"],
    "Power": ["Power (kW)"],
    "WindSpeed": ["Wind speed (m/s)"],
    "WindSpeedStd": ["Wind speed, Standard deviation (m/s)"],
    "AmbientTemp": ["Nacelle ambient temperature (°C)", "Ambient temperature (converter) (°C)"],
    "GearOilTemp": ["Gear oil temperature (°C)"],
    "GearOilInletTemp": ["Gear oil inlet temperature (°C)"],
    "GenBearingRearTemp": ["Generator bearing rear temperature (°C)"],
    "GenBearingFrontTemp": ["Generator bearing front temperature (°C)"],
    "StatorTemp": ["Stator temperature 1 (°C)"],
    "DriveTrainAcc": ["Drive train acceleration (mm/ss)"],
    "TowerAccX": ["Tower Acceleration X (mm/ss)"],
    "TowerAccY": ["Tower Acceleration y (mm/ss)"],
    "MetalParticleCount": ["Metal particle count"],
    "GearOilPumpPressure": ["Gear oil pump pressure (bar)"],
    "GridFrequency": ["Grid frequency (Hz)"],
    "RotorSpeed": ["Rotor speed (RPM)"],
}

STATUS_COLUMNS = {
    "Timestamp": ["Timestamp", "Date and time", "Timestamp start"],
    "IECCategory": ["IEC category"],
}

FAILURE_CATEGORIES = {"Forced outage", "Scheduled Maintenance"}


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


def load_telemetry(raw_path: str = "data/raw") -> pd.DataFrame:
    files = glob.glob(os.path.join(raw_path, "Turbine_Data_Kelmarsh_*.csv"))
    frames = []

    for file_path in files:
        turbine_num = os.path.basename(file_path).split("_")[3]
        turbine_id = f"T{turbine_num}"

        resolved = _resolve_columns(file_path, TELEMETRY_FEATURES)
        required = {"Timestamp", "Power"}
        if not required.issubset(resolved.keys()):
            continue

        df = pd.read_csv(file_path, skiprows=9, usecols=list(resolved.values()))
        inverse = {v: k for k, v in resolved.items()}
        df = df.rename(columns=inverse)
        df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
        df = df.dropna(subset=["Timestamp", "Power"]).copy()

        numeric_cols = [c for c in df.columns if c != "Timestamp"]
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
        df["Turbine"] = turbine_id

        frames.append(df)

    if not frames:
        return pd.DataFrame()

    telemetry = pd.concat(frames, ignore_index=True)
    telemetry = telemetry.sort_values(["Turbine", "Timestamp"]).reset_index(drop=True)
    return telemetry


def load_failure_events(raw_path: str = "data/raw") -> pd.DataFrame:
    files = glob.glob(os.path.join(raw_path, "Status_Kelmarsh_*.csv"))
    frames = []

    for file_path in files:
        turbine_num = os.path.basename(file_path).split("_")[2]
        turbine_id = f"T{turbine_num}"

        resolved = _resolve_columns(file_path, STATUS_COLUMNS)
        if "Timestamp" not in resolved or "IECCategory" not in resolved:
            continue

        df = pd.read_csv(file_path, skiprows=9, usecols=list(resolved.values()))
        inverse = {v: k for k, v in resolved.items()}
        df = df.rename(columns=inverse)
        df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
        df = df.dropna(subset=["Timestamp", "IECCategory"]).copy()

        df = df[df["IECCategory"].isin(FAILURE_CATEGORIES)]
        if df.empty:
            continue

        df["Turbine"] = turbine_id
        frames.append(df[["Turbine", "Timestamp", "IECCategory"]])

    if not frames:
        return pd.DataFrame(columns=["Turbine", "Timestamp", "IECCategory"])

    events = pd.concat(frames, ignore_index=True)
    return events.sort_values(["Turbine", "Timestamp"]).reset_index(drop=True)


def add_future_failure_target(
    telemetry: pd.DataFrame,
    events: pd.DataFrame,
    horizon_hours: int = 24,
) -> pd.DataFrame:
    if telemetry.empty:
        return telemetry

    telemetry = telemetry.copy()
    telemetry["target_failure_in_horizon"] = 0

    if events.empty:
        return telemetry

    horizon_ns = pd.Timedelta(hours=horizon_hours).value

    for turbine, idx in telemetry.groupby("Turbine").groups.items():
        turbine_times = telemetry.loc[idx, "Timestamp"].view("int64").to_numpy()
        event_times = events.loc[events["Turbine"] == turbine, "Timestamp"].view("int64").to_numpy()
        if len(event_times) == 0:
            continue

        next_pos = np.searchsorted(event_times, turbine_times, side="left")
        valid = next_pos < len(event_times)
        deltas = np.full(len(turbine_times), np.inf)
        deltas[valid] = event_times[next_pos[valid]] - turbine_times[valid]
        telemetry.loc[idx, "target_failure_in_horizon"] = (deltas <= horizon_ns).astype(int)

    return telemetry


def create_engineered_features(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.sort_values(["Turbine", "Timestamp"]).copy()
    sensor_cols = [
        c
        for c in [
            "Power",
            "WindSpeed",
            "GearOilTemp",
            "GenBearingRearTemp",
            "DriveTrainAcc",
            "MetalParticleCount",
            "GearOilPumpPressure",
            "RotorSpeed",
        ]
        if c in df.columns
    ]

    for col in sensor_cols:
        g = df.groupby("Turbine")[col]
        df[f"{col}_diff_1"] = g.diff()
        df[f"{col}_roll_mean_6"] = g.transform(lambda s: s.rolling(6, min_periods=1).mean())
        df[f"{col}_roll_std_6"] = g.transform(lambda s: s.rolling(6, min_periods=3).std())

    if {"GearOilTemp", "AmbientTemp"}.issubset(df.columns):
        df["GearOilTemp_delta_ambient"] = df["GearOilTemp"] - df["AmbientTemp"]

    if {"GenBearingRearTemp", "AmbientTemp"}.issubset(df.columns):
        df["GenBearingTemp_delta_ambient"] = df["GenBearingRearTemp"] - df["AmbientTemp"]

    return df


def train_failure_model(df: pd.DataFrame) -> Tuple[Pipeline, pd.DataFrame]:
    model_df = df.dropna(subset=["Timestamp", "target_failure_in_horizon"]).copy()
    model_df = model_df.sort_values("Timestamp")

    target = "target_failure_in_horizon"
    feature_cols = [c for c in model_df.columns if c not in {"Timestamp", target, "IECCategory"}]

    split_idx = int(len(model_df) * 0.8)
    train_df = model_df.iloc[:split_idx]
    test_df = model_df.iloc[split_idx:]

    X_train = train_df[feature_cols]
    y_train = train_df[target]
    X_test = test_df[feature_cols]
    y_test = test_df[target]

    categorical = [c for c in X_train.columns if X_train[c].dtype == "object"]
    numeric = [c for c in X_train.columns if c not in categorical]

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", Pipeline([("imputer", SimpleImputer(strategy="median"))]), numeric),
            (
                "cat",
                Pipeline(
                    [
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("onehot", OneHotEncoder(handle_unknown="ignore")),
                    ]
                ),
                categorical,
            ),
        ]
    )

    model = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            (
                "classifier",
                RandomForestClassifier(
                    n_estimators=200,
                    min_samples_leaf=10,
                    class_weight="balanced_subsample",
                    random_state=42,
                    n_jobs=-1,
                ),
            ),
        ]
    )

    model.fit(X_train, y_train)
    preds = model.predict(X_test)
    probs = model.predict_proba(X_test)[:, 1]

    print("\n===== Avaliação do modelo (horizonte futuro) =====")
    print(classification_report(y_test, preds, digits=3))
    if y_test.nunique() > 1:
        print(f"ROC-AUC: {roc_auc_score(y_test, probs):.4f}")
        print(f"PR-AUC:  {average_precision_score(y_test, probs):.4f}")

    return model, test_df.assign(pred_failure_prob=probs)


def run_predictive_failure_pipeline(raw_path: str = "data/raw", horizon_hours: int = 24) -> None:
    telemetry = load_telemetry(raw_path=raw_path)
    events = load_failure_events(raw_path=raw_path)

    if telemetry.empty:
        print("Nenhum dado de telemetria carregado. Verifique a pasta data/raw.")
        return

    dataset = add_future_failure_target(telemetry=telemetry, events=events, horizon_hours=horizon_hours)
    dataset = create_engineered_features(dataset)

    failure_rate = dataset["target_failure_in_horizon"].mean()
    print(f"Taxa de positivos (falha em até {horizon_hours}h): {failure_rate:.2%}")

    if dataset["target_failure_in_horizon"].nunique() < 2:
        print("Sem classes suficientes para treinar um classificador. Ajuste o horizonte ou os eventos.")
        return

    train_failure_model(dataset)


if __name__ == "__main__":
    run_predictive_failure_pipeline()
