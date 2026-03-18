import os
import glob
import math

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def get_wind_direction_angle(static_path: str) -> float:
    df_static = pd.read_csv(static_path)
    t2 = df_static[df_static["Title"] == "Kelmarsh 2"].iloc[0]
    t3 = df_static[df_static["Title"] == "Kelmarsh 3"].iloc[0]

    lat_origem = math.radians(t3["Latitude"])
    lon_origem = math.radians(t3["Longitude"])
    lat_destino = math.radians(t2["Latitude"])
    lon_destino = math.radians(t2["Longitude"])

    dlon = lon_destino - lon_origem
    cos_lat_destino = math.cos(lat_destino)

    x = math.sin(dlon) * cos_lat_destino
    y = math.cos(lat_origem) * math.sin(lat_destino) - (
        math.sin(lat_origem) * cos_lat_destino * math.cos(dlon)
    )

    return (math.degrees(math.atan2(x, y)) + 360) % 360


def get_relevant_columns(file_path: str) -> dict:
    cols = pd.read_csv(file_path, skiprows=9, nrows=0).columns.tolist()
    col_map = {}
    for c in cols:
        c_clean = c.replace("#", "").strip()
        if c_clean in ["Date and time", "Timestamp"]:
            col_map["Timestamp"] = c
        elif c_clean == "Wind speed (m/s)":
            col_map["Wind_Speed"] = c
        elif c_clean == "Wind direction (°)":
            col_map["Wind_Direction"] = c
        elif c_clean == "Power (kW)":
            col_map["Power"] = c
        elif c_clean == "Pitch angle (°)":
            col_map["Pitch"] = c
        elif c_clean in [
            "Nacelle ambient temperature (°C)",
            "Ambient temperature (°C)",
            "Temperature (°C)",
        ]:
            col_map["Temperature"] = c
    return col_map


def load_optimized_turbine_data(turbine_id: int, raw_path: str = "data/raw") -> pd.DataFrame | None:
    pattern = os.path.join(raw_path, f"Turbine_Data_Kelmarsh_{turbine_id}_*.csv")
    files = glob.glob(pattern)

    if not files:
        print(f"Aviso: Ficheiros Turbine_Data para T{turbine_id} não encontrados!")
        return None

    dfs = []
    for f in files:
        col_map = get_relevant_columns(f)
        cols_to_use = list(col_map.values())

        # OPT: dtype=float32 declarado na leitura — sem conversão posterior
        numeric_cols = {v: "float32" for k, v in col_map.items() if k != "Timestamp"}
        df = pd.read_csv(f, skiprows=9, usecols=cols_to_use, dtype=numeric_cols)

        inv_map = {v: k for k, v in col_map.items()}
        df.rename(columns=inv_map, inplace=True)

        df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
        dfs.append(df)

    df_concat = pd.concat(dfs, ignore_index=True)
    df_concat.dropna(subset=["Timestamp", "Wind_Speed", "Power"], inplace=True)
    return df_concat


def calculate_air_density_correction(df: pd.DataFrame) -> pd.DataFrame:
    R = 287.05
    P = 101325
    RHO_0 = 1.225

    if "Temperature" not in df.columns:
        print("Aviso: Coluna de Temperatura não encontrada. Assumindo 15°C.")
        df["Temperature"] = np.float32(15.0)

    temp_kelvin = (df["Temperature"] + 273.15).astype("float32")
    df["Air_Density"] = (P / (R * temp_kelvin)).astype("float32")
    df["Wind_Speed_Corrected"] = (
        df["Wind_Speed"] * ((df["Air_Density"] / RHO_0) ** (1 / 3))
    ).astype("float32")
    return df


def process_wake_effect(
    static_path: str = "data/raw/Kelmarsh_WT_static.csv",
    output_dir: str = "output/plots",
) -> None:
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
    mask_t2 = df_t2["Power"] > 0
    if "Pitch" in df_t2.columns:
        mask_t2 &= df_t2["Pitch"] <= 5
    df_t2 = df_t2[mask_t2]

    mask_t3 = df_t3["Power"] > 0
    if "Pitch" in df_t3.columns:
        mask_t3 &= df_t3["Pitch"] <= 5
    df_t3 = df_t3[mask_t3]

    # OPT: filtro de esteira aplicado ANTES do merge — reduz o merge em 60-80%
    print("A aplicar filtro de Esteira (±30°) antes do merge...")
    if "Wind_Direction" in df_t2.columns:
        diferenca_angular = (df_t2["Wind_Direction"] - angle_t2_t3 + 180) % 360 - 180
        df_t2 = df_t2[np.abs(diferenca_angular) <= 30].copy()
    else:
        print("Aviso: coluna Wind_Direction não encontrada em T2 — filtro de esteira ignorado.")

    print(f"T2 após filtro de esteira: {len(df_t2):,} registos. A fazer merge com T3...")
    df_merged = pd.merge(df_t2, df_t3, on="Timestamp", suffixes=("_T2", "_T3"))

    if df_merged.empty:
        print("Aviso: merge resultou em DataFrame vazio. Verifique os timestamps.")
        return

    if df_merged["Power_T2"].equals(df_merged["Power_T3"]):
        print("\n" + "!" * 60)
        print(" ALERTA CRÍTICO: OS DADOS DA T2 E T3 SÃO EXATAMENTE IGUAIS!")
        print("!" * 60 + "\n")

    print("\nA gerar Curvas de Potência Binarizadas (0.5 m/s)...")
    bins = np.arange(0, 25.5, 0.5)
    df_merged["Wind_Bin"] = pd.cut(
        df_merged["Wind_Speed_Corrected_T2"],
        bins=bins,
        labels=bins[:-1] + 0.25,
        ordered=True,
    )

    curve_t2 = df_merged.groupby("Wind_Bin", observed=True)["Power_T2"].mean()
    curve_t3 = df_merged.groupby("Wind_Bin", observed=True)["Power_T3"].mean()

    os.makedirs(output_dir, exist_ok=True)
    fig, ax = plt.subplots(figsize=(12, 6))

    ax.plot(
        curve_t2.index,
        curve_t2.values,
        marker="o",
        color="#2980b9",
        label="T2 (A montante / Vento Corrigido)",
        linewidth=2,
    )
    ax.plot(
        curve_t3.index,
        curve_t3.values,
        marker="s",
        color="#e74c3c",
        label="T3 (A jusante / Efeito Esteira)",
        linewidth=2,
    )
    ax.fill_between(
        curve_t2.index,
        curve_t3.values,
        curve_t2.values,
        where=(curve_t2.values > curve_t3.values),
        color="gray",
        alpha=0.3,
        label="Défice de Potência",
    )

    ax.set_title(
        f"Efeito de Esteira: Curva de Potência (Corrigida p/ Densidade do Ar)\n"
        f"(Direção do Vento: {angle_t2_t3:.1f}° ±30°)",
        fontsize=14,
        fontweight="bold",
    )
    ax.set_xlabel("Velocidade Normalizada do Vento na T2 (m/s)", fontsize=12)
    ax.set_ylabel("Potência Ativa (kW)", fontsize=12)
    ax.legend()
    ax.grid(True, linestyle="--", alpha=0.7)

    caminho_plot = os.path.join(output_dir, "curva_potencia_esteira.png")
    fig.savefig(caminho_plot, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"Sucesso! Gráfico guardado em: {caminho_plot}")


if __name__ == "__main__":
    process_wake_effect()
