import os
import glob

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


def get_ml_columns(file_path: str) -> dict:
    """Mapeia apenas as colunas de sensores críticos para modelos de Machine Learning."""
    cols = pd.read_csv(file_path, skiprows=9, nrows=0).columns.tolist()
    col_map = {}
    for c in cols:
        c_clean = c.replace("#", "").strip()
        if c_clean in ["Date and time", "Timestamp"]:
            col_map["Timestamp"] = c
        elif c_clean == "Power (kW)":
            col_map["Power"] = c
        elif c_clean == "Gear oil temperature (°C)":
            col_map["Gear_Oil_Temp"] = c
        elif c_clean == "Generator bearing rear temperature (°C)":
            col_map["Gen_Bearing_Temp"] = c
        elif c_clean == "Stator temperature 1 (°C)":
            col_map["Stator_Temp"] = c
        elif c_clean == "Drive train acceleration (mm/ss)":
            col_map["Drive_Train_Acc"] = c
    return col_map


def load_all_sensor_data(raw_path: str = "data/raw") -> pd.DataFrame | None:
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
        turbine_num = nome_ficheiro.split("_")[3]
        turbine_id = f"T{turbine_num}"

        col_map = get_ml_columns(f)
        cols_to_use = list(col_map.values())

        # OPT: dtype=float32 declarado na leitura — sem conversão posterior (pico de RAM evitado)
        numeric_dtypes = {v: "float32" for k, v in col_map.items() if k != "Timestamp"}
        df = pd.read_csv(f, skiprows=9, usecols=cols_to_use, dtype=numeric_dtypes)

        inv_map = {v: k for k, v in col_map.items()}
        df.rename(columns=inv_map, inplace=True)

        df.dropna(subset=["Timestamp", "Power"], inplace=True)
        df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")

        # OPT: 'category' direto na atribuição, sem conversão posterior
        df["Turbine"] = pd.Categorical([turbine_id] * len(df))

        dfs.append(df)
        print(f" -> Ficheiro {nome_ficheiro} carregado e pré-processado.")

    print("\nA concatenar todos os dados otimizados num único conjunto...")
    return pd.concat(dfs, ignore_index=True)


def generate_predictive_dashboard(output_dir: str = "output/plots") -> None:
    # OPT: sem .copy() — df_sensors não é modificado, apenas filtrado com máscaras
    df_sensors = load_all_sensor_data()

    if df_sensors is None or df_sensors.empty:
        return

    os.makedirs(output_dir, exist_ok=True)

    df_sensors["Estado_Operacional"] = np.where(
        df_sensors["Power"] > 0, "Em Operação (Normal)", "Parada / Falha"
    )

    print("\nA gerar o Painel de Manutenção Preditiva (Dashboard)...")

    fig, axes = plt.subplots(2, 2, figsize=(18, 12))
    fig.suptitle(
        "Dashboard Preditivo: Análise de Sensores Críticos e Estresse Mecânico",
        fontsize=18,
        fontweight="bold",
    )

    sns.violinplot(
        data=df_sensors,
        x="Turbine",
        y="Gear_Oil_Temp",
        hue="Estado_Operacional",
        split=True,
        ax=axes[0, 0],
        palette="viridis",
    )
    axes[0, 0].set_title(
        "Distribuição Térmica: Óleo da Caixa de Engrenagens", fontsize=12, fontweight="bold"
    )
    axes[0, 0].set_ylabel("Temperatura (°C)")
    axes[0, 0].grid(axis="y", linestyle="--", alpha=0.6)

    sns.violinplot(
        data=df_sensors,
        x="Turbine",
        y="Gen_Bearing_Temp",
        hue="Estado_Operacional",
        split=True,
        ax=axes[0, 1],
        palette="magma",
    )
    axes[0, 1].set_title(
        "Distribuição Térmica: Rolamento Traseiro do Gerador", fontsize=12, fontweight="bold"
    )
    axes[0, 1].set_ylabel("Temperatura (°C)")
    axes[0, 1].grid(axis="y", linestyle="--", alpha=0.6)

    limite_vibracao = df_sensors["Drive_Train_Acc"].quantile(0.99)
    sns.boxplot(
        data=df_sensors,
        x="Turbine",
        y="Drive_Train_Acc",
        hue="Estado_Operacional",
        ax=axes[1, 0],
        palette="Set2",
    )
    axes[1, 0].set_title(
        "Vibração Mecânica (Trem de Força) vs Operação", fontsize=12, fontweight="bold"
    )
    axes[1, 0].set_ylabel("Aceleração (mm/ss)")
    axes[1, 0].set_ylim(0, limite_vibracao)
    axes[1, 0].grid(axis="y", linestyle="--", alpha=0.6)

    # OPT: filtragem com máscara booleana — sem .copy() (read-only para correlação)
    df_operacao = df_sensors[df_sensors["Estado_Operacional"] == "Em Operação (Normal)"]
    sensor_cols = ["Power", "Gear_Oil_Temp", "Gen_Bearing_Temp", "Stator_Temp", "Drive_Train_Acc"]
    df_corr = df_operacao[sensor_cols].corr()

    sns.heatmap(
        df_corr,
        annot=True,
        cmap="coolwarm",
        vmin=-1,
        vmax=1,
        ax=axes[1, 1],
        fmt=".2f",
        linewidths=0.5,
    )
    axes[1, 1].set_title(
        "Correlação de Sensores (Durante Operação Normal)", fontsize=12, fontweight="bold"
    )

    plt.tight_layout(rect=[0, 0.03, 1, 0.96])

    caminho_plot = os.path.join(output_dir, "dashboard_manutencao_preditiva.png")
    fig.savefig(caminho_plot, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"Sucesso! Painel Preditivo guardado em: {caminho_plot}")


if __name__ == "__main__":
    generate_predictive_dashboard()
