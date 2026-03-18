import os

import pandas as pd


def analyze_top_failures(
    df=None,
    input_path: str = "data/processed/kelmarsh_consolidated.parquet",
    output_path: str = "data/processed/top_failures.csv",
) -> pd.DataFrame:
    """
    Recebe os dados consolidados, filtra os períodos de indisponibilidade real
    e identifica as 3 principais causas de paragem por turbina.
    """
    print("A analisar as principais causas de falha (Top 3)...")

    if df is None:
        print(f"A carregar os dados de: {input_path}")
        df = pd.read_parquet(input_path)
    else:
        # OPT: cópia apenas quando recebe df externo
        df = df.copy()

    unavailable_categories = ["Forced outage", "Scheduled Maintenance"]
    # OPT: filtragem direta sem .copy() intermediário desnecessário
    df_unavail = df[df["IEC category"].isin(unavailable_categories)]

    if "Alarm_Message" not in df.columns:
        print(
            "Erro: A coluna 'Alarm_Message' não foi encontrada. "
            "Por favor, corra o data_loader.py primeiro."
        )
        return pd.DataFrame()

    agrupado = (
        df_unavail.groupby(["Turbine", "Alarm_Message"])["Duration_Hours"]
        .sum()
        .reset_index()
    )

    agrupado = agrupado.sort_values(
        by=["Turbine", "Duration_Hours"], ascending=[True, False]
    )

    top_3_por_turbina = agrupado.groupby("Turbine").head(3).copy()
    top_3_por_turbina["Duration_Hours"] = top_3_por_turbina["Duration_Hours"].round(2)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    top_3_por_turbina.to_csv(output_path, index=False)
    print(f"Análise concluída! A guardar as 3 principais causas em: {output_path}")

    print("\n" + "=" * 50)
    print(" TOP 3 CAUSAS DE INDISPONIBILIDADE (2019 - 2021) ")
    print("=" * 50)

    for turbina in top_3_por_turbina["Turbine"].unique():
        print(f"\n[{turbina}]")
        dados_t = top_3_por_turbina[top_3_por_turbina["Turbine"] == turbina]
        for _, row in dados_t.iterrows():
            print(f"  -> {row['Alarm_Message']}: {row['Duration_Hours']} horas paradas")

    return top_3_por_turbina


if __name__ == "__main__":
    analyze_top_failures()
