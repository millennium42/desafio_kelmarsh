import os
import glob

import pandas as pd


def load_and_combine_data(
    raw_path: str = "data/raw", processed_path: str = "data/processed"
) -> pd.DataFrame:
    pattern = os.path.join(raw_path, "Status_Kelmarsh_*.csv")
    file_list = glob.glob(pattern)
    dataframes = []

    print(f"Encontrados {len(file_list)} ficheiros. A iniciar a leitura...")

    time_cols = ["Timestamp", "Timestamp start", "Date and time", "Time", "Date_Time"]

    for file in file_list:
        filename = os.path.basename(file)
        turbine_num = filename.split("_")[2]
        turbine_id = f"T{turbine_num}"

        df = pd.read_csv(file, skiprows=9)

        # OPT: 'category' na criação, sem conversão posterior
        df["Turbine"] = pd.Categorical([turbine_id] * len(df))

        for col in time_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
                if col != "Timestamp":
                    df.rename(columns={col: "Timestamp"}, inplace=True)
                break

        dataframes.append(df)
        print(f"Ficheiro {filename} carregado e pré-processado.")

    print("\nA combinar todos os dados num único conjunto...")
    # OPT: sem .copy() aqui — concat cria um novo DataFrame, cópia é desnecessária
    combined_df = pd.concat(dataframes, ignore_index=True)

    print("A processar e limpar as durações de falha...")
    if "Duration" in combined_df.columns:
        combined_df = combined_df[combined_df["Duration"] != "-"]
        combined_df["Duration_Hours"] = (
            pd.to_timedelta(combined_df["Duration"]).dt.total_seconds() / 3600.0
        ).astype("float32")
        combined_df = combined_df.dropna(subset=["Duration_Hours", "Timestamp"])

    print("A padronizar a nomenclatura dos alarmes...")
    if "Message" in combined_df.columns:
        combined_df.rename(columns={"Message": "Alarm_Message"}, inplace=True)
    elif "Status" in combined_df.columns:
        combined_df.rename(columns={"Status": "Alarm_Message"}, inplace=True)

    if "Alarm_Message" in combined_df.columns:
        combined_df["Alarm_Message"] = combined_df["Alarm_Message"].astype("category")
    if "IEC category" in combined_df.columns:
        combined_df["IEC category"] = combined_df["IEC category"].astype("category")

    os.makedirs(processed_path, exist_ok=True)
    output_file = os.path.join(processed_path, "kelmarsh_consolidated.parquet")
    print(f"A guardar os dados processados e otimizados em: {output_file}")

    combined_df.to_parquet(output_file, index=False)
    print("Processo de carga e limpeza concluído com sucesso!")

    return combined_df


if __name__ == "__main__":
    load_and_combine_data()
