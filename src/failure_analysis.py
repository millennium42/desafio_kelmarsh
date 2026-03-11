import pandas as pd
import os

def analyze_top_failures(input_path="data/processed/kelmarsh_consolidated.csv", output_path="data/processed/top_failures.csv"):
    """
    Analisa os dados consolidados, filtra os períodos de indisponibilidade real
    e identifica as 3 principais causas (alarmes) para cada turbina com base
    na duração total das paragens.
    """
    print("A carregar os dados para análise de falhas...")
    df = pd.read_csv(input_path)
    
    # 1. Tratar a coluna de duração
    df = df[df['Duration'] != '-'].copy()
    df['Duration_Hours'] = pd.to_timedelta(df['Duration']).dt.total_seconds() / 3600.0
    
    # 2. Filtrar apenas as categorias que representam indisponibilidade da máquina
    # Confirmado pela nossa verificação prévia no dataset
    unavailable_categories = ['Forced outage', 'Scheduled Maintenance']
    df_unavail = df[df['IEC category'].isin(unavailable_categories)].copy()
    
    # O dataset de SCADA de Kelmarsh possui uma coluna com a descrição específica do evento.
    # Geralmente chama-se 'Message' ou 'Status'. Vamos detetar qual usar dinamicamente:
    coluna_alarme = 'Message' if 'Message' in df.columns else 'Status' if 'Status' in df.columns else None
    
    if not coluna_alarme:
        print("Erro: Não foi possível encontrar a coluna de descrição do alarme.")
        return
        
    # 3. Agrupar os dados por Turbina e pela Mensagem de Alarme
    # Somamos a duração total de todas as vezes que esse alarme ocorreu entre 2019 e 2021
    agrupado = df_unavail.groupby(['Turbine', coluna_alarme])['Duration_Hours'].sum().reset_index()
    
    # 4. Ordenar os resultados para cada turbina, da maior duração para a menor (descendente)
    agrupado = agrupado.sort_values(by=['Turbine', 'Duration_Hours'], ascending=[True, False])
    
    # 5. Extrair apenas o Top 3 de causas por turbina
    top_3_por_turbina = agrupado.groupby('Turbine').head(3)
    
    # Arredondar as horas para ficar apresentável
    top_3_por_turbina['Duration_Hours'] = top_3_por_turbina['Duration_Hours'].round(2)
    
    # Guardar o resultado num CSV
    print(f"\nAnálise concluída! A guardar as 3 principais causas em: {output_path}")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    top_3_por_turbina.to_csv(output_path, index=False)
    
    # Exibir o relatório formatado no terminal para podermos discutir a criticidade
    print("\n" + "="*50)
    print(" TOP 3 CAUSAS DE INDISPONIBILIDADE (2019 - 2021) ")
    print("="*50)
    
    for turbina in top_3_por_turbina['Turbine'].unique():
        print(f"\n[{turbina}]")
        dados_t = top_3_por_turbina[top_3_por_turbina['Turbine'] == turbina]
        for i, row in dados_t.iterrows():
            print(f"  -> {row[coluna_alarme]}: {row['Duration_Hours']} horas paradas")
            
    return top_3_por_turbina

if __name__ == "__main__":
    analyze_top_failures()