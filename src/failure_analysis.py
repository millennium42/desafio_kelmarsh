import pandas as pd
import os

def analyze_top_failures(df=None, input_path="data/processed/kelmarsh_consolidated.parquet", output_path="data/processed/top_failures.csv"):
    """
    Recebe os dados consolidados (já limpos e padronizados), filtra os períodos 
    de indisponibilidade real e identifica as 3 principais causas (alarmes) para 
    cada turbina com base na duração total das paragens.
    Pode receber os dados diretamente em memória (df) ou ler do disco.
    """
    print("A analisar as principais causas de falha (Top 3)...")
    
    # 1. Carregamento de Dados (em memória ou via disco)
    if df is None:
        print(f"A carregar os dados de: {input_path}")
        df = pd.read_parquet(input_path)
    else:
        # Cópia para proteger os dados em memória
        df = df.copy()
        
    # 2. Filtrar apenas as categorias que representam indisponibilidade
    unavailable_categories = ['Forced outage', 'Scheduled Maintenance']
    df_unavail = df[df['IEC category'].isin(unavailable_categories)].copy()
    
    # Verificação de segurança: garantir que o data_loader fez o seu trabalho
    if 'Alarm_Message' not in df.columns:
        print("Erro: A coluna 'Alarm_Message' não foi encontrada. Por favor, corra o data_loader.py primeiro.")
        return
        
    # 3. Agrupar os dados por Turbina e pela Mensagem de Alarme (agora padronizada)
    agrupado = df_unavail.groupby(['Turbine', 'Alarm_Message'])['Duration_Hours'].sum().reset_index()
    
    # 4. Ordenar os resultados para cada turbina, da maior duração para a menor (descendente)
    agrupado = agrupado.sort_values(by=['Turbine', 'Duration_Hours'], ascending=[True, False])
    
    # 5. Extrair apenas o Top 3 de causas por turbina
    top_3_por_turbina = agrupado.groupby('Turbine').head(3).copy()
    
    # Arredondar as horas para ficar apresentável
    top_3_por_turbina['Duration_Hours'] = top_3_por_turbina['Duration_Hours'].round(2)
    
    # Guardar o resultado num CSV
    print(f"Análise concluída! A guardar as 3 principais causas em: {output_path}")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    top_3_por_turbina.to_csv(output_path, index=False)
    
    # Exibir o relatório formatado no terminal
    print("\n" + "="*50)
    print(" TOP 3 CAUSAS DE INDISPONIBILIDADE (2019 - 2021) ")
    print("="*50)
    
    for turbina in top_3_por_turbina['Turbine'].unique():
        print(f"\n[{turbina}]")
        dados_t = top_3_por_turbina[top_3_por_turbina['Turbine'] == turbina]
        for i, row in dados_t.iterrows():
            print(f"  -> {row['Alarm_Message']}: {row['Duration_Hours']} horas paradas")
            
    return top_3_por_turbina

if __name__ == "__main__":
    analyze_top_failures()