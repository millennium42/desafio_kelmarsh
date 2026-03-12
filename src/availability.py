import pandas as pd
import numpy as np

def calculate_availability_and_losses(df=None, input_path="data/processed/kelmarsh_consolidated.parquet", output_path="data/processed/availability_results.csv"):
    """
    Recebe os dados consolidados limpos, calcula a disponibilidade temporal anual 
    por turbina e estima a energia e o valor financeiro perdido.
    Pode receber os dados diretamente em memória (df) ou ler do disco.
    """
    print("A processar o cálculo de disponibilidade e perdas financeiras...")
    
    # Se a função não receber um DataFrame diretamente (quando corrida isoladamente), lê do disco
    if df is None:
        print(f"A carregar os dados de: {input_path}")
        df = pd.read_parquet(input_path)
    else:
        # Cria uma cópia para garantir que não alteramos o DataFrame original partilhado em memória
        df = df.copy()
    
    # 1. Extrair Ano e Mês diretamente do 'Timestamp' 
    # (Como usamos Parquet ou passamos em memória, já é garantido que é tipo datetime)
    df['Year'] = df['Timestamp'].dt.year
    df['Month'] = df['Timestamp'].dt.month
    
    # 2. Lógica de Indisponibilidade
    unavailable_categories = ['Forced outage', 'Scheduled Maintenance', 'Suspended']
    
    # Criar um filtro para os eventos que causaram paragem da turbina
    is_unavailable = df['IEC category'].isin(unavailable_categories)
    df_unavail = df[is_unavailable].copy()
    
    # 3. Estimativa de Energia Não Produzida (kWh)
    monthly_avg_power_kw = {
        1: 850, 2: 800, 3: 700, 4: 600, 5: 500, 6: 450,
        7: 400, 8: 450, 9: 550, 10: 650, 11: 750, 12: 850
    }
    
    # Atribui a potência e calcula a energia perdida usando a 'Duration_Hours' já tratada
    df_unavail['Avg_Power_kW'] = df_unavail['Month'].map(monthly_avg_power_kw)
    df_unavail['Lost_Energy_kWh'] = df_unavail['Duration_Hours'] * df_unavail['Avg_Power_kW']
    
    # 4. Cálculo do Prejuízo Financeiro
    uk_power_prices = {
        2019: 0.14,  
        2020: 0.13,  
        2021: 0.19   
    }
    
    df_unavail['Price_per_kWh'] = df_unavail['Year'].map(uk_power_prices)
    df_unavail['Financial_Loss_GBP'] = df_unavail['Lost_Energy_kWh'] * df_unavail['Price_per_kWh']
    
    # 5. Agrupar os resultados agregados por Turbina e Ano
    results = df_unavail.groupby(['Turbine', 'Year']).agg(
        Unavailable_Hours=('Duration_Hours', 'sum'),
        Lost_Energy_kWh=('Lost_Energy_kWh', 'sum'),
        Financial_Loss_GBP=('Financial_Loss_GBP', 'sum')
    ).reset_index()
    
    # 6. Calcular a Taxa de Disponibilidade (%)
    results['Total_Year_Hours'] = results['Year'].apply(lambda y: 8784 if y == 2020 else 8760)
    results['Available_Hours'] = results['Total_Year_Hours'] - results['Unavailable_Hours']
    results['Availability_Percentage'] = (results['Available_Hours'] / results['Total_Year_Hours']) * 100
    
    # Arredondar valores a 2 casas decimais
    results = results.round({
        'Unavailable_Hours': 2, 'Available_Hours': 2, 
        'Lost_Energy_kWh': 2, 'Financial_Loss_GBP': 2, 
        'Availability_Percentage': 2
    })
    
    print(f"Disponibilidade calculada com sucesso! A guardar resultados em: {output_path}")
    results.to_csv(output_path, index=False)
    
    # Exibir um pequeno resumo no terminal
    print("\n--- Resumo dos Resultados ---")
    print(results[['Turbine', 'Year', 'Availability_Percentage', 'Financial_Loss_GBP']].head())
    print("-----------------------------\n")
    
    return results

if __name__ == "__main__":
    calculate_availability_and_losses()