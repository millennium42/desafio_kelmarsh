import pandas as pd
import numpy as np

def calculate_availability_and_losses(input_path="data/processed/kelmarsh_consolidated.csv", output_path="data/processed/availability_results.csv"):
    """
    Lê os dados consolidados, calcula a disponibilidade temporal anual por turbina,
    e estima a energia e o valor financeiro perdido.
    """
    print("A carregar os dados consolidados...")
    df = pd.read_csv(input_path)
    
    # 1. Tratar colunas de tempo e duração
    # O dataset de Status do Kelmarsh utiliza 'Timestamp start' e 'Duration' (HH:MM:SS)
    if 'Timestamp start' in df.columns:
        df['Timestamp start'] = pd.to_datetime(df['Timestamp start'], errors='coerce')
    
    # Remover registos onde a duração é '-' (são apenas mensagens informativas instantâneas sem tempo de paragem)
    df = df[df['Duration'] != '-'].copy()
    
    # Converter a coluna 'Duration' (ex: '01:30:00') para horas em formato decimal (ex: 1.5)
    df['Duration_Hours'] = pd.to_timedelta(df['Duration']).dt.total_seconds() / 3600.0
    
    # Remover linhas que não possuam data válida
    df = df.dropna(subset=['Timestamp start', 'Duration_Hours'])
    
    # Extrair o Ano e o Mês para podermos segmentar a estimativa de geração por época do ano
    df['Year'] = df['Timestamp start'].dt.year
    df['Month'] = df['Timestamp start'].dt.month
    
    # 2. Lógica de Indisponibilidade
    # De acordo com as normas do setor (ex: IEC 61400-26), a indisponibilidade operacional
    # ocorre em estados de falha forçada ou manutenção programada.
    unavailable_categories = ['Forced outage', 'Scheduled Maintenance', 'Suspended']
    
    # Criar um filtro (máscara) apenas para os eventos que causaram paragem da turbina
    is_unavailable = df['IEC category'].isin(unavailable_categories)
    df_unavail = df[is_unavailable].copy()
    
    # 3. Estimativa de Energia Não Produzida (kWh)
    # Criamos um perfil médio de potência gerada pela turbina (Senvion MM92 - 2050 kW).
    # No inverno do Reino Unido a geração é maior, no verão é menor. Estes são valores de referência.
    monthly_avg_power_kw = {
        1: 850, 2: 800, 3: 700, 4: 600, 5: 500, 6: 450,
        7: 400, 8: 450, 9: 550, 10: 650, 11: 750, 12: 850
    }
    
    # Atribuímos a potência média estimada consoante o mês em que a falha ocorreu
    df_unavail['Avg_Power_kW'] = df_unavail['Month'].map(monthly_avg_power_kw)
    
    # A energia perdida é o tempo parado multiplicado pela potência que estaria a ser gerada
    df_unavail['Lost_Energy_kWh'] = df_unavail['Duration_Hours'] * df_unavail['Avg_Power_kW']
    
    # 4. Cálculo do Prejuízo Financeiro
    # Tarifas médias estimadas no mercado do Reino Unido (em Libras GBP £ por kWh)
    uk_power_prices = {
        2019: 0.14,  # ~14 pence/kWh
        2020: 0.13,  # ~13 pence/kWh (ligeira queda no início da pandemia)
        2021: 0.19   # ~19 pence/kWh (início da escalada da crise energética)
    }
    
    # Mapear o preço por ano da ocorrência
    df_unavail['Price_per_kWh'] = df_unavail['Year'].map(uk_power_prices)
    
    # Calcular o valor em dinheiro perdido (em Libras)
    df_unavail['Financial_Loss_GBP'] = df_unavail['Lost_Energy_kWh'] * df_unavail['Price_per_kWh']
    
    # 5. Agrupar os resultados de falhas agregados por Turbina e Ano
    results = df_unavail.groupby(['Turbine', 'Year']).agg(
        Unavailable_Hours=('Duration_Hours', 'sum'),
        Lost_Energy_kWh=('Lost_Energy_kWh', 'sum'),
        Financial_Loss_GBP=('Financial_Loss_GBP', 'sum')
    ).reset_index()
    
    # Identificar o total de horas de cada ano (2020 foi ano bissexto: 366 dias = 8784 horas)
    results['Total_Year_Hours'] = results['Year'].apply(lambda y: 8784 if y == 2020 else 8760)
    
    # Calcular as horas efetivamente disponíveis e a Taxa de Disponibilidade (%)
    results['Available_Hours'] = results['Total_Year_Hours'] - results['Unavailable_Hours']
    results['Availability_Percentage'] = (results['Available_Hours'] / results['Total_Year_Hours']) * 100
    
    # Arredondar valores a 2 casas decimais para manter a tabela limpa
    results = results.round({
        'Unavailable_Hours': 2, 'Available_Hours': 2, 
        'Lost_Energy_kWh': 2, 'Financial_Loss_GBP': 2, 
        'Availability_Percentage': 2
    })
    
    print(f"\nDisponibilidade calculada com sucesso! A guardar resultados em: {output_path}")
    results.to_csv(output_path, index=False)
    
    # Exibir uma pré-visualização no terminal para validar
    print("\n--- Resumo dos Resultados ---")
    print(results[['Turbine', 'Year', 'Availability_Percentage', 'Financial_Loss_GBP']])
    
    return results

if __name__ == "__main__":
    calculate_availability_and_losses()