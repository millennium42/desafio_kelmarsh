import pandas as pd
import numpy as np

def calculate_availability_and_losses(df=None, input_path="data/processed/kelmarsh_consolidated.parquet", output_path="data/processed/availability_results.csv"):
    print("A processar o cálculo de disponibilidade e perdas financeiras...")
    
    if df is None:
        df = pd.read_parquet(input_path)
    else:
        df = df.copy()
    
    df['Year'] = df['Timestamp'].dt.year
    df['Month'] = df['Timestamp'].dt.month
    
    unavailable_categories = ['Forced outage', 'Scheduled Maintenance', 'Suspended']
    is_unavailable = df['IEC category'].isin(unavailable_categories)
    df_unavail = df[is_unavailable].copy()
    
    monthly_avg_power_kw = {
        1: 850, 2: 800, 3: 700, 4: 600, 5: 500, 6: 450,
        7: 400, 8: 450, 9: 550, 10: 650, 11: 750, 12: 850
    }
    
    df_unavail['Avg_Power_kW'] = df_unavail['Month'].map(monthly_avg_power_kw)
    df_unavail['Lost_Energy_kWh'] = df_unavail['Duration_Hours'] * df_unavail['Avg_Power_kW']
    
    uk_power_prices = {2019: 0.14, 2020: 0.13, 2021: 0.19}
    
    df_unavail['Price_per_kWh'] = df_unavail['Year'].map(uk_power_prices)
    df_unavail['Financial_Loss_GBP'] = df_unavail['Lost_Energy_kWh'] * df_unavail['Price_per_kWh']
    
    results = df_unavail.groupby(['Turbine', 'Year']).agg(
        Unavailable_Hours=('Duration_Hours', 'sum'),
        Lost_Energy_kWh=('Lost_Energy_kWh', 'sum'),
        Financial_Loss_GBP=('Financial_Loss_GBP', 'sum')
    ).reset_index()
    
    # OTIMIZAÇÃO: Substituição do .apply() por np.where() (Vetorização matemática rápida)
    results['Total_Year_Hours'] = np.where(results['Year'] == 2020, 8784, 8760)
    
    results['Available_Hours'] = results['Total_Year_Hours'] - results['Unavailable_Hours']
    results['Availability_Percentage'] = (results['Available_Hours'] / results['Total_Year_Hours']) * 100
    
    results = results.round({
        'Unavailable_Hours': 2, 'Available_Hours': 2, 
        'Lost_Energy_kWh': 2, 'Financial_Loss_GBP': 2, 
        'Availability_Percentage': 2
    })
    
    results.to_csv(output_path, index=False)
    
    print("\n--- Resumo dos Resultados ---")
    print(results[['Turbine', 'Year', 'Availability_Percentage', 'Financial_Loss_GBP']].head())
    return results

if __name__ == "__main__":
    calculate_availability_and_losses()