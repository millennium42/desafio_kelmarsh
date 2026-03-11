import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

def plot_availability_and_losses(input_path="data/processed/availability_results.csv", output_dir="output/plots"):
    """
    Lê os resultados de disponibilidade e gera gráficos de pizza para a disponibilidade 
    e gráficos de barras para as perdas financeiras.
    """
    # Garante que a pasta de saída existe
    os.makedirs(output_dir, exist_ok=True)
    
    # Carrega os resultados gerados no passo anterior
    df = pd.read_csv(input_path)
    
    print("A gerar gráficos de pizza para a disponibilidade...")
    
    # Lista de anos e turbinas para iterar
    anos = df['Year'].unique()
    turbinas = df['Turbine'].unique()
    
    # 1. Gráficos de Pizza (Disponibilidade vs Indisponibilidade)
    # Vamos criar uma figura (grid) para cada ano, contendo as 6 turbinas
    for ano in anos:
        df_ano = df[df['Year'] == ano]
        
        # Cria uma janela com 2 linhas e 3 colunas para as 6 turbinas
        fig, axes = plt.subplots(2, 3, figsize=(15, 10))
        fig.suptitle(f'Disponibilidade Operacional das Turbinas - Ano {ano}', fontsize=16, fontweight='bold')
        
        # Achata a matriz de eixos para facilitar o loop
        axes = axes.flatten()
        
        for i, turbina in enumerate(turbinas):
            # Filtra os dados específicos da turbina e do ano
            dados_turbina = df_ano[df_ano['Turbine'] == turbina].iloc[0]
            
            # Valores para o gráfico de pizza
            labels = ['Disponível', 'Indisponível']
            sizes = [dados_turbina['Available_Hours'], dados_turbina['Unavailable_Hours']]
            cores = ['#2ecc71', '#e74c3c'] # Verde para disponível, Vermelho para indisponível
            
            # Plota o gráfico de pizza no subplot correspondente
            axes[i].pie(sizes, labels=labels, colors=cores, autopct='%1.2f%%', startangle=90, explode=(0, 0.1))
            axes[i].set_title(f'Turbina {turbina}')
        
        # Ajusta o layout e salva a imagem
        plt.tight_layout()
        caminho_pizza = os.path.join(output_dir, f'disponibilidade_pizza_{ano}.png')
        plt.savefig(caminho_pizza, dpi=300)
        plt.close() # Fecha a figura para liberar memória
        print(f"Gráfico de pizza do ano {ano} salvo em {caminho_pizza}")
        
    # 2. Gráfico de Barras para Perdas Financeiras Estimadas
    print("A gerar gráfico de impacto financeiro...")
    
    plt.figure(figsize=(12, 6))
    # Usamos o seaborn para criar um gráfico de barras agrupado por ano e turbina
    sns.barplot(data=df, x='Turbine', y='Financial_Loss_GBP', hue='Year', palette='viridis')
    
    plt.title('Estimativa de Perda Financeira por Indisponibilidade (Libras £)', fontsize=14, fontweight='bold')
    plt.xlabel('Turbina', fontsize=12)
    plt.ylabel('Perda Estimada (£)', fontsize=12)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.legend(title='Ano')
    
    caminho_financeiro = os.path.join(output_dir, 'perdas_financeiras_barras.png')
    plt.savefig(caminho_financeiro, dpi=300)
    plt.close()
    print(f"Gráfico financeiro salvo em {caminho_financeiro}")

if __name__ == "__main__":
    plot_availability_and_losses()