import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

def plot_availability_and_losses(df_results=None, input_path="data/processed/availability_results.csv", output_dir="output/plots"):
    """
    Gera gráficos de pizza para a disponibilidade e gráficos de barras para as 
    perdas financeiras.
    Pode receber os dados dos resultados diretamente em memória (df_results) ou ler do disco.
    """
    # Garante que a pasta de saída existe
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. Carregamento de Dados (em memória ou via disco)
    if df_results is None:
        print(f"A carregar os resultados de disponibilidade a partir de: {input_path}")
        df_results = pd.read_csv(input_path)
    else:
        # Usamos uma cópia para não alterar o DataFrame partilhado
        df_results = df_results.copy()
    
    print("A gerar gráficos de pizza para a disponibilidade...")
    
    # Lista de anos e turbinas para iterar
    anos = df_results['Year'].unique()
    turbinas = df_results['Turbine'].unique()
    
    # 2. Gráficos de Pizza (Disponibilidade vs Indisponibilidade)
    for ano in anos:
        df_ano = df_results[df_results['Year'] == ano]
        
        # Cria uma janela com 2 linhas e 3 colunas para as 6 turbinas
        fig, axes = plt.subplots(2, 3, figsize=(15, 10))
        fig.suptitle(f'Disponibilidade Operacional das Turbinas - Ano {ano}', fontsize=16, fontweight='bold')
        
        # Achata a matriz de eixos para facilitar o loop
        axes = axes.flatten()
        
        for i, turbina in enumerate(turbinas):
            # Filtra os dados específicos da turbina e do ano
            dados_turbina = df_ano[df_ano['Turbine'] == turbina]
            
            # Se não houver dados para esta turbina neste ano, salta a iteração
            if dados_turbina.empty:
                continue
                
            dados_turbina = dados_turbina.iloc[0]
            
            # Valores para o gráfico de pizza
            labels = ['Disponível', 'Indisponível']
            sizes = [dados_turbina['Available_Hours'], dados_turbina['Unavailable_Hours']]
            cores = ['#2ecc71', '#e74c3c'] # Verde para disponível, Vermelho para indisponível
            
            # Plota o gráfico de pizza no subplot correspondente
            axes[i].pie(sizes, labels=labels, colors=cores, autopct='%1.2f%%', startangle=90, explode=(0, 0.1))
            axes[i].set_title(f'Turbina {turbina}')
        
        # Ajusta o layout e guarda a imagem
        plt.tight_layout()
        caminho_pizza = os.path.join(output_dir, f'disponibilidade_pizza_{ano}.png')
        plt.savefig(caminho_pizza, dpi=300)
        plt.close(fig) # Fecha explicitamente a figura 'fig' para libertar memória
        print(f" -> Gráfico do ano {ano} guardado com sucesso.")
        
    # 3. Gráfico de Barras para Perdas Financeiras Estimadas
    print("A gerar gráfico de impacto financeiro...")
    
    fig_bar, ax = plt.subplots(figsize=(12, 6))
    
    # Usamos o seaborn para criar um gráfico de barras agrupado por ano e turbina
    sns.barplot(data=df_results, x='Turbine', y='Financial_Loss_GBP', hue='Year', palette='viridis', ax=ax)
    
    ax.set_title('Estimativa de Perda Financeira por Indisponibilidade (£)', fontsize=14, fontweight='bold')
    ax.set_xlabel('Turbina', fontsize=12)
    ax.set_ylabel('Perda Estimada (£)', fontsize=12)
    ax.grid(axis='y', linestyle='--', alpha=0.7)
    ax.legend(title='Ano')
    
    caminho_financeiro = os.path.join(output_dir, 'perdas_financeiras_barras.png')
    plt.savefig(caminho_financeiro, dpi=300)
    plt.close(fig_bar)
    print(f" -> Gráfico financeiro guardado com sucesso em: {output_dir}")

if __name__ == "__main__":
    plot_availability_and_losses()