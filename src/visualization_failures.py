import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

def plot_top_failures(input_path="data/processed/top_failures.csv", output_dir="output/plots"):
    """
    Gera um painel com gráficos de barras horizontais mostrando as top 3 falhas
    de cada turbina em termos de horas paradas, baseando-se no ficheiro top_failures.csv.
    """
    print("A gerar os gráficos de Criticidade de Falhas...")
    os.makedirs(output_dir, exist_ok=True)
    
    # Carrega os dados processados no passo 1.2
    df = pd.read_csv(input_path)
    turbinas = df['Turbine'].unique()
    
    # Vamos criar uma figura grande com 6 subplots (2 linhas, 3 colunas)
    fig, axes = plt.subplots(2, 3, figsize=(18, 10))
    fig.suptitle('Análise de Criticidade: Top 3 Causas de Indisponibilidade por Turbina (2019-2021)', 
                 fontsize=18, fontweight='bold')
    
    axes = axes.flatten()
    
    # Iterar sobre cada turbina e desenhar o gráfico
    for i, turbina in enumerate(turbinas):
        # Filtrar os dados da turbina e ordenar da maior para a menor duração para o gráfico horizontal
        df_t = df[df['Turbine'] == turbina].sort_values('Duration_Hours', ascending=True)
        
        # O nome da coluna do alarme será 'Message' ou 'Status' dependendo de como o Pandas a salvou
        col_alarme = 'Message' if 'Message' in df_t.columns else 'Status' if 'Status' in df_t.columns else df_t.columns[1]
        
        # Limpar um pouco as mensagens de erro se forem muito longas para caber no gráfico
        df_t['Alarme_Curto'] = df_t[col_alarme].apply(lambda x: str(x)[:30] + '...' if len(str(x)) > 30 else str(x))
        
        # Gerar o barplot horizontal
        sns.barplot(x='Duration_Hours', y='Alarme_Curto', hue='Alarme_Curto', data=df_t, ax=axes[i], palette='Reds_r', legend=False)
       
        # Formatação visual
        axes[i].set_title(f'Turbina {turbina}', fontsize=14, fontweight='bold')
        axes[i].set_xlabel('Total de Horas Paradas', fontsize=11)
        axes[i].set_ylabel('')
        axes[i].grid(axis='x', linestyle='--', alpha=0.7)
        
        # Adicionar o número de horas no final de cada barra
        for index, value in enumerate(df_t['Duration_Hours']):
            axes[i].text(value + 5, index, f'{value:.0f}h', va='center', fontsize=10, fontweight='bold')

    # Ajustar os espaços para que os textos longos dos alarmes não se sobreponham
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    
    # Guardar a imagem
    caminho_imagem = os.path.join(output_dir, 'criticidade_falhas_barras.png')
    plt.savefig(caminho_imagem, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"Sucesso! Gráfico de falhas salvo em: {caminho_imagem}")

if __name__ == "__main__":
    plot_top_failures()