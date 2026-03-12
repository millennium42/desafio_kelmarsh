import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

def plot_top_failures(df_top_failures=None, input_path="data/processed/top_failures.csv", output_dir="output/plots"):
    """
    Gera um painel com gráficos de barras horizontais mostrando as top 3 falhas
    de cada turbina em termos de horas paradas.
    Pode receber os dados dos resultados diretamente em memória ou ler do disco.
    """
    print("A gerar os gráficos de Criticidade de Falhas...")
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. Carregamento de Dados (em memória ou via disco)
    if df_top_failures is None:
        print(f"A carregar os dados de falhas a partir de: {input_path}")
        df_top_failures = pd.read_csv(input_path)
    else:
        # Cópia para não alterar o DataFrame original
        df_top_failures = df_top_failures.copy()
        
    turbinas = df_top_failures['Turbine'].unique()
    
    # 2. Configuração da Figura
    # Criar uma figura grande com 6 subplots (2 linhas, 3 colunas)
    fig, axes = plt.subplots(2, 3, figsize=(18, 10))
    fig.suptitle('Análise de Criticidade: Top 3 Causas de Indisponibilidade por Turbina (2019-2021)', 
                 fontsize=18, fontweight='bold')
    
    axes = axes.flatten()
    
    # 3. Iterar sobre cada turbina e desenhar o gráfico
    for i, turbina in enumerate(turbinas):
        # Filtrar os dados da turbina e ordenar da maior para a menor duração
        df_t = df_top_failures[df_top_failures['Turbine'] == turbina].sort_values('Duration_Hours', ascending=True)
        
        # Prevenção de erros caso uma turbina não tenha falhas registadas
        if df_t.empty:
            continue
        
        # Como padronizámos no data_loader, a coluna chama-se garantidamente 'Alarm_Message'
        col_alarme = 'Alarm_Message'
        
        # Limpar mensagens de erro muito longas para caberem perfeitamente no gráfico
        df_t['Alarme_Curto'] = df_t[col_alarme].apply(lambda x: str(x)[:30] + '...' if len(str(x)) > 30 else str(x))
        
        # Gerar o barplot horizontal
        sns.barplot(x='Duration_Hours', y='Alarme_Curto', hue='Alarme_Curto', data=df_t, ax=axes[i], palette='Reds_r', legend=False)
       
        # Formatação visual do subplot
        axes[i].set_title(f'Turbina {turbina}', fontsize=14, fontweight='bold')
        axes[i].set_xlabel('Total de Horas Paradas', fontsize=11)
        axes[i].set_ylabel('')
        axes[i].grid(axis='x', linestyle='--', alpha=0.7)
        
        # Adicionar o número de horas no final de cada barra visualmente
        for index, value in enumerate(df_t['Duration_Hours']):
            axes[i].text(value + 5, index, f'{value:.0f}h', va='center', fontsize=10, fontweight='bold')

    # Ajustar os espaços para que os textos longos dos alarmes não se sobreponham
    plt.tight_layout(rect=(0.0, 0.03, 1.0, 0.95))
    
    # Guardar a imagem e libertar memória explicitamente
    caminho_imagem = os.path.join(output_dir, 'criticidade_falhas_barras.png')
    plt.savefig(caminho_imagem, dpi=300, bbox_inches='tight')
    plt.close(fig)
    
    print(f" -> Sucesso! Gráfico de falhas salvo em: {caminho_imagem}")

if __name__ == "__main__":
    plot_top_failures()