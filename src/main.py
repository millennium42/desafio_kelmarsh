import sys
import os

# Adiciona a raiz do projeto ao sys.path para evitar erros de importação de módulos
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# --- Importações dos módulos da Parte 1 (Disponibilidade e Falhas) ---
from src.data_loader import load_and_combine_data
from src.availability import calculate_availability_and_losses
from src.failure_analysis import analyze_top_failures
from src.visualization import plot_availability_and_losses
from src.visualization_failures import plot_top_failures

def main():
    print("="*60)
    print(" INICIANDO PROCESSAMENTO - DESAFIO KELMARSH (OTIMIZADO) ".center(60))
    print("="*60)
    
    # [PASSO 1] Extração e Limpeza Centralizada
    print("\n[PASSO 1/5] A ler, limpar e consolidar os dados brutos...")
    # O data_loader agora retorna o DataFrame limpo (e guarda o Parquet como backup)
    df_consolidado = load_and_combine_data()
    
    # Verificação de segurança caso não existam dados
    if df_consolidado is None or df_consolidado.empty:
        print("Erro crítico: Não foi possível carregar os dados. Verifique a pasta data/raw.")
        return

    # [PASSO 2] Cálculos de Disponibilidade e Perdas (In-Memory)
    print("\n[PASSO 2/5] A calcular disponibilidade temporal e impacto financeiro...")
    # Passamos o DataFrame diretamente pela memória
    df_resultados_disp = calculate_availability_and_losses(df=df_consolidado)
    
    # [PASSO 3] Análise de Falhas (In-Memory)
    print("\n[PASSO 3/5] A identificar as principais causas de falha (indisponibilidade)...")
    # Passamos o mesmo DataFrame limpo para extrair o Top 3
    df_top_falhas = analyze_top_failures(df=df_consolidado)
    
    # [PASSO 4] Geração de Gráficos de Disponibilidade (In-Memory)
    print("\n[PASSO 4/5] A gerar gráficos anuais de disponibilidade e perdas financeiras...")
    # Injetamos os resultados calculados no passo 2
    plot_availability_and_losses(df_results=df_resultados_disp)
    
    # [PASSO 5] Geração de Gráficos de Falhas (In-Memory)
    print("\n[PASSO 5/5] A gerar o painel de criticidade (Top 3 Falhas)...")
    # Injetamos os resultados calculados no passo 3
    plot_top_failures(df_top_failures=df_top_falhas)
    
    print("\n" + "="*60)
    print(" PIPELINE CONCLUÍDO COM SUCESSO! ".center(60))
    print("="*60)

if __name__ == "__main__":
    main()