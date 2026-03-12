import sys
import os

# Adiciona a raiz do projeto ao sys.path para evitar erros de importação de módulos
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_loader import load_and_combine_data
from src.availability import calculate_availability_and_losses
from src.failure_analysis import analyze_top_failures
from src.visualization import plot_availability_and_losses
from src.visualization_failures import plot_top_failures
from src.wake_effect import process_wake_effect

def main():
    print("="*60)
    print(" INICIANDO PROCESSAMENTO - DESAFIO KELMARSH ".center(60))
    print("="*60)
    
    # =========================================================
    # PARTE 1: Análise de Disponibilidade e Causas de Falha
    # =========================================================
    print("\n" + "-"*40)
    print(" PARTE 1: DISPONIBILIDADE E FALHAS ")
    print("-" * 40)
    
    print("\n[PASSO 1/6] A ler e consolidar os dados brutos das turbinas...")
    load_and_combine_data()
    
    print("\n[PASSO 2/6] A calcular disponibilidade temporal e impacto financeiro...")
    calculate_availability_and_losses()
    
    print("\n[PASSO 3/6] A identificar as principais causas de falha (indisponibilidade)...")
    analyze_top_failures()
    
    print("\n[PASSO 4/6] A gerar gráficos anuais de disponibilidade e perdas financeiras...")
    plot_availability_and_losses()
    
    print("\n[PASSO 5/6] A gerar o painel de criticidade (Top 3 Falhas)...")
    plot_top_failures()
    
    # =========================================================
    # PARTE 2: Análise de Curva de Potência e Efeito de Esteira
    # =========================================================
    print("\n" + "-"*40)
    print(" PARTE 2: EFEITO DE ESTEIRA (T2 -> T3) ")
    print("-" * 40)
    
    print("\n[PASSO 6/6] A calcular alinhamento, aplicar filtros e gerar a curva binarizada...")
    process_wake_effect()
    
    print("\n" + "="*60)
    print(" PIPELINE CONCLUÍDO COM SUCESSO! ".center(60))
    print("="*60)

if __name__ == "__main__":
    main()