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
    print(" INICIANDO PROCESSAMENTO - DESAFIO KELMARSH (PARTE 1) ".center(60))
    print("="*60)
    
    print("\n[PASSO 1/5] A ler e consolidar os dados brutos das turbinas...")
    load_and_combine_data()
    
    print("\n[PASSO 2/5] A calcular disponibilidade temporal e impacto financeiro...")
    calculate_availability_and_losses()
    
    print("\n[PASSO 3/5] A identificar as principais causas de falha (indisponibilidade)...")
    analyze_top_failures()
    
    print("\n[PASSO 4/5] A gerar gráficos anuais de disponibilidade e perdas financeiras...")
    plot_availability_and_losses()
    
    print("\n[PASSO 5/5] A gerar o painel de criticidade (Top 3 Falhas)...")
    plot_top_failures()
    
    print("\n" + "="*60)
    print(" PARTE 1 CONCLUÍDA COM SUCESSO! ".center(60))
    print("="*60)

if __name__ == "__main__":
    main()