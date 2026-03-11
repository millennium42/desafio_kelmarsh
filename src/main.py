import sys
import os

# Esta linha garante que o Python reconhece a pasta 'src' como um módulo, 
# independentemente de onde o script seja executado, evitando erros de importação.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_loader import load_and_combine_data
from src.availability import calculate_availability_and_losses
from src.visualization import plot_availability_and_losses
from src.failure_analysis import analyze_top_failures

def main():
    print("="*55)
    print(" INICIANDO PROCESSAMENTO - DESAFIO KELMARSH (PARTE 1.1) ")
    print("="*55)
    
    # Passo 1: Ler e combinar os dados brutos
    print("\n[PASSO 1/4] A ler e consolidar os dados das turbinas...")
    load_and_combine_data()
    
    # Passo 2: Lógica de negócio e cálculos
    print("\n[PASSO 2/4] A calcular disponibilidade e impacto financeiro...")
    calculate_availability_and_losses()
    
    # Passo 3: Geração de relatórios visuais
    print("\n[PASSO 3/4] A gerar gráficos de resultados (Pizza e Barras)...")
    plot_availability_and_losses()
    
    # Passo 4: Extraçlão das principais causas de falha
    print("\n[PASSO 4/4] A extrair as principais causas de falha...")
    analyze_top_failures()
    
    print("\n" + "="*55)
    print(" PROCESSAMENTO CONCLUÍDO COM SUCESSO! ")
    print(" Verifique os resultados em 'data/processed' e 'output/plots'.")
    print("="*55)

if __name__ == "__main__":
    main()