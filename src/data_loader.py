import os
import glob
import pandas as pd

def load_and_combine_data(raw_path="data/raw", processed_path="data/processed"):
    """
    Função para ler todos os ficheiros CSV de turbinas, extrair o ID da turbina
    a partir do nome do ficheiro, combiná-los e guardar o resultado.
    """
    # Define o padrão de busca para encontrar apenas os ficheiros CSV que nos interessam
    pattern = os.path.join(raw_path, "Status_Kelmarsh_*.csv")
    
    # Utiliza a biblioteca glob para encontrar todos os caminhos que correspondem ao padrão
    file_list = glob.glob(pattern)
    
    # Cria uma lista vazia que irá armazenar os dados de cada ficheiro temporariamente
    dataframes = []
    
    print(f"Encontrados {len(file_list)} ficheiros. A iniciar a leitura...")
    
    # Inicia um ciclo (loop) para iterar sobre cada ficheiro encontrado
    for file in file_list:
        # Extrai apenas o nome do ficheiro a partir do caminho completo (exclui as pastas)
        filename = os.path.basename(file)
        
        # Divide o nome do ficheiro usando o '_' como separador para descobrir a turbina
        # Ex: em "Status_Kelmarsh_1_2019...", o índice 2 corresponde ao número "1"
        turbine_num = filename.split('_')[2]
        
        # Formata o identificador da turbina para ficar no padrão T1, T2, etc.
        turbine_id = f"T{turbine_num}"
        
        # Lê o ficheiro CSV utilizando a biblioteca pandas
        # NOTA: O dataset do Kelmarsh costuma ter algumas linhas de cabeçalho descritivo (metadata)
        # que precedem a tabela real. Pela estrutura comum do Zenodo, costumam ser as primeiras 9 linhas.
        # Caso ocorra um erro de leitura, podemos precisar adicionar o parâmetro: skiprows=9
        df = pd.read_csv(file, skiprows=9)
        
        # Adiciona uma nova coluna aos dados para identificar a qual turbina eles pertencem
        df['Turbine'] = turbine_id
        
        # Anexa o DataFrame atual à nossa lista de DataFrames
        dataframes.append(df)
        
        print(f"Ficheiro {filename} carregado. Identificada: {turbine_id} ({len(df)} registos)")
        
    # Combina (concatena) todos os DataFrames da lista num único DataFrame gigante
    print("\nA combinar todos os dados num único conjunto...")
    combined_df = pd.concat(dataframes, ignore_index=True)
    
    # A conversão de datas (timestamps) é crucial para a análise de disponibilidade.
    # Vamos procurar os nomes de colunas de tempo mais comuns nestes sistemas SCADA
    time_cols = ['Timestamp', 'Date and time', 'Time', 'Date_Time']
    
    # Percorre as possíveis colunas de tempo para encontrar qual está presente nos nossos dados
    for col in time_cols:
        if col in combined_df.columns:
            print(f"A converter a coluna '{col}' para o formato datetime do Pandas...")
            # Converte a coluna para datetime (o parâmetro coerce transforma erros em valores nulos/NaT)
            combined_df[col] = pd.to_datetime(combined_df[col], errors='coerce')
            
            # Renomeia a coluna para 'Timestamp' para garantir um padrão único nos próximos scripts
            combined_df.rename(columns={col: 'Timestamp'}, inplace=True)
            break # Interrompe o ciclo assim que encontra e converte a coluna correta
            
    # Define o caminho completo para o ficheiro final onde os dados consolidados serão guardados
    output_file = os.path.join(processed_path, "kelmarsh_consolidated.csv")
    
    # Guarda o conjunto de dados completo num novo ficheiro CSV na pasta 'processed'
    print(f"A guardar os dados processados e consolidados em: {output_file}")
    combined_df.to_csv(output_file, index=False)
    
    print("Processo concluído com sucesso!")
    return combined_df

# Este bloco garante que o código só é executado se este script for chamado diretamente
if __name__ == "__main__":
    # Chama a função principal
    load_and_combine_data()