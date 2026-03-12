# Desafio Kelmarsh - Análise de Dados SCADA 🌬️

Este repositório contém o código-fonte desenvolvido para resolver o desafio técnico de processamento e análise de dados SCADA das turbinas eólicas do parque de Kelmarsh. O software foi arquitetado de forma modular em **Python**, com foco em engenharia de dados, processamento vetorizado e otimização de memória para lidar com grandes volumes de dados temporais.

## ⚙️ Arquitetura do Software e Módulos

O projeto está dividido em scripts independentes, cada um responsável por uma etapa específica do pipeline de dados:

* **`src/data_loader.py`**: Pipeline de ETL inicial. Lê múltiplos ficheiros brutos (ignorando metadados), normaliza colunas de tempo, limpa anomalias e concatena os registos num único dataset consolidado.
* **`src/availability.py`**: Motor de cálculo financeiro e temporal. Aplica regras normativas (IEC) para classificar falhas técnicas e cruza o tempo de inatividade com tarifas do mercado de energia para estimar custos de oportunidade.
* **`src/failure_analysis.py`**: Agrupamento e agregação. Mapeia milhões de linhas de alarmes SCADA para elencar as causas-raiz mais críticas (Top 3) de indisponibilidade de cada máquina.
* **`src/visualization.py` e `src/visualization_failures.py`**: Módulos de *Data Visualization*. Utilizam `matplotlib` e `seaborn` para gerar relatórios visuais estáticos (gráficos de pizza, barras e painéis horizontais) a partir dos datasets processados.
* **`src/wake_effect.py`**: Módulo de alta performance e cálculo geodésico. Utiliza trigonometria para descobrir o azimute entre turbinas, lê os ficheiros massivos de dados operativos com `usecols` (otimização extrema de RAM), aplica filtros dinâmicos cruzados (direção, *pitch*, potência) e gera as curvas de potência binarizadas para avaliar o Efeito de Esteira.
* **`src/main.py`**: Script orquestrador que atua como o ponto de entrada principal para a execução sequencial e automatizada de todo o pipeline (Parte 1 e Parte 2).

## 📁 Estrutura do Diretório

```text
desafio_kelmarsh/
├── data/
│   ├── raw/                 # Ficheiros originais CSV (NÃO versionados no Git)
│   └── processed/           # Datasets consolidados gerados pelos scripts
├── src/
│   ├── data_loader.py       
│   ├── availability.py      
│   ├── failure_analysis.py  
│   ├── visualization.py     
│   ├── visualization_failures.py 
│   ├── wake_effect.py       
│   └── main.py              
├── output/plots/            # Gráficos finais gerados automaticamente (.png/.jpg)
├── requirements.txt         # Dependências do ambiente Python
└── README.md                # Documentação do projeto

```

## 🚀 Como Configurar e Executar

### 1. Clonar o repositório

```bash
git clone [https://github.com/millennium42/desafio_kelmarsh.git](https://github.com/millennium42/desafio_kelmarsh.git)
cd desafio_kelmarsh

```

### 2. Configurar o Ambiente Virtual

Recomenda-se a utilização de um ambiente virtual para isolar as dependências do projeto.

```bash
python3 -m venv venv
source venv/bin/activate  # Em ambientes Windows utilize: venv\Scripts\activate

```

### 3. Instalar Dependências

```bash
pip install -r requirements.txt

```

### 4. Preparação dos Dados Brutos (`data/raw/`)

O repositório não inclui os dados brutos devido ao seu tamanho. Antes de executar o código, certifique-se de alocar os seguintes ficheiros dentro da pasta `data/raw/`:

* Ficheiros de Status: `Status_Kelmarsh_..._23X.csv`
* Ficheiros de Operação (T2 e T3): `Turbine_Data_Kelmarsh_...csv`
* Ficheiro de Coordenadas: `Kelmarsh_WT_static.csv`

### 5. Execução do Pipeline Completo

O script orquestrador executará todo o fluxo de dados sequencialmente:

```bash
python src/main.py

```

*O que o script faz:* 1. Gera os datasets consolidados em `data/processed/`.
2. Processa os cálculos financeiros e as principais causas de falha.
3. Calcula o azimute e extrai as curvas de potência binarizadas.
4. Guarda todos os relatórios visuais (Disponibilidade, Falhas e Efeito de Esteira) em `output/plots/`.

```
