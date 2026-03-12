# Desafio Kelmarsh - Análise de Dados SCADA 🌬️

Este repositório contém o código-fonte desenvolvido para resolver o desafio técnico de processamento e análise de dados SCADA das turbinas eólicas do parque de Kelmarsh. O software foi arquitetado de forma modular em **Python**, com um forte foco em engenharia de dados de alta performance, otimização extrema de memória RAM, alinhamento com as normas internacionais (IEC 61400-12-1) e **Manutenção Preditiva**.

## 🚀 Principais Otimizações e Funcionalidades (Estado da Arte)

* **Leitura Multithread (PyArrow):** Utilização do motor C/C++ do PyArrow para carregar ficheiros CSV massivos numa fração do tempo tradicional.
* **Gestão Extrema de Memória:** Implementação de tipagem inteligente (conversão para `category`, *downcasting* explícito em lotes para `float32`) e pré-filtragem precoce para evitar estrangulamentos de RAM.
* **Correção de Densidade do Ar:** Algoritmo normativo (IEC 61400-12-1) integrado para normalizar a velocidade do vento com base na temperatura ambiente local da *nacelle*.
* **Processamento Vetorizado:** Substituição de iterações tradicionais por operações matemáticas vetorizadas no NumPy.
* **Manutenção Preditiva (Novo):** Análise exploratória avançada (EDA) que cruza a telemetria de vibração mecânica e estresse térmico para detetar anomalias antes da falha.

## ⚙️ Arquitetura do Software e Módulos

O projeto está estruturado em scripts independentes, cada um responsável por uma etapa crítica do pipeline de processamento:

* **`src/data_loader.py`**: Pipeline de ETL inicial. Lê múltiplos ficheiros brutos, limpa anomalias, processa timestamps em blocos e exporta um dataset consolidado ultra-leve no formato `.parquet`.
* **`src/availability.py`**: Motor de cálculo financeiro e temporal. Aplica regras normativas (IEC) para classificar falhas técnicas e calcular perdas operacionais estimadas.
* **`src/failure_analysis.py`**: Agrupamento e agregação. Mapeia milhões de linhas de alarmes SCADA para elencar as causas-raiz mais críticas (Top 3) de indisponibilidade de cada máquina.
* **`src/visualization.py` e `src/visualization_failures.py`**: Módulos de *Data Visualization* que geram relatórios visuais estáticos sobre disponibilidade e criticidade.
* **`src/wake_effect.py`**: Módulo central de aerodinâmica. Calcula o azimute geodésico, aplica a correção térmica do ar e gera curvas de potência para avaliar o défice provocado pelo Efeito de Esteira.
* **`src/predictive_maintenance.py`**: **[NOVO]** Módulo base para Machine Learning. Extrai telemetria pesada (temperaturas de rolamentos, vibração do trem de força) e gera um *dashboard* com as correlações preditivas em função do estado operacional das turbinas.
* **`src/main.py`**: Script orquestrador que atua como o ponto de entrada principal para a execução sequencial, limpa e automatizada de todo o pipeline de ponta a ponta.

## 📁 Estrutura do Diretório

```text
desafio_kelmarsh/
├── data/
│   ├── raw/                 # Ficheiros originais CSV (NÃO versionados no Git)
│   └── processed/           # Datasets consolidados gerados pelos scripts (.parquet / .csv)
├── src/
│   ├── data_loader.py       
│   ├── availability.py      
│   ├── failure_analysis.py  
│   ├── visualization.py     
│   ├── visualization_failures.py 
│   ├── wake_effect.py       
│   ├── predictive_maintenance.py  # <-- Novo módulo de Machine Learning / EDA
│   └── main.py              
├── output/plots/            # Gráficos e painéis finais gerados automaticamente (.png)
├── requirements.txt         # Dependências do ambiente Python
└── README.md                # Documentação do projeto

```

## 🛠️ Como Configurar e Executar

### 1. Clonar o repositório

```bash
git clone [https://github.com/millennium42/desafio_kelmarsh.git](https://github.com/millennium42/desafio_kelmarsh.git)
cd desafio_kelmarsh

```

### 2. Configurar o Ambiente Virtual

Recomenda-se a utilização de um ambiente virtual para isolar as dependências do projeto.

```bash
python3 -m venv .venv
source .venv/bin/activate  # Em ambientes Windows utilize: .venv\Scripts\activate

```

### 3. Instalar Dependências

```bash
pip install -r requirements.txt

```

### 4. Preparação dos Dados Brutos (`data/raw/`)

O repositório não inclui os dados SCADA originais devido ao grande volume. Antes de executar, certifique-se de alocar os seguintes ficheiros na pasta `data/raw/`:

* Ficheiros de Status: `Status_Kelmarsh_..._23X.csv`
* Ficheiros de Operação: `Turbine_Data_Kelmarsh_...csv`
* Ficheiro de Coordenadas: `Kelmarsh_WT_static.csv`

### 5. Execução do Pipeline Completo

O script orquestrador executará todo o fluxo de dados sequencialmente:

```bash
python src/main.py

```

**O que o script faz automaticamente:**

1. Consolida os ficheiros brutos num único arquivo `.parquet`.
2. Calcula as métricas de disponibilidade temporal e o impacto financeiro (Parte 1).
3. Identifica e extrai o Top 3 das falhas causadoras de paragem técnica.
4. Calcula o alinhamento geográfico, aplica a normalização IEC (densidade do ar) e extrai o défice provocado pelo Efeito Esteira (Parte 2).
5. Extrai a telemetria avançada de vibração e temperatura para gerar o Dashboard Preditivo (Parte 3).
6. Exporta todos os relatórios visuais (incluindo painéis e curvas binarizadas) para a pasta `output/plots/`.
