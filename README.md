# Desafio Kelmarsh - Análise de Dados SCADA 

Este repositório contém o código-fonte desenvolvido para resolver o desafio técnico de processamento e análise de dados SCADA das turbinas eólicas do parque de Kelmarsh. O software foi arquitetado de forma modular em **Python**, com um forte foco em engenharia de dados de alta performance, otimização extrema de memória RAM e alinhamento rigoroso com as normas internacionais (IEC 61400-12-1).

## 🚀 Principais Otimizações e Funcionalidades (Estado da Arte)

* **Leitura Multithread (PyArrow):** Utilização do motor C/C++ do PyArrow para carregar ficheiros CSV massivos numa fração do tempo tradicional.
* **Gestão Extrema de Memória:** Implementação de tipagem inteligente (conversão para `category`, downcasting explícito para `float32`) e pré-filtragem condicional para evitar estrangulamentos de RAM durante os cruzamentos de dados (`pd.merge`).
* **Correção de Densidade do Ar:** Algoritmo normativo (IEC 61400-12-1) integrado para normalizar a velocidade do vento com base na temperatura ambiente.
* **Processamento Vetorizado:** Substituição de iterações tradicionais por operações matemáticas vetorizadas com o NumPy, garantindo máxima eficiência computacional.

## ⚙️ Arquitetura do Software e Módulos

O projeto está estruturado em scripts independentes, cada um responsável por uma etapa crítica do pipeline de processamento:

* **`src/data_loader.py`**: Pipeline de ETL inicial. Lê múltiplos ficheiros brutos otimizados, limpa anomalias, processa timestamps em blocos e exporta um dataset consolidado ultra-leve no formato `.parquet`.
* **`src/availability.py`**: Motor de cálculo financeiro e temporal. Aplica regras normativas (IEC) para classificar falhas técnicas, utilizando vetorização para calcular a disponibilidade e cruzar tempos de inatividade com o preço do mercado de energia.
* **`src/failure_analysis.py`**: Agrupamento e agregação. Mapeia milhões de linhas de alarmes SCADA para elencar as causas-raiz mais críticas (Top 3) de indisponibilidade de cada máquina.
* **`src/visualization.py` e `src/visualization_failures.py`**: Módulos de *Data Visualization*. Utilizam `matplotlib` e `seaborn` para gerar relatórios visuais estáticos (gráficos de pizza, barras e painéis de criticidade) a partir dos datasets processados.
* **`src/wake_effect.py`**: Módulo central de aerodinâmica e performance. Utiliza trigonometria para descobrir o azimute entre turbinas, aplica filtros operacionais em memória e gera curvas de potência binarizadas (0.5 m/s) para avaliar o défice provocado pelo Efeito de Esteira.
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
│   └── main.py              
├── output/plots/            # Gráficos finais gerados automaticamente (.png)
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

O repositório não inclui os dados SCADA originais devido ao grande volume de dados. Antes de executar, certifique-se de descarregar e alocar os seguintes ficheiros dentro da pasta `data/raw/`:

* Ficheiros de Status: `Status_Kelmarsh_..._23X.csv`
* Ficheiros de Operação (Mínimo T2 e T3): `Turbine_Data_Kelmarsh_...csv`
* Ficheiro de Coordenadas: `Kelmarsh_WT_static.csv`

### 5. Execução do Pipeline Completo

O script orquestrador executará todo o fluxo de dados sequencialmente:

```bash
python src/main.py

```

**O que o script faz automaticamente:**

1. Lê e compacta os dados originais criando datasets consolidados em `data/processed/`.
2. Processa as métricas de disponibilidade temporal, impacto financeiro e o Top 3 das falhas.
3. Calcula o alinhamento geodésico, aplica a correção térmica e extrai as curvas de potência para avaliação aerodinâmica.
4. Exporta todos os relatórios visuais gerados para a pasta `output/plots/`.
