# Desafio Kelmarsh - Análise SCADA 🌬️

Repositório dedicado à solução do desafio técnico de análise de dados SCADA das turbinas eólicas do parque de Kelmarsh (2019-2021). O projeto foi desenvolvido em Python de forma modular, focado na análise de disponibilidade e falhas técnicas.

## 📁 Estrutura do Projeto

```text
desafio_kelmarsh/
├── data/
│   ├── raw/                 # Arquivos CSV originais do parque (não versionados)
│   └── processed/           # Datasets consolidados e resultados analíticos
├── src/
│   ├── data_loader.py       # Consolidação e limpeza dos arquivos SCADA
│   ├── availability.py      # Cálculo de disponibilidade e impacto financeiro
│   ├── failure_analysis.py  # Mapeamento do Top 3 falhas por turbina
│   ├── visualization.py     # Geração de gráficos de pizza e perdas
│   ├── visualization_failures.py # Geração do painel visual de criticidade
│   ├── wake_effect.py       # Cálculo de azimute para efeito de esteira
│   └── main.py              # Script orquestrador principal
├── output/plots/            # Gráficos e resultados visuais
├── requirements.txt         # Dependências do projeto
└── README.md

```

## ⚙️ Como Configurar e Executar

1. **Clone o repositório:**
```bash
git clone [https://github.com/millennium42/desafio_kelmarsh.git](https://github.com/millennium42/desafio_kelmarsh.git)
cd desafio_kelmarsh

```


2. **Crie e ative o ambiente virtual:**
```bash
python -m venv venv
source venv/bin/activate  # No Windows: venv\Scripts\activate

```


3. **Instale as dependências:**
```bash
pip install -r requirements.txt

```


4. **Prepare os Dados Brutos:**
Coloque os arquivos de dados SCADA (`Status_Kelmarsh_...csv`) e as coordenadas (`Kelmarsh_WT_static.csv`) na pasta `data/raw/`.

6. **Execute a análise (Parte 1):**
```bash
python src/main.py

```

