# Apuração de Viagens (SPPO & BRT)

Este projeto realiza a apuração de viagens de ônibus no Rio de Janeiro (BRT e Frescão) utilizando dados de GPS do Datalake da SMTR e arquivos GTFS.

## Estrutura do Projeto

- `codigos_py/`: Scripts em Python para apuração.
- `codigos_R/`: Scripts originais em R.
- `requirements.txt`: Dependências Python necessárias.

## Pré-requisitos

- [uv](https://github.com/astral-sh/uv) (Gerenciador de pacotes e ambientes Python rápido).
- Acesso ao Google BigQuery (Projeto `rj-smtr`).
- Arquivos GTFS localizados em `../../dados/gtfs/` (relativo à raiz deste projeto).

## Configuração do Ambiente Python

Para configurar o ambiente e instalar as dependências, execute os seguintes comandos na raiz do projeto:

```powershell
# Criar o ambiente virtual
uv venv

# Instalar as dependências
uv pip install -r requirements.txt
```

## Como Executar

Sempre utilize o interpretador do ambiente virtual criado:

### Apuração BRT
```powershell
.\.venv\Scripts\python.exe codigos_py/1.1_apuracao_brt.py
```

### Apuração Frescão
```powershell
.\.venv\Scripts\python.exe codigos_py/1.2_apuracao_frescao.py
```

## Dependências Principais

- `pandas` & `geopandas`: Manipulação de dados e geometrias.
- `basedosdados`: Interface para download de dados do Datalake.
- `google-cloud-bigquery`: Cliente para consultas diretas no BigQuery.
- `shapely`: Operações geométricas (buffers, interseções).
- `pyarrow`: Suporte a arquivos Parquet.

## Notas de Autenticação

Ao executar pela primeira vez, o `basedosdados` ou o cliente Google Cloud pode solicitar autenticação via navegador. Siga as instruções no terminal para autorizar o acesso ao projeto `rj-smtr`.
