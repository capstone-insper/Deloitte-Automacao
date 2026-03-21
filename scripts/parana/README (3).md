# ETL Pipeline — Camada Analítica Executiva (Paraná)

Pipeline Python que lê as bases brutas em `entrada/`, limpa, transforma e gera uma camada analítica completa em `output/parana/`, pronta para consumo direto em dashboard executivo (Power BI ou similar).

---

## Bases de Entrada

| Arquivo | Tipo | Formato bruto | Conteúdo |
|---------|------|--------------|---------|
| `BookService.txt` | Orçamento | Wide — Area × Tipo × Mês | Receita, Custo e Margin planejados por área para os 12 meses (jun/25–mai/26) |
| `data1.csv.txt` | Operacional | Transacional | Registros bi-semanais por funcionário, projeto e centro de custo com receita prevista e realizada |

Ambos os arquivos estão em encoding **UTF-16**, separador **tab**, valores no formato monetário brasileiro (`R$ 1.234,56`).

---

## Pré-requisitos e Execução

```bash
pip install pandas numpy openpyxl
```

Execute a partir da raiz do projeto:

```bash
python scripts/parana/etl_parana.py
```

O script imprime o progresso de cada etapa no console e gera os 12 arquivos em `output/parana/`.

> **Importante:** feche todos os arquivos de output no Excel antes de rodar. O Excel bloqueia a escrita e o script retornara `PermissionError`.

> **Nota:** caracteres acentuados podem aparecer corrompidos no terminal do Windows (cp1252), mas os arquivos `.xlsx` e `.csv` gerados são salvos corretamente em UTF-8.

---

## Fluxo do Pipeline

```
entrada/
  BookService.txt  ──────────────────────────────┐
  data1.csv.txt    ─────────────────────────────┐ │
                                                │ │
  detectar_arquivos_txt()                       │ │
  carregar_txt()                                │ │
  identificar_tipo_base()                       │ │
                                                ▼ ▼
                              limpar_operacional()   limpar_orcamento()
                                       │                     │
                        calcular_metricas_operacional()       │
                                       │                     │
                               ┌───────┴──────────────────────┘
                               │      (join por area + mes_ano)
                               ▼
                    criar_kpis_executivos()
                    criar_serie_temporal()
                    criar_orcado_vs_realizado_area()
                    criar_receita_por_projeto()
                    criar_receita_por_subarea()
                    criar_custos_por_dimensao()
                    criar_ranking_desvios()
                    criar_participacao_percentual()
                    criar_orcamento_mensal_completo()
                    criar_janelas_temporais()
                               │
                    exportar_todos_outputs()
                               │
                               ▼
                         output/parana/
                           01 a 12 .xlsx
```

---

## Arquitetura do Código (`etl_parana.py`)

### Secao 1 — Leitura

| Funcao | Responsabilidade |
|--------|-----------------|
| `detectar_arquivos_txt(pasta)` | Varre `entrada/` com `glob("*.txt")` e retorna lista de caminhos |
| `carregar_txt(caminho)` | Tenta encodings em cascata (UTF-16 → UTF-16-LE → UTF-8 → Latin1) e autodetecta o delimitador por contagem de ocorrencias |
| `identificar_tipo_base(df)` | Classifica o DataFrame como `orcamento` (presenca de coluna `Type`) ou `operacional` (presenca de `Funcionario`) |

### Secao 2 — Padronizacao

| Funcao | Responsabilidade |
|--------|-----------------|
| `remover_acentos(texto)` | Decomposicao Unicode (NFD) + remocao de marcas diacriticas |
| `padronizar_colunas(df)` | Nomes em snake_case, lowercase, sem acentos, sem caracteres especiais |
| `limpar_valor_monetario(serie)` | Remove `R$`, separador de milhar (`.`), troca decimal (`,` → `.`), converte para `float` |

### Secao 3 — Limpeza por base

| Funcao | Responsabilidade |
|--------|-----------------|
| `limpar_operacional(df)` | Converte colunas monetarias, parseia datas (`dayfirst=True`), valida `id_quinzena` (1 ou 2), cria `ano`, `mes`, `ano_mes_str` |
| `limpar_orcamento(df)` | Padroniza colunas, detecta colunas de mes por regex (`jun_25` etc.), aplica `melt` (wide → long), parseia mes de referencia para `datetime` |

### Secao 4 — Metricas derivadas

| Funcao / Helper | Responsabilidade |
|-----------------|-----------------|
| `calcular_metricas_operacional(df)` | Cria `custo_total`, `desvio_receita`, `desvio_pct`, `atingimento_pct`, `receita_ajustada` |
| `_extrair_orcado_receita(df_orc)` | Filtra tipo `Receita` do orcamento e agrega por area + mes |
| `_extrair_orcado_custo(df_orc)` | Filtra tipo `Custo` do orcamento e agrega por area + mes |
| `_extrair_orcado_margem(df_orc)` | Filtra tipo `Margin` do orcamento e agrega por area + mes |

### Secao 5 — Tabelas analiticas

Uma funcao dedicada por output. Todas recebem `df_op` (base operacional limpa) e/ou `df_orc` (orcamento longo) como parametros.

| Funcao | Output gerado |
|--------|--------------|
| `criar_kpis_executivos` | 03 |
| `criar_serie_temporal` | 04 |
| `criar_orcado_vs_realizado_area` | 05 |
| `criar_receita_por_projeto` | 06 |
| `criar_receita_por_subarea` | 07 |
| `criar_custos_por_dimensao` | 08 |
| `criar_ranking_desvios` | 09 |
| `criar_participacao_percentual` | 10 |
| `criar_orcamento_mensal_completo` | 11 |
| `criar_janelas_temporais` | 12 |

### Secao 6 — Exportacao

| Funcao | Responsabilidade |
|--------|-----------------|
| `exportar_xlsx(df, caminho, nome_aba)` | Salva DataFrame em `.xlsx` com openpyxl |
| `exportar_xlsx_multiplas_abas(dfs, caminho)` | Salva dict de DataFrames em abas separadas de um unico `.xlsx` |
| `exportar_todos_outputs(df_op, df_orc, pasta)` | Orquestra a geracao e salvamento dos 12 outputs |

### Orquestracao

`run_pipeline()` — ponto de entrada unico. Chama em sequencia: detectar → carregar → identificar → limpar → calcular → exportar.

---

## Outputs Gerados em `output/parana/`

### 01 — `01_base_operacional_limpa.xlsx` + `.csv`

**Granularidade:** 1 linha = 1 registro bi-semanal por funcionario × projeto
**Uso no dashboard:** tabela de dados completa para drill-through e filtros detalhados

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `ajuste` | float | Valor de ajuste financeiro aplicado ao registro (R$) |
| `allowance` | float | Custo de allowance do funcionario para a quinzena (R$) |
| `funcionario` | texto | Classificacao do funcionario: `STAFF` ou `EXECUTIVO` |
| `centro_de_custo` | texto | Centro de custo alocado: CC1, CC2 ou CC3 |
| `projeto` | texto | Projeto referente ao registro: P1 a P10 |
| `contingencia` | float | Valor de contingencia reservada para o periodo (R$) |
| `area` | texto | Area de servico: SL01 ou SL02 |
| `mes_ano` | data | Data de referencia do registro (sempre dia 1 do mes) |
| `id_quinzena` | int | Quinzena dentro do mes: `1` (1a metade) ou `2` (2a metade) |
| `receita_prevista` | float | Receita planejada para o registro no periodo (R$) |
| `receita_liquida` | float | Receita efetivamente realizada no periodo (R$) |
| `sigla_sub_area` | texto | Sub-area: `CO` (Comercial), `AI` (AI & Data), `En` (Engenharia) |
| `ano` | int | Ano extraido de `mes_ano` |
| `mes` | int | Numero do mes extraido de `mes_ano` |
| `ano_mes_str` | texto | Periodo no formato `YYYY-MM` (ex: `2025-12`) |
| `custo_total` | float | allowance + contingencia — proxy do custo total do registro (R$) |
| `desvio_receita` | float | receita_liquida − receita_prevista (positivo = acima do plano) |
| `desvio_pct` | float | Desvio percentual sobre receita prevista (%) |
| `atingimento_pct` | float | receita_liquida / receita_prevista × 100 (%) |
| `receita_ajustada` | float | receita_liquida − allowance − contingencia (margem operacional bruta) |

---

### 02 — `02_base_orcamento_longa.xlsx`

**Granularidade:** 1 linha = area × tipo × mes
**Uso no dashboard:** base de referencia orcamentaria; alimenta joins com o realizado

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `area` | texto | Area de servico: SL01 ou SL02 |
| `tipo` | texto | Metrica orcada: `Receita`, `Custo` ou `Margin` |
| `mes_ref` | texto | Codigo original do mes apos padronizacao (ex: `jun_25`) |
| `valor` | float | Valor orcado para area + tipo + mes (R$) |
| `mes_ano` | data | Data de referencia do mes (sempre dia 1) |
| `ano` | int | Ano de referencia |
| `mes` | int | Numero do mes |
| `ano_mes_str` | texto | Periodo no formato `YYYY-MM` |

---

### 03 — `03_kpis_executivos.xlsx`

**Granularidade:** 1 linha por area + 1 linha `TOTAL`
**Uso no dashboard:** cards KPI e scorecard executivo — visao consolidada de performance

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `area` | texto | Area de servico (SL01, SL02 ou TOTAL) |
| `receita_prevista_total` | float | Soma da receita prevista operacional no periodo com dados (R$) |
| `receita_realizada_total` | float | Soma da receita liquida realizada (R$) |
| `custo_total_realizado` | float | Soma de allowance + contingencia realizados (R$) |
| `n_registros` | int | Quantidade de registros transacionais |
| `atingimento_medio_pct` | float | Media do atingimento individual por registro (%) |
| `orcado_receita` | float | Receita orcada (BookService) nos meses com dados realizados (R$) |
| `desvio_absoluto` | float | Receita realizada − receita orcada (R$) |
| `desvio_pct` | float | Desvio percentual versus orcamento (%) |
| `taxa_execucao_pct` | float | Receita realizada / receita orcada × 100 (%) |
| `margem_estimada` | float | Receita realizada − custo total realizado (R$) |
| `margem_pct` | float | Margem estimada / receita realizada × 100 (%) |

---

### 04 — `04_serie_temporal_mensal.xlsx`

**Granularidade:** 1 linha = area × mes (todos os 12 meses do orcamento)
**Uso no dashboard:** grafico de linha ou area empilhada — evolucao orcado vs realizado ao longo do tempo

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `area` | texto | Area de servico |
| `mes_ano` | data | Data de referencia (dia 1 do mes) |
| `ano_mes_str` | texto | Periodo `YYYY-MM` |
| `orcado_receita` | float | Receita orcada no mes — BookService (R$) |
| `orcado_custo` | float | Custo orcado no mes (R$) |
| `orcado_margem` | float | Margem orcada no mes (R$) |
| `receita_prevista_op` | float | Receita prevista operacional agregada no mes (R$) |
| `receita_realizada` | float | Receita liquida realizada no mes — `0` nos meses sem dados operacionais |
| `custo_realizado` | float | Custo total realizado no mes — `0` nos meses sem dados operacionais (R$) |
| `desvio_receita` | float | Desvio interno realizado − previsto operacional no mes — `0` nos meses sem dados |
| `desvio_vs_orcado` | float | Realizado − orcado BookService (R$) |
| `desvio_vs_previsto` | float | Realizado − previsto operacional (R$) |
| `taxa_execucao_pct` | float | Realizado / orcado × 100 (%) |
| `n_registros` | int | Quantidade de registros no mes — `0` nos meses sem dados operacionais |

---

### 05 — `05_orcado_vs_realizado_area.xlsx`

**Granularidade:** 1 linha por area
**Uso no dashboard:** grafico de barras agrupadas — comparacao direta orcado × realizado por area

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `area` | texto | Area de servico |
| `orcado_receita` | float | Total orcado nos meses com dados realizados (R$) |
| `receita_realizada` | float | Total realizado (R$) |
| `receita_prevista_op` | float | Total previsto operacional (R$) |
| `desvio_absoluto` | float | Realizado − orcado (R$) |
| `desvio_pct` | float | Desvio percentual (%) |
| `taxa_execucao_pct` | float | Taxa de execucao do orcamento (%) |

---

### 06 — `06_receita_por_projeto.xlsx`

**Granularidade:** 1 linha por projeto
**Uso no dashboard:** ranking de projetos por receita, grafico de barras horizontais

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `projeto` | texto | Codigo do projeto (P1 a P10) |
| `area` | texto | Area de servico do projeto |
| `sigla_sub_area` | texto | Sub-area predominante do projeto |
| `receita_prevista_sum` | float | Receita prevista acumulada do projeto (R$) |
| `receita_realizada_sum` | float | Receita realizada acumulada do projeto (R$) |
| `desvio_sum` | float | Desvio acumulado (R$) |
| `custo_total_sum` | float | Custo total acumulado do projeto (R$) |
| `atingimento_medio_pct` | float | Media de atingimento por registro do projeto (%) |
| `n_quinzenas` | int | Quantidade de registros quinzenais do projeto |
| `desvio_pct` | float | Desvio percentual do projeto (%) |
| `receita_ajustada_sum` | float | Receita realizada − custo total — margem estimada do projeto (R$) |
| `ranking` | int | Posicao no ranking por receita realizada (1 = maior) |

---

### 07 — `07_receita_por_subarea.xlsx`

**Granularidade:** 1 linha por sub-area (CO, AI, En)
**Uso no dashboard:** pizza ou donut — participacao de cada pratica na receita total

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `sigla_sub_area` | texto | Codigo da sub-area: `CO` (Comercial), `AI` (AI & Data), `En` (Engenharia) |
| `receita_prevista_sum` | float | Receita prevista acumulada (R$) |
| `receita_realizada_sum` | float | Receita realizada acumulada (R$) |
| `desvio_sum` | float | Desvio acumulado (R$) |
| `custo_total_sum` | float | Custo total acumulado (R$) |
| `atingimento_medio_pct` | float | Media de atingimento por registro (%) |
| `n_registros` | int | Quantidade de registros |
| `participacao_pct` | float | Participacao da sub-area na receita total realizada (%) |
| `desvio_pct` | float | Desvio percentual da sub-area (%) |

---

### 08 — `08_custos_por_dimensao.xlsx`

**Granularidade:** 1 linha por centro de custo × tipo de funcionario
**Uso no dashboard:** analise de estrutura de custos — treemap ou waterfall

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `centro_de_custo` | texto | Centro de custo: CC1, CC2 ou CC3 |
| `funcionario` | texto | Tipo de funcionario: `STAFF` ou `EXECUTIVO` |
| `allowance_sum` | float | Total de allowance pago no periodo (R$) |
| `contingencia_sum` | float | Total de contingencia reservada no periodo (R$) |
| `custo_total_sum` | float | Custo total = allowance + contingencia (R$) |
| `n_registros` | int | Quantidade de registros |
| `participacao_custo_pct` | float | Participacao desse grupo no custo total geral (%) |

---

### 09 — `09_ranking_desvios_projeto.xlsx`

**Granularidade:** 1 linha por projeto, ordenado por magnitude de desvio absoluto
**Uso no dashboard:** tabela de alertas — projetos mais distantes do planejado (em qualquer direcao)

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `ranking` | int | Posicao no ranking (1 = maior desvio em valor absoluto) |
| `projeto` | texto | Codigo do projeto |
| `area` | texto | Area de servico |
| `sigla_sub_area` | texto | Sub-area |
| `desvio_absoluto` | float | Realizado − previsto (pode ser negativo = abaixo do plano) |
| `receita_realizada` | float | Receita realizada acumulada (R$) |
| `receita_prevista` | float | Receita prevista acumulada (R$) |
| `desvio_pct` | float | Desvio percentual (%) |

---

### 10 — `10_participacao_percentual.xlsx` _(3 abas)_

**Granularidade:** 1 linha por dimensao, ordenado por valor decrescente
**Uso no dashboard:** analise de concentracao (curva de Pareto) e grafico de pizza/donut por dimensao

**Abas:** `Por Area` | `Por Projeto` | `Por Sub-Area`

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `dimensao` | texto | Valor da dimensao (area, projeto ou sub-area) |
| `valor` | float | Receita realizada total (R$) |
| `participacao_pct` | float | Participacao individual no total (%) |
| `participacao_acumulada_pct` | float | Participacao acumulada — eixo da curva de Pareto (%) |

---

### 12 — `12_janelas_temporais.xlsx`

**Granularidade:** 1 linha = janela × area (+ linha `TOTAL` por janela)
**Uso no dashboard:** tabela de comparacao temporal — visao executiva de performance por horizonte

A data de referencia e determinada automaticamente como o ultimo mes com dados operacionais presentes na base. Janelas sem nenhum dado operacional sao omitidas.

| Janela | Periodo coberto |
|--------|----------------|
| `M` | Mes mais recente com dados |
| `M-1` | Mes anterior ao mais recente |
| `3M` | Ultimos 3 meses com dados |
| `6M` | Ultimos 6 meses com dados |
| `12M` | Ultimos 12 meses com dados |
| `YTD` | Janeiro do ano corrente ate o mes mais recente |

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `janela` | texto | Identificador da janela: `M`, `M-1`, `3M`, `6M`, `12M` ou `YTD` |
| `data_inicio` | texto | Mes de inicio da janela no formato `YYYY-MM` |
| `data_fim` | texto | Mes de fim da janela no formato `YYYY-MM` |
| `area` | texto | Area de servico (SL01, SL02 ou TOTAL) |
| `orcado_receita` | float | Receita orcada (BookService) no periodo da janela (R$) |
| `receita_prevista` | float | Receita prevista operacional no periodo (R$) |
| `receita_realizada` | float | Receita liquida realizada no periodo (R$) |
| `desvio_vs_orcado` | float | Realizado − orcado (R$) |
| `taxa_execucao_pct` | float | Realizado / orcado × 100 (%) |
| `desvio_vs_previsto` | float | Realizado − previsto operacional (R$) |
| `atingimento_pct` | float | Realizado / previsto operacional × 100 (%) |
| `custo_total` | float | Soma de allowance + contingencia no periodo (R$) |
| `margem_estimada` | float | Receita realizada − custo total (R$) |
| `margem_pct` | float | Margem estimada / receita realizada × 100 (%) |
| `n_registros` | int | Quantidade de registros transacionais no periodo |

---

### 11 — `11_orcamento_mensal_completo.xlsx`

**Granularidade:** 1 linha = area × mes (todos os 12 meses do horizonte orcamentario)
**Uso no dashboard:** tabela de referencia — baseline estrategico completo e calendario de metas

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `area` | texto | Area de servico |
| `mes_ano` | data | Data de referencia do mes (dia 1) |
| `ano_mes_str` | texto | Periodo `YYYY-MM` |
| `receita_orcada` | float | Receita orcada para o mes — BookService (R$) |
| `custo_orcado` | float | Custo orcado para o mes (R$) |
| `margem_orcada` | float | Margem orcada = receita − custo (R$) |
| `margem_pct` | float | Margem orcada percentual sobre receita (%) |
| `mes_nome` | texto | Rotulo legivel do mes (ex: `Jun/2025`) |

---

## Estrutura de Diretorios

```
projeto/
├── entrada/
│   ├── BookService.txt          # orcamento (input)
│   └── data1.csv.txt            # operacional (input)
│
├── scripts/
│   └── parana/
│       ├── etl_parana.py        # pipeline principal
│       └── README.md            # esta documentacao
│
└── output/
    └── parana/
        ├── 01_base_operacional_limpa.xlsx
        ├── 01_base_operacional_limpa.csv
        ├── 02_base_orcamento_longa.xlsx
        ├── 03_kpis_executivos.xlsx
        ├── 04_serie_temporal_mensal.xlsx
        ├── 05_orcado_vs_realizado_area.xlsx
        ├── 06_receita_por_projeto.xlsx
        ├── 07_receita_por_subarea.xlsx
        ├── 08_custos_por_dimensao.xlsx
        ├── 09_ranking_desvios_projeto.xlsx
        ├── 10_participacao_percentual.xlsx
        ├── 11_orcamento_mensal_completo.xlsx
        └── 12_janelas_temporais.xlsx
```
