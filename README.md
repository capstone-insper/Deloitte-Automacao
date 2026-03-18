# Arquitetura de Automação de Dados para Monitoramento Estratégico na Deloitte

## Descrição do projeto

Este projeto tem como objetivo desenvolver uma arquitetura de automação de dados para apoiar o monitoramento de indicadores estratégicos da área de AI & Data da Deloitte. A solução busca consolidar informações financeiras e operacionais que atualmente se encontram distribuídas em diferentes fontes, permitindo que esses dados sejam tratados, estruturados e posteriormente utilizados na construção de dashboards analíticos.

A proposta do projeto é centralizar a lógica de processamento de dados em Python, utilizando um pipeline de ETL (Extract, Transform, Load) responsável por realizar a extração das bases, a limpeza e padronização das informações, e a geração de um dataset estruturado que poderá ser utilizado em ferramentas de visualização, como o Power BI.

## Estrutura dos dados utilizados

O projeto trabalha atualmente com duas bases de dados principais:

1. Base operacional  
   Contém registros relacionados à receita associada a projetos e colaboradores. Cada linha representa um lançamento de receita vinculado a um funcionário, projeto e período específico.

   Principais campos:
   - Funcionario
   - Centro de Custo
   - Projeto
   - Area
   - Mês/Ano
   - ID Quinzena
   - Receita Prevista
   - Receita Líquida
   - Allowance
   - Contingência
   - Ajuste
   - Sigla Sub Area

2. Base orçamentária  
   Contém valores agregados de planejamento financeiro por área e tipo de valor ao longo de um horizonte mensal.

   Principais campos:
   - Area
   - Type
   - Colunas de meses (jun/25 até mai/26)

## Pipeline de processamento

Até o momento, foi implementado um pipeline em Python responsável por preparar os dados para análise. Esse pipeline realiza as seguintes etapas:

### 1. Padronização de formatos de entrada

Foi desenvolvido um script responsável por converter arquivos recebidos em diferentes formatos para o formato padrão Excel (.xlsx). Esse passo garante que o pipeline consiga trabalhar com fontes heterogêneas de dados, independentemente do formato original.

### 2. Extração das bases

O pipeline realiza a leitura das duas bases de dados utilizando a biblioteca Pandas.

Durante a execução, o script identifica automaticamente qual arquivo corresponde à base operacional e qual corresponde à base orçamentária, com base nas colunas presentes em cada dataset.

### 3. Limpeza e padronização

Nesta etapa são realizados alguns tratamentos nos dados:

- Remoção de espaços extras nos nomes das colunas
- Padronização dos nomes de colunas (minúsculas e uso de underscores)
- Conversão de colunas numéricas para tipo numérico
- Tratamento de valores inválidos ou ausentes
- Conversão de colunas de data para formato apropriado

Essas etapas são necessárias para garantir consistência na manipulação das informações.

### 4. Transformação da base orçamentária

A base orçamentária originalmente possui uma estrutura com meses representados em colunas.

O pipeline transforma essa estrutura para um formato tabular (long format), no qual cada linha representa um valor associado a uma área, tipo de valor e mês específico. Esse formato facilita análises temporais e integração com ferramentas de BI.

### 5. Validação dos dados

São aplicadas algumas verificações básicas de qualidade, como:

- validação dos valores da coluna de quinzena
- identificação de valores negativos em campos financeiros

Essas verificações ajudam a identificar inconsistências que podem comprometer a análise posterior.

### 6. Criação de métricas

O pipeline também calcula algumas métricas derivadas que podem ser utilizadas diretamente nos dashboards, como:

- Desvio de receita (diferença entre receita líquida e receita prevista)
- Receita ajustada
- Atingimento de meta

### 7. Geração do dataset final

Após as transformações e validações, o dataset resultante é exportado em formato CSV. Esse arquivo será utilizado como fonte de dados para a construção dos dashboards no Power BI.

## Próximos passos

As próximas etapas do projeto incluem:

- aprimorar a validação e qualidade dos dados
- estruturar melhor a integração entre as bases
- construir dashboards analíticos no Power BI
- automatizar a execução do pipeline de dados
- expandir a arquitetura para suportar novas fontes de dados

Essas etapas permitirão consolidar a solução proposta como uma arquitetura de dados reutilizável e escalável para o monitoramento de indicadores estratégicos.