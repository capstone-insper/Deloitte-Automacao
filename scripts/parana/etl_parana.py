"""
ETL Pipeline — Camada Analítica Executiva (Paraná)
===================================================
Lê os arquivos .txt da pasta entrada/, limpa, transforma e gera
11 outputs prontos para consumo em dashboard executivo em output/parana/.

Bases de entrada:
  - BookService.txt   → orçamento (budget): Area × Tipo × Mês
  - data1.csv.txt     → operacional (realizado): transações bi-semanais por
                        funcionário, projeto e centro de custo

Outputs gerados em output/parana/:
  01_base_operacional_limpa.xlsx    — dataset transacional limpo (drill-through)
  02_base_orcamento_longa.xlsx      — orçamento no formato long (referência)
  03_kpis_executivos.xlsx           — painel executivo: totais, %, desvios
  04_serie_temporal_mensal.xlsx     — evolução mensal orçado vs realizado
  05_orcado_vs_realizado_area.xlsx  — comparativo budget × realizado por área
  06_receita_por_projeto.xlsx       — ranking de performance por projeto
  07_receita_por_subarea.xlsx       — análise por sub-área (CO / AI / En)
  08_custos_por_dimensao.xlsx       — breakdown de custos (allowance + contingência)
  09_ranking_desvios_projeto.xlsx   — top desvios por projeto (alertas)
  10_participacao_percentual.xlsx   — composição % por área, projeto e sub-área
  11_orcamento_mensal_completo.xlsx — calendário orçamentário (12 meses completos)
"""

import os
import re
import glob
import unicodedata
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÕES
# ─────────────────────────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parents[2]
PASTA_ENTRADA = ROOT / "entrada"
PASTA_SAIDA = ROOT / "output" / "parana"

ENCODINGS_TENTATIVA = ["utf-16", "utf-16-le", "utf-8-sig", "utf-8", "latin1"]

# Mapeamento de meses em português abreviado → número do mês
MESES_PT = {
    "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
    "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12,
}


# ─────────────────────────────────────────────────────────────────────────────
# 1. LEITURA DE ARQUIVOS
# ─────────────────────────────────────────────────────────────────────────────

def detectar_arquivos_txt(pasta: Path) -> list[Path]:
    """Retorna todos os arquivos .txt dentro da pasta de entrada."""
    arquivos = sorted(pasta.glob("*.txt"))
    print(f"[leitura] {len(arquivos)} arquivo(s) .txt encontrado(s): "
          f"{[a.name for a in arquivos]}")
    return arquivos


def carregar_txt(caminho: Path) -> pd.DataFrame:
    """
    Carrega um arquivo .txt tentando encodings em cascata.
    Detecta automaticamente o separador (tab ou ponto-e-vírgula ou vírgula).
    """
    for enc in ENCODINGS_TENTATIVA:
        try:
            # Tenta ler as primeiras linhas para detectar delimitador
            with open(caminho, encoding=enc, errors="replace") as f:
                amostra = f.read(2048)

            # Conta ocorrências de cada delimitador candidato
            tab_count = amostra.count("\t")
            semi_count = amostra.count(";")
            comma_count = amostra.count(",")

            if tab_count >= semi_count and tab_count >= comma_count:
                sep = "\t"
            elif semi_count >= comma_count:
                sep = ";"
            else:
                sep = ","

            df = pd.read_csv(
                caminho,
                sep=sep,
                encoding=enc,
                on_bad_lines="skip",
                engine="python",
                skip_blank_lines=True,
            )

            # Remove colunas completamente nulas (artefatos de UTF-16 com BOM)
            df = df.dropna(axis=1, how="all")
            df = df.dropna(axis=0, how="all")

            if df.empty or len(df.columns) < 2:
                continue

            print(f"[leitura] '{caminho.name}' carregado com encoding={enc}, "
                  f"sep={repr(sep)}, shape={df.shape}")
            return df

        except Exception:
            continue

    raise RuntimeError(f"Não foi possível carregar '{caminho.name}' com nenhum encoding tentado.")


def identificar_tipo_base(df: pd.DataFrame) -> str:
    """
    Detecta se o DataFrame é:
      - 'orcamento'   → contém coluna 'Type' e meses como colunas
      - 'operacional' → contém coluna 'Funcionario'
    """
    cols_lower = [str(c).strip().lower() for c in df.columns]
    if "type" in cols_lower:
        return "orcamento"
    if "funcionario" in cols_lower:
        return "operacional"
    # Heurística adicional: se houver coluna com padrão mês/ano (ex: jun/25)
    for col in cols_lower:
        if re.match(r"(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)/\d{2}", col):
            return "orcamento"
    return "desconhecido"


# ─────────────────────────────────────────────────────────────────────────────
# 2. PADRONIZAÇÃO E LIMPEZA
# ─────────────────────────────────────────────────────────────────────────────

def remover_acentos(texto: str) -> str:
    """Remove acentos de uma string."""
    return "".join(
        c for c in unicodedata.normalize("NFD", texto)
        if unicodedata.category(c) != "Mn"
    )


def padronizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza os nomes das colunas:
    - strip de espaços
    - lowercase
    - remove acentos
    - substitui espaços e barras por underscore
    - remove caracteres especiais residuais
    """
    novas = []
    for col in df.columns:
        col = str(col).strip()
        col = remover_acentos(col)
        col = col.lower()
        col = re.sub(r"[\s/\\]+", "_", col)
        col = re.sub(r"[^\w]", "", col)
        col = re.sub(r"_+", "_", col).strip("_")
        novas.append(col)
    df.columns = novas
    return df


def limpar_valor_monetario(serie: pd.Series) -> pd.Series:
    """
    Converte série de valores monetários brasileiros para float.
    Trata: 'R$ 1.234,56' → 1234.56
    Também lida com strings vazias, NaN e valores já numéricos.
    """
    if pd.api.types.is_numeric_dtype(serie):
        return serie

    return (
        serie.astype(str)
        .str.strip()
        .str.replace(r"R\$\s*", "", regex=True)
        .str.replace(r"\s+", "", regex=True)
        .str.replace(r"\.", "", regex=True)   # remove separador de milhar
        .str.replace(",", ".", regex=False)   # troca decimal
        .replace({"": np.nan, "nan": np.nan, "None": np.nan})
        .pipe(pd.to_numeric, errors="coerce")
        .fillna(0.0)
    )


# ─────────────────────────────────────────────────────────────────────────────
# 3. LIMPEZA ESPECÍFICA POR BASE
# ─────────────────────────────────────────────────────────────────────────────

def limpar_operacional(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpeza completa da base operacional (data1.csv.txt).

    Colunas esperadas após padronização:
      Ajuste, Allowance, Funcionário, Centro de Custo, Projeto,
      Contingência, Área, Mês/Ano, ID Quinzena,
      Receita Prevista, Receita Líquida, Sigla da Subarea
    """
    print("[limpeza] Processando base operacional...")

    df = padronizar_colunas(df)

    # Colunas monetárias — converter para float
    cols_monetarias = [
        "ajuste", "allowance", "contingencia",
        "receita_prevista", "receita_liquida",
    ]
    for col in cols_monetarias:
        if col in df.columns:
            df[col] = limpar_valor_monetario(df[col])
        else:
            print(f"  [aviso] coluna '{col}' não encontrada — será criada com 0")
            df[col] = 0.0

    # Data — mes_ano
    col_data = next((c for c in df.columns if "mes" in c and "ano" in c), None)
    if col_data:
        df = df.rename(columns={col_data: "mes_ano"})
        df["mes_ano"] = pd.to_datetime(df["mes_ano"], dayfirst=True, errors="coerce")
        linhas_invalidas = df["mes_ano"].isna().sum()
        if linhas_invalidas > 0:
            print(f"  [aviso] {linhas_invalidas} data(s) inválida(s) descartada(s)")
        df = df.dropna(subset=["mes_ano"])
        df["ano"] = df["mes_ano"].dt.year
        df["mes"] = df["mes_ano"].dt.month
        df["ano_mes_str"] = df["mes_ano"].dt.strftime("%Y-%m")
    else:
        print("  [aviso] coluna de data não identificada")

    # id_quinzena — manter apenas 1 e 2
    if "id_quinzena" in df.columns:
        df["id_quinzena"] = pd.to_numeric(df["id_quinzena"], errors="coerce")
        antes = len(df)
        df = df[df["id_quinzena"].isin([1, 2])]
        removidas = antes - len(df)
        if removidas > 0:
            print(f"  [aviso] {removidas} linha(s) com id_quinzena inválido removida(s)")

    # Padronizar texto das dimensões categoricas
    for col in ["funcionario", "area", "sigla_sub_area", "centro_de_custo", "projeto"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

    # Renomear colunas para nomes mais legíveis
    df = df.rename(columns={
        "ajuste": "Ajuste",
        "allowance": "Allowance",
        "funcionario": "Funcionário",
        "centro_de_custo": "Centro de Custo",
        "projeto": "Projeto",
        "contingencia": "Contingência",
        "area": "Área",
        "mes_ano": "Mês/Ano",
        "id_quinzena": "ID Quinzena",
        "receita_prevista": "Receita Prevista",
        "receita_liquida": "Receita Líquida",
        "sigla_sub_area": "Sigla da Subarea",
        "ano": "Ano",
        "mes": "Mês",
        "ano_mes_str": "Ano-Mês",
        "custo_total": "Custo Total",
        "desvio_receita": "Desvio Receita",
        "desvio_pct": "Desvio %",
        "atingimento_pct": "Atingimento %",
        "receita_ajustada": "Receita Ajustada",
    })

    print(f"  [ok] base operacional limpa: {df.shape[0]} linhas × {df.shape[1]} colunas")
    return df.reset_index(drop=True)


def limpar_orcamento(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpeza + transformação da base de orçamento (BookService.txt).
    Converte de formato wide (meses como colunas) para long.

    Resultado:
      area, tipo, mes_ref (ex: 'jun/25'), mes_ano (datetime), ano, mes, valor
    """
    print("[limpeza] Processando base de orçamento...")

    df = padronizar_colunas(df)

    # Identificar colunas de meses (padrão após padronização: mmm_aa ou mmmaa)
    colunas_meses = [
        c for c in df.columns
        if re.match(r"(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)_?\d{2}$", c)
    ]

    if not colunas_meses:
        print("  [aviso] nenhuma coluna de mês identificada no orçamento")
        return df

    # Colunas de identificação (tudo que não é mês)
    id_cols = [c for c in df.columns if c not in colunas_meses]

    # Converter valores monetários nas colunas de mês
    for col in colunas_meses:
        df[col] = limpar_valor_monetario(df[col])

    # Melt: wide → long
    df_long = df.melt(
        id_vars=id_cols,
        value_vars=colunas_meses,
        var_name="mes_ref",
        value_name="valor",
    )

    # Renomear colunas de identificação para padrão esperado
    rename_map = {}
    for col in id_cols:
        if col == "area":
            rename_map[col] = "area"
        elif col in ("type", "tipo", "type_"):
            rename_map[col] = "tipo"
    df_long = df_long.rename(columns=rename_map)

    # Parsear mes_ref (ex: 'jun25') → datetime
    def parsear_mes_ref(s: str) -> pd.Timestamp:
        # Aceita formatos: jun25, jun_25
        match = re.match(r"([a-z]+)_?(\d{2})", s.lower())
        if not match:
            return pd.NaT
        nome_mes, ano_2d = match.groups()
        num_mes = MESES_PT.get(nome_mes)
        if not num_mes:
            return pd.NaT
        ano = 2000 + int(ano_2d)
        return pd.Timestamp(year=ano, month=num_mes, day=1)

    df_long["mes_ano"] = df_long["mes_ref"].apply(parsear_mes_ref)
    df_long["ano"] = df_long["mes_ano"].dt.year
    df_long["mes"] = df_long["mes_ano"].dt.month
    df_long["ano_mes_str"] = df_long["mes_ano"].dt.strftime("%Y-%m")

    # Padronizar texto
    if "area" in df_long.columns:
        df_long["area"] = df_long["area"].astype(str).str.strip().str.upper()
    if "tipo" in df_long.columns:
        df_long["tipo"] = df_long["tipo"].astype(str).str.strip().str.capitalize()

    df_long = df_long.dropna(subset=["mes_ano"])
    df_long = df_long.sort_values(["area", "mes_ano", "tipo"]).reset_index(drop=True)

    print(f"  [ok] base orçamento longa: {df_long.shape[0]} linhas × {df_long.shape[1]} colunas")
    return df_long


# ─────────────────────────────────────────────────────────────────────────────
# 4. MÉTRICAS DERIVADAS
# ─────────────────────────────────────────────────────────────────────────────

def calcular_metricas_operacional(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona colunas de métricas analíticas à base operacional:
      - Custo Total         = Allowance + Contingência
      - Desvio Receita      = Receita Líquida − Receita Prevista
      - Desvio %            = Desvio Receita / Receita Prevista × 100
      - Atingimento %       = Receita Líquida / Receita Prevista × 100
      - Receita Ajustada    = Receita Líquida − Allowance − Contingência
    """
    print("[metricas] Calculando métricas derivadas...")

    df["Custo Total"] = df["Allowance"] + df["Contingência"]
    df["Desvio Receita"] = df["Receita Líquida"] - df["Receita Prevista"]

    df["Desvio %"] = np.where(
        df["Receita Prevista"] != 0,
        df["Desvio Receita"] / df["Receita Prevista"] * 100,
        np.nan,
    )

    df["Atingimento %"] = np.where(
        df["Receita Prevista"] != 0,
        df["Receita Líquida"] / df["Receita Prevista"] * 100,
        np.nan,
    )

    df["Receita Ajustada"] = df["Receita Líquida"] - df["Allowance"] - df["Contingência"]

    return df


def _extrair_orcado_receita(df_orc: pd.DataFrame) -> pd.DataFrame:
    """Filtra apenas a linha de Receita do orçamento e retorna (area, mes_ano, orcado_receita)."""
    mask = df_orc["tipo"].str.lower().str.contains("receita", na=False)
    return (
        df_orc[mask]
        .groupby(["area", "mes_ano", "ano_mes_str"], as_index=False)["valor"]
        .sum()
        .rename(columns={"valor": "orcado_receita"})
    )


def _extrair_orcado_custo(df_orc: pd.DataFrame) -> pd.DataFrame:
    """Filtra apenas a linha de Custo do orçamento."""
    mask = df_orc["tipo"].str.lower().str.contains("custo", na=False)
    return (
        df_orc[mask]
        .groupby(["area", "mes_ano", "ano_mes_str"], as_index=False)["valor"]
        .sum()
        .rename(columns={"valor": "orcado_custo"})
    )


def _extrair_orcado_margem(df_orc: pd.DataFrame) -> pd.DataFrame:
    """Filtra apenas a linha de Margin do orçamento."""
    mask = df_orc["tipo"].str.lower().str.contains("margin|margem", na=False)
    return (
        df_orc[mask]
        .groupby(["area", "mes_ano", "ano_mes_str"], as_index=False)["valor"]
        .sum()
        .rename(columns={"valor": "orcado_margem"})
    )


# ─────────────────────────────────────────────────────────────────────────────
# 5. TABELAS ANALÍTICAS
# ─────────────────────────────────────────────────────────────────────────────

def criar_serie_temporal(df_op: pd.DataFrame, df_orc: pd.DataFrame) -> pd.DataFrame:
    """
    OUTPUT 04 — Série temporal mensal: área × mês com orçado e realizado.
    Granularidade: 1 linha = área × mês.
    Uso: gráfico de linha / área empilhada no dashboard.
    """
    print("[tabelas] Criando série temporal mensal...")

    # Agregação operacional por área × mês
    op_mensal = (
        df_op.groupby(["Área", "Mês/Ano", "Ano-Mês"], as_index=False)
        .agg(
            receita_prevista_op=("Receita Prevista", "sum"),
            receita_realizada=("Receita Líquida", "sum"),
            custo_realizado=("Custo Total", "sum"),
            desvio_receita=("Desvio Receita", "sum"),
            n_registros=("Receita Líquida", "count"),
        )
    )

    # Orçado por área × mês
    orc_receita = _extrair_orcado_receita(df_orc).rename(columns={"area": "Área", "mes_ano": "Mês/Ano", "ano_mes_str": "Ano-Mês"})
    orc_custo = _extrair_orcado_custo(df_orc).rename(columns={"area": "Área", "mes_ano": "Mês/Ano", "ano_mes_str": "Ano-Mês"})
    orc_margem = _extrair_orcado_margem(df_orc).rename(columns={"area": "Área", "mes_ano": "Mês/Ano", "ano_mes_str": "Ano-Mês"})

    # Join orçado com realizado (left join a partir do orçamento para exibir todos os meses)
    serie = orc_receita.merge(orc_custo, on=["Área", "Mês/Ano", "Ano-Mês"], how="outer")
    serie = serie.merge(orc_margem, on=["Área", "Mês/Ano", "Ano-Mês"], how="outer")
    serie = serie.merge(op_mensal, on=["Área", "Mês/Ano", "Ano-Mês"], how="left")

    # Colunas operacionais ficam 0 nos meses sem dados realizados
    for col in ["receita_realizada", "receita_prevista_op", "custo_realizado", "desvio_receita", "n_registros"]:
        serie[col] = serie[col].fillna(0)

    serie["desvio_vs_orcado"] = serie["receita_realizada"] - serie["orcado_receita"]
    serie["desvio_vs_previsto"] = serie["receita_realizada"] - serie["receita_prevista_op"]
    serie["taxa_execucao_pct"] = np.where(
        serie["orcado_receita"] != 0,
        serie["receita_realizada"] / serie["orcado_receita"] * 100,
        np.nan,
    )

    serie = serie.sort_values(["Área", "Mês/Ano"]).reset_index(drop=True)
    return serie


def criar_kpis_executivos(df_op: pd.DataFrame, df_orc: pd.DataFrame) -> pd.DataFrame:
    """
    OUTPUT 03 — Painel executivo com KPIs consolidados por área + linha TOTAL.
    Granularidade: 1 linha por área (+ TOTAL).
    Uso: cards KPI, scorecard executivo.
    """
    print("[tabelas] Criando KPIs executivos...")

    # Totais operacionais por área
    op_area = (
        df_op.groupby("Área", as_index=False)
        .agg(
            receita_prevista_total=("Receita Prevista", "sum"),
            receita_realizada_total=("Receita Líquida", "sum"),
            custo_total_realizado=("Custo Total", "sum"),
            n_registros=("Receita Líquida", "count"),
            atingimento_medio_pct=("Atingimento %", "mean"),
        )
    )

    # Orçado total por área (soma todos os meses com dados realizados)
    # Consideramos os meses que têm dados operacionais para comparação justa
    meses_com_dados = df_op["Mês/Ano"].unique()
    orc_filtrado = df_orc[df_orc["mes_ano"].isin(meses_com_dados)]
    orc_receita_area = (
        _extrair_orcado_receita(orc_filtrado)
        .groupby("area", as_index=False)["orcado_receita"]
        .sum()
        .rename(columns={"area": "Área"})
    )

    kpis = op_area.merge(orc_receita_area, on="Área", how="outer")
    kpis["orcado_receita"] = kpis["orcado_receita"].fillna(0)
    kpis["desvio_absoluto"] = kpis["receita_realizada_total"] - kpis["orcado_receita"]
    kpis["desvio_pct"] = np.where(
        kpis["orcado_receita"] != 0,
        kpis["desvio_absoluto"] / kpis["orcado_receita"] * 100,
        np.nan,
    )
    kpis["taxa_execucao_pct"] = np.where(
        kpis["orcado_receita"] != 0,
        kpis["receita_realizada_total"] / kpis["orcado_receita"] * 100,
        np.nan,
    )
    kpis["margem_estimada"] = kpis["receita_realizada_total"] - kpis["custo_total_realizado"]
    kpis["margem_pct"] = np.where(
        kpis["receita_realizada_total"] != 0,
        kpis["margem_estimada"] / kpis["receita_realizada_total"] * 100,
        np.nan,
    )

    # Linha TOTAL
    total = pd.DataFrame([{
        "Área": "TOTAL",
        "receita_prevista_total": kpis["receita_prevista_total"].sum(),
        "receita_realizada_total": kpis["receita_realizada_total"].sum(),
        "custo_total_realizado": kpis["custo_total_realizado"].sum(),
        "n_registros": kpis["n_registros"].sum(),
        "atingimento_medio_pct": kpis["atingimento_medio_pct"].mean(),
        "orcado_receita": kpis["orcado_receita"].sum(),
        "desvio_absoluto": kpis["desvio_absoluto"].sum(),
        "desvio_pct": np.nan,
        "taxa_execucao_pct": np.nan,
        "margem_estimada": kpis["margem_estimada"].sum(),
        "margem_pct": np.nan,
    }])

    # Recalcular % para o total
    if total["orcado_receita"].iloc[0] != 0:
        total["desvio_pct"] = total["desvio_absoluto"] / total["orcado_receita"] * 100
        total["taxa_execucao_pct"] = total["receita_realizada_total"] / total["orcado_receita"] * 100
    if total["receita_realizada_total"].iloc[0] != 0:
        total["margem_pct"] = total["margem_estimada"] / total["receita_realizada_total"] * 100

    kpis = pd.concat([kpis, total], ignore_index=True)
    return kpis


def criar_orcado_vs_realizado_area(df_op: pd.DataFrame, df_orc: pd.DataFrame) -> pd.DataFrame:
    """
    OUTPUT 05 — Comparativo orçado × realizado por área (totais históricos dos meses com dados).
    Uso: gráfico de barras agrupadas.
    """
    print("[tabelas] Criando orçado vs realizado por área...")

    meses_com_dados = df_op["Mês/Ano"].unique()
    orc_filtrado = df_orc[df_orc["mes_ano"].isin(meses_com_dados)]

    orc = _extrair_orcado_receita(orc_filtrado).groupby("area", as_index=False)["orcado_receita"].sum().rename(columns={"area": "Área"})
    real = df_op.groupby("Área", as_index=False).agg(
        receita_realizada=("Receita Líquida", "sum"),
        receita_prevista_op=("Receita Prevista", "sum"),
    )

    tab = orc.merge(real, on="Área", how="outer").fillna(0)
    tab["desvio_absoluto"] = tab["receita_realizada"] - tab["orcado_receita"]
    tab["desvio_pct"] = np.where(
        tab["orcado_receita"] != 0,
        tab["desvio_absoluto"] / tab["orcado_receita"] * 100,
        np.nan,
    )
    tab["taxa_execucao_pct"] = np.where(
        tab["orcado_receita"] != 0,
        tab["receita_realizada"] / tab["orcado_receita"] * 100,
        np.nan,
    )
    return tab.sort_values("Área").reset_index(drop=True)


def criar_receita_por_projeto(df_op: pd.DataFrame) -> pd.DataFrame:
    """
    OUTPUT 06 — Performance de receita por projeto.
    Uso: ranking de projetos, gráfico de barras.
    """
    print("[tabelas] Criando análise por projeto...")

    tab = (
        df_op.groupby(["Projeto", "Área", "Sigla da Subarea"] if "Sigla da Subarea" in df_op.columns else ["Projeto", "Área"], as_index=False)
        .agg(
            receita_prevista_sum=("Receita Prevista", "sum"),
            receita_realizada_sum=("Receita Líquida", "sum"),
            desvio_sum=("Desvio Receita", "sum"),
            custo_total_sum=("Custo Total", "sum"),
            atingimento_medio_pct=("Atingimento %", "mean"),
            n_quinzenas=("ID Quinzena", "count"),
        )
    )

    tab["desvio_pct"] = np.where(
        tab["receita_prevista_sum"] != 0,
        tab["desvio_sum"] / tab["receita_prevista_sum"] * 100,
        np.nan,
    )
    tab["receita_ajustada_sum"] = tab["receita_realizada_sum"] - tab["custo_total_sum"]

    tab = tab.sort_values("receita_realizada_sum", ascending=False).reset_index(drop=True)
    tab["ranking"] = tab.index + 1
    return tab


def criar_receita_por_subarea(df_op: pd.DataFrame) -> pd.DataFrame:
    """
    OUTPUT 07 — Análise por sub-área (CO / AI / En).
    Uso: gráfico de participação, barras comparativas.
    """
    print("[tabelas] Criando análise por sub-área...")

    if "Sigla da Subarea" not in df_op.columns:
        print("  [aviso] coluna Sigla da Subarea não encontrada — output 07 vazio")
        return pd.DataFrame()

    tab = (
        df_op.groupby("Sigla da Subarea", as_index=False)
        .agg(
            receita_prevista_sum=("Receita Prevista", "sum"),
            receita_realizada_sum=("Receita Líquida", "sum"),
            desvio_sum=("Desvio Receita", "sum"),
            custo_total_sum=("Custo Total", "sum"),
            atingimento_medio_pct=("Atingimento %", "mean"),
            n_registros=("Receita Líquida", "count"),
        )
    )

    total_realizado = tab["receita_realizada_sum"].sum()
    tab["participacao_pct"] = np.where(
        total_realizado != 0,
        tab["receita_realizada_sum"] / total_realizado * 100,
        np.nan,
    )
    tab["desvio_pct"] = np.where(
        tab["receita_prevista_sum"] != 0,
        tab["desvio_sum"] / tab["receita_prevista_sum"] * 100,
        np.nan,
    )
    return tab.sort_values("receita_realizada_sum", ascending=False).reset_index(drop=True)


def criar_custos_por_dimensao(df_op: pd.DataFrame) -> pd.DataFrame:
    """
    OUTPUT 08 — Breakdown de custos (allowance + contingência) por centro de custo × tipo funcionário.
    Uso: análise de custo, treemap / waterfall.
    """
    print("[tabelas] Criando breakdown de custos...")

    dims = [c for c in ["centro_de_custo", "funcionario"] if c in df_op.columns]
    if not dims:
        return pd.DataFrame()

    tab = (
        df_op.groupby(dims, as_index=False)
        .agg(
            allowance_sum=("allowance", "sum"),
            contingencia_sum=("contingencia", "sum"),
            custo_total_sum=("custo_total", "sum"),
            n_registros=("custo_total", "count"),
        )
    )

    custo_global = tab["custo_total_sum"].sum()
    tab["participacao_custo_pct"] = np.where(
        custo_global != 0,
        tab["custo_total_sum"] / custo_global * 100,
        np.nan,
    )

    return tab.sort_values("custo_total_sum", ascending=False).reset_index(drop=True)


def criar_ranking_desvios(df_op: pd.DataFrame) -> pd.DataFrame:
    """
    OUTPUT 09 — Ranking de projetos com maiores desvios absolutos (alertas).
    Uso: tabela de alertas no dashboard.
    """
    print("[tabelas] Criando ranking de desvios por projeto...")

    dims = [c for c in ["Projeto", "Área", "Sigla da Subarea"] if c in df_op.columns]

    tab = (
        df_op.groupby(dims, as_index=False)
        .agg(
            desvio_absoluto=("desvio_receita", "sum"),
            receita_realizada=("receita_liquida", "sum"),
            receita_prevista=("receita_prevista", "sum"),
        )
    )

    tab["desvio_pct"] = np.where(
        tab["receita_prevista"] != 0,
        tab["desvio_absoluto"] / tab["receita_prevista"] * 100,
        np.nan,
    )
    tab["desvio_absoluto_magnitude"] = tab["desvio_absoluto"].abs()
    tab = tab.sort_values("desvio_absoluto_magnitude", ascending=False).reset_index(drop=True)
    tab["ranking"] = tab.index + 1
    tab = tab.drop(columns=["desvio_absoluto_magnitude"])
    return tab


def criar_participacao_percentual(df_op: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    OUTPUT 10 — Participação % por área, projeto e sub-área (3 tabelas / abas).
    Uso: gráfico de pizza/donut e análise de concentração (Pareto).
    Retorna dict com chaves 'por_area', 'por_projeto', 'por_subarea'.
    """
    print("[tabelas] Criando tabelas de participação percentual...")

    def _tabela_pareto(df_grp: pd.DataFrame, dim: str) -> pd.DataFrame:
        total = df_grp["receita_realizada"].sum()
        df_grp = df_grp.sort_values("receita_realizada", ascending=False).reset_index(drop=True)
        df_grp["participacao_pct"] = df_grp["receita_realizada"] / total * 100 if total else np.nan
        df_grp["participacao_acumulada_pct"] = df_grp["participacao_pct"].cumsum()
        df_grp = df_grp.rename(columns={"receita_realizada": "valor", dim: "dimensao"})
        return df_grp[["dimensao", "valor", "participacao_pct", "participacao_acumulada_pct"]]

    por_area = _tabela_pareto(
        df_op.groupby("Área", as_index=False).agg(receita_realizada=("Receita Líquida", "sum")),
        "Área",
    )

    por_projeto = _tabela_pareto(
        df_op.groupby("Projeto", as_index=False).agg(receita_realizada=("Receita Líquida", "sum")),
        "Projeto",
    )

    if "Sigla da Subarea" in df_op.columns:
        por_subarea = _tabela_pareto(
            df_op.groupby("Sigla da Subarea", as_index=False).agg(receita_realizada=("Receita Líquida", "sum")),
            "Sigla da Subarea",
        )
    else:
        por_subarea = pd.DataFrame()

    return {"por_area": por_area, "por_projeto": por_projeto, "por_subarea": por_subarea}


def criar_orcamento_mensal_completo(df_orc: pd.DataFrame) -> pd.DataFrame:
    """
    OUTPUT 11 — Calendário orçamentário completo (todos os 12 meses).
    Granularidade: área × mês com Receita + Custo + Margem.
    Uso: tabela de referência, baseline estratégico.
    """
    print("[tabelas] Criando orçamento mensal completo...")

    orc_r = _extrair_orcado_receita(df_orc).rename(columns={"orcado_receita": "receita_orcada"})
    orc_c = _extrair_orcado_custo(df_orc).rename(columns={"orcado_custo": "custo_orcado"})
    orc_m = _extrair_orcado_margem(df_orc).rename(columns={"orcado_margem": "margem_orcada"})

    tab = orc_r.merge(orc_c, on=["area", "mes_ano", "ano_mes_str"], how="outer")
    tab = tab.merge(orc_m, on=["area", "mes_ano", "ano_mes_str"], how="outer")
    tab = tab.fillna(0)

    # Se margem não estiver no orçamento, calcular
    if tab["margem_orcada"].sum() == 0:
        tab["margem_orcada"] = tab["receita_orcada"] - tab["custo_orcado"]

    tab["margem_pct"] = np.where(
        tab["receita_orcada"] != 0,
        tab["margem_orcada"] / tab["receita_orcada"] * 100,
        np.nan,
    )

    tab["mes_nome"] = tab["mes_ano"].dt.strftime("%b/%Y")
    tab = tab.sort_values(["area", "mes_ano"]).reset_index(drop=True)
    return tab


def criar_janelas_temporais(df_op: pd.DataFrame, df_orc: pd.DataFrame) -> pd.DataFrame:
    """
    OUTPUT 12 — KPIs comparativos por janela temporal: M, M-1, 3M, 6M, 12M e YTD.
    Granularidade: 1 linha = janela × área (+ TOTAL por janela).
    Uso: tabela de comparação temporal no dashboard executivo.

    A data de referência é o último mês com dados operacionais presentes na base.
    Janelas sem nenhum dado operacional são omitidas do resultado.
    """
    print("[tabelas] Criando comparativo por janelas temporais...")

    data_ref = df_op["mes_ano"].max()
    ano_ref = data_ref.year

    janelas = {
        "M":   (data_ref, data_ref),
        "M-1": (data_ref - pd.DateOffset(months=1), data_ref - pd.DateOffset(months=1)),
        "3M":  (data_ref - pd.DateOffset(months=2), data_ref),
        "6M":  (data_ref - pd.DateOffset(months=5), data_ref),
        "12M": (data_ref - pd.DateOffset(months=11), data_ref),
        "YTD": (pd.Timestamp(year=ano_ref, month=1, day=1), data_ref),
    }

    blocos = []
    for janela, (dt_inicio, dt_fim) in janelas.items():
        df_j = df_op[
            (df_op["mes_ano"] >= dt_inicio) & (df_op["mes_ano"] <= dt_fim)
        ]

        if df_j.empty:
            print(f"  [aviso] janela {janela}: sem dados operacionais — omitida")
            continue

        orc_j = df_orc[
            (df_orc["mes_ano"] >= dt_inicio) & (df_orc["mes_ano"] <= dt_fim)
        ]
        orc_receita = (
            _extrair_orcado_receita(orc_j)
            .groupby("area", as_index=False)["orcado_receita"]
            .sum()
            .rename(columns={"area": "Área"})
        )

        # Agrega por área
        op_area = df_j.groupby("Área", as_index=False).agg(
            receita_realizada=("Receita Líquida", "sum"),
            receita_prevista=("Receita Prevista", "sum"),
            custo_total=("Custo Total", "sum"),
            n_registros=("Receita Líquida", "count"),
        )

        tab = op_area.merge(orc_receita, on="Área", how="left")
        tab["orcado_receita"] = tab["orcado_receita"].fillna(0)

        tab["desvio_vs_orcado"] = tab["receita_realizada"] - tab["orcado_receita"]
        tab["taxa_execucao_pct"] = np.where(
            tab["orcado_receita"] != 0,
            tab["receita_realizada"] / tab["orcado_receita"] * 100,
            np.nan,
        )
        tab["desvio_vs_previsto"] = tab["receita_realizada"] - tab["receita_prevista"]
        tab["atingimento_pct"] = np.where(
            tab["receita_prevista"] != 0,
            tab["receita_realizada"] / tab["receita_prevista"] * 100,
            np.nan,
        )
        tab["margem_estimada"] = tab["receita_realizada"] - tab["custo_total"]
        tab["margem_pct"] = np.where(
            tab["receita_realizada"] != 0,
            tab["margem_estimada"] / tab["receita_realizada"] * 100,
            np.nan,
        )
        tab["janela"] = janela
        tab["data_inicio"] = dt_inicio.strftime("%Y-%m")
        tab["data_fim"] = dt_fim.strftime("%Y-%m")

        # Linha TOTAL da janela
        tot_real = tab["receita_realizada"].sum()
        tot_orc = tab["orcado_receita"].sum()
        tot_prev = tab["receita_prevista"].sum()
        tot_custo = tab["custo_total"].sum()
        tot_marg = tot_real - tot_custo

        total_row = {
            "area": "TOTAL",
            "receita_realizada": tot_real,
            "receita_prevista": tot_prev,
            "custo_total": tot_custo,
            "n_registros": tab["n_registros"].sum(),
            "orcado_receita": tot_orc,
            "desvio_vs_orcado": tot_real - tot_orc,
            "taxa_execucao_pct": tot_real / tot_orc * 100 if tot_orc else np.nan,
            "desvio_vs_previsto": tot_real - tot_prev,
            "atingimento_pct": tot_real / tot_prev * 100 if tot_prev else np.nan,
            "margem_estimada": tot_marg,
            "margem_pct": tot_marg / tot_real * 100 if tot_real else np.nan,
            "janela": janela,
            "data_inicio": dt_inicio.strftime("%Y-%m"),
            "data_fim": dt_fim.strftime("%Y-%m"),
        }
        tab = pd.concat([tab, pd.DataFrame([total_row])], ignore_index=True)
        blocos.append(tab)

    if not blocos:
        return pd.DataFrame()

    result = pd.concat(blocos, ignore_index=True)

    # Ordenar janelas na sequência lógica
    ordem_janela = ["M", "M-1", "3M", "6M", "12M", "YTD"]
    result["_ord"] = result["janela"].map({j: i for i, j in enumerate(ordem_janela)})
    result = result.sort_values(["_ord", "area"]).drop(columns=["_ord"]).reset_index(drop=True)

    cols = [
        "janela", "data_inicio", "data_fim", "area",
        "orcado_receita", "receita_prevista", "receita_realizada",
        "desvio_vs_orcado", "taxa_execucao_pct",
        "desvio_vs_previsto", "atingimento_pct",
        "custo_total", "margem_estimada", "margem_pct",
        "n_registros",
    ]
    result = result[[c for c in cols if c in result.columns]]

    print(f"  [ok] janelas geradas: {result['janela'].unique().tolist()}")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# 6. EXPORTAÇÃO
# ─────────────────────────────────────────────────────────────────────────────

def exportar_xlsx(df: pd.DataFrame, caminho: Path, nome_aba: str = "Dados") -> None:
    """Exporta um DataFrame para .xlsx com formatação básica."""
    with pd.ExcelWriter(caminho, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=nome_aba, index=False)


def exportar_xlsx_multiplas_abas(dfs: dict[str, pd.DataFrame], caminho: Path) -> None:
    """Exporta múltiplos DataFrames em abas de um único .xlsx."""
    with pd.ExcelWriter(caminho, engine="openpyxl") as writer:
        for nome_aba, df in dfs.items():
            if not df.empty:
                df.to_excel(writer, sheet_name=nome_aba[:31], index=False)


def exportar_todos_outputs(
    df_op_limpa: pd.DataFrame,
    df_orc_longa: pd.DataFrame,
    pasta: Path,
) -> None:
    """
    Orquestra a geração e exportação de todos os 11 outputs analíticos.
    """
    pasta.mkdir(parents=True, exist_ok=True)
    print(f"\n[exportacao] Salvando outputs em: {pasta}\n")

    # 01 — Base operacional limpa
    p = pasta / "01_base_operacional_limpa.xlsx"
    exportar_xlsx(df_op_limpa, p, "Base Operacional")
    df_op_limpa.to_csv(pasta / "01_base_operacional_limpa.csv", index=False, encoding="utf-8-sig")
    print(f"  [ok] 01_base_operacional_limpa.xlsx + .csv")

    # 02 — Base orçamento longa
    p = pasta / "02_base_orcamento_longa.xlsx"
    exportar_xlsx(df_orc_longa, p, "Orçamento Long")
    print(f"  [ok] 02_base_orcamento_longa.xlsx")

    # 03 — KPIs executivos
    kpis = criar_kpis_executivos(df_op_limpa, df_orc_longa)
    exportar_xlsx(kpis, pasta / "03_kpis_executivos.xlsx", "KPIs Executivos")
    print(f"  [ok] 03_kpis_executivos.xlsx")

    # 04 — Série temporal mensal
    serie = criar_serie_temporal(df_op_limpa, df_orc_longa)
    exportar_xlsx(serie, pasta / "04_serie_temporal_mensal.xlsx", "Série Temporal")
    print(f"  [ok] 04_serie_temporal_mensal.xlsx")

    # 05 — Orçado vs Realizado por área
    orc_real = criar_orcado_vs_realizado_area(df_op_limpa, df_orc_longa)
    exportar_xlsx(orc_real, pasta / "05_orcado_vs_realizado_area.xlsx", "Orc vs Real Área")
    print(f"  [ok] 05_orcado_vs_realizado_area.xlsx")

    # 06 — Receita por projeto
    proj = criar_receita_por_projeto(df_op_limpa)
    exportar_xlsx(proj, pasta / "06_receita_por_projeto.xlsx", "Por Projeto")
    print(f"  [ok] 06_receita_por_projeto.xlsx")

    # 07 — Receita por sub-área
    subarea = criar_receita_por_subarea(df_op_limpa)
    if not subarea.empty:
        exportar_xlsx(subarea, pasta / "07_receita_por_subarea.xlsx", "Por Sub-Área")
    print(f"  [ok] 07_receita_por_subarea.xlsx")

    # 08 — Custos por dimensão
    custos = criar_custos_por_dimensao(df_op_limpa)
    if not custos.empty:
        exportar_xlsx(custos, pasta / "08_custos_por_dimensao.xlsx", "Custos Dimensão")
    print(f"  [ok] 08_custos_por_dimensao.xlsx")

    # 09 — Ranking de desvios por projeto
    desvios = criar_ranking_desvios(df_op_limpa)
    exportar_xlsx(desvios, pasta / "09_ranking_desvios_projeto.xlsx", "Ranking Desvios")
    print(f"  [ok] 09_ranking_desvios_projeto.xlsx")

    # 10 — Participação percentual (múltiplas abas)
    participacao = criar_participacao_percentual(df_op_limpa)
    exportar_xlsx_multiplas_abas(
        {
            "Por Área": participacao["por_area"],
            "Por Projeto": participacao["por_projeto"],
            "Por Sub-Área": participacao["por_subarea"],
        },
        pasta / "10_participacao_percentual.xlsx",
    )
    print(f"  [ok] 10_participacao_percentual.xlsx")

    # 11 — Orçamento mensal completo
    orc_completo = criar_orcamento_mensal_completo(df_orc_longa)
    exportar_xlsx(orc_completo, pasta / "11_orcamento_mensal_completo.xlsx", "Orçamento Mensal")
    print(f"  [ok] 11_orcamento_mensal_completo.xlsx")

    # 12 — Comparativo por janelas temporais (M, M-1, 3M, 6M, 12M, YTD)
    janelas = criar_janelas_temporais(df_op_limpa, df_orc_longa)
    if not janelas.empty:
        exportar_xlsx(janelas, pasta / "12_janelas_temporais.xlsx", "Janelas Temporais")
    print(f"  [ok] 12_janelas_temporais.xlsx")

    print(f"\n[exportacao] {12} outputs gerados com sucesso.")


# ─────────────────────────────────────────────────────────────────────────────
# 7. ORQUESTRAÇÃO PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def run_pipeline():
    """Ponto de entrada: executa todo o pipeline ETL do início ao fim."""
    print("=" * 60)
    print("  ETL ANALÍTICO — PARANÁ")
    print("=" * 60)

    # 1. Detectar arquivos
    arquivos = detectar_arquivos_txt(PASTA_ENTRADA)
    if not arquivos:
        raise FileNotFoundError(f"Nenhum arquivo .txt encontrado em: {PASTA_ENTRADA}")

    # 2. Carregar e classificar cada arquivo
    df_operacional = None
    df_orcamento = None

    for arq in arquivos:
        df_raw = carregar_txt(arq)
        tipo = identificar_tipo_base(df_raw)
        print(f"  -> '{arq.name}' identificado como: {tipo}")

        if tipo == "operacional":
            df_operacional = df_raw
        elif tipo == "orcamento":
            df_orcamento = df_raw
        else:
            print(f"  [aviso] '{arq.name}' não reconhecido — ignorado")

    if df_operacional is None:
        raise ValueError("Nenhuma base operacional identificada nos arquivos .txt.")
    if df_orcamento is None:
        raise ValueError("Nenhuma base de orçamento identificada nos arquivos .txt.")

    # 3. Limpar bases
    print()
    df_op_limpa = limpar_operacional(df_operacional)
    df_orc_longa = limpar_orcamento(df_orcamento)

    # 4. Calcular métricas derivadas
    print()
    df_op_limpa = calcular_metricas_operacional(df_op_limpa)

    # 5. Gerar e exportar todos os outputs
    exportar_todos_outputs(df_op_limpa, df_orc_longa, PASTA_SAIDA)

    print("\n" + "=" * 60)
    print("  PIPELINE CONCLUÍDO COM SUCESSO")
    print(f"  Outputs em: {PASTA_SAIDA}")
    print("=" * 60)


if __name__ == "__main__":
    run_pipeline()
