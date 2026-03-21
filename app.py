"""
Dashboard Executivo — Paraná
============================
Visualização dos outputs gerados pelo ETL pipeline (etl_parana.py).

Para rodar:
    python -m streamlit run app.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÃO DA PÁGINA
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Dashboard Executivo — Deloitte",
    layout="wide",
)

ROOT = Path(__file__).resolve().parent
PASTA_SAIDA = ROOT / "output" / "parana"

# ─────────────────────────────────────────────────────────────────────────────
# CARREGAMENTO DA BASE PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data
def carregar_base() -> pd.DataFrame:
    caminho_csv = PASTA_SAIDA / "01_base_operacional_limpa.csv"
    caminho_xlsx = PASTA_SAIDA / "01_base_operacional_limpa.xlsx"

    if caminho_csv.exists():
        df = pd.read_csv(caminho_csv)
    elif caminho_xlsx.exists():
        df = pd.read_excel(caminho_xlsx)
    else:
        return pd.DataFrame()

    if "mes_ano" in df.columns:
        df["mes_ano"] = pd.to_datetime(df["mes_ano"], errors="coerce")
    return df


df_base = carregar_base()

# ─────────────────────────────────────────────────────────────────────────────
# PERSISTÊNCIA DOS FILTROS NA URL (sobrevive ao F5)
# ─────────────────────────────────────────────────────────────────────────────

params = st.query_params

areas_disponiveis = sorted(df_base["area"].dropna().unique().tolist()) if "area" in df_base.columns else []
projetos_disponiveis = sorted(df_base["projeto"].dropna().unique().tolist()) if "projeto" in df_base.columns else []

# Lê filtros salvos na URL ou usa todos como padrão
areas_default = params.get_all("area") or areas_disponiveis
projetos_default = params.get_all("projeto") or projetos_disponiveis

# Garante que os valores da URL ainda existem nos dados
areas_default = [a for a in areas_default if a in areas_disponiveis] or areas_disponiveis
projetos_default = [p for p in projetos_default if p in projetos_disponiveis] or projetos_disponiveis

# Data
if "mes_ano" in df_base.columns and not df_base["mes_ano"].isna().all():
    min_data = df_base["mes_ano"].min().date()
    max_data = df_base["mes_ano"].max().date()
    try:
        data_ini = pd.to_datetime(params.get("data_ini")).date() if "data_ini" in params else min_data
        data_fim = pd.to_datetime(params.get("data_fim")).date() if "data_fim" in params else max_data
    except Exception:
        data_ini, data_fim = min_data, max_data
else:
    min_data = max_data = data_ini = data_fim = None

# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR — FILTROS GLOBAIS
# ─────────────────────────────────────────────────────────────────────────────

st.sidebar.title("Filtros")

if st.sidebar.button("Resetar filtros"):
    params.clear()
    st.session_state["area_sel"] = areas_disponiveis
    st.session_state["projeto_sel"] = projetos_disponiveis
    if min_data and max_data:
        st.session_state["periodo_sel"] = (min_data, max_data)
    st.rerun()

area_sel = st.sidebar.multiselect("Area", areas_disponiveis, default=areas_default, key="area_sel")
projeto_sel = st.sidebar.multiselect("Projeto", projetos_disponiveis, default=projetos_default, key="projeto_sel")

if min_data and max_data:
    periodo_sel = st.sidebar.date_input(
        "Periodo",
        value=(data_ini, data_fim),
        min_value=min_data,
        max_value=max_data,
        key="periodo_sel",
    )
else:
    periodo_sel = None

# Salva seleção atual na URL para persistir no F5
params["area"] = area_sel
params["projeto"] = projeto_sel
if periodo_sel and len(periodo_sel) == 2:
    params["data_ini"] = str(periodo_sel[0])
    params["data_fim"] = str(periodo_sel[1])

# ─────────────────────────────────────────────────────────────────────────────
# APLICA FILTROS — df_filtrado é a fonte de verdade de TODAS as abas
# ─────────────────────────────────────────────────────────────────────────────

df = df_base.copy()

if area_sel and "area" in df.columns:
    df = df[df["area"].isin(area_sel)]

if projeto_sel and "projeto" in df.columns:
    df = df[df["projeto"].isin(projeto_sel)]

if periodo_sel and len(periodo_sel) == 2 and "mes_ano" in df.columns:
    df = df[
        (df["mes_ano"] >= pd.Timestamp(periodo_sel[0])) &
        (df["mes_ano"] <= pd.Timestamp(periodo_sel[1]))
    ]

# ─────────────────────────────────────────────────────────────────────────────
# TÍTULO
# ─────────────────────────────────────────────────────────────────────────────

st.title("Dashboard Executivo — Deloitte")
st.caption(f"{len(df):,} registros após filtros")
st.markdown("---")

if df.empty:
    st.warning("Nenhum dado encontrado para os filtros selecionados.")
    st.stop()

# ─────────────────────────────────────────────────────────────────────────────
# ABAS
# ─────────────────────────────────────────────────────────────────────────────

tab_kpi, tab_temporal, tab_area, tab_projetos, tab_desvios, tab_dados = st.tabs([
    "KPIs",
    "Serie Temporal",
    "Area",
    "Projetos",
    "Desvios",
    "Dados",
])

# ── ABA 1: KPIs ──────────────────────────────────────────────────────────────

with tab_kpi:
    st.subheader("KPIs Executivos")

    receita_realizada = df["receita_liquida"].sum() if "receita_liquida" in df.columns else 0
    receita_prevista  = df["receita_prevista"].sum() if "receita_prevista" in df.columns else 0
    custo_total       = df["custo_total"].sum() if "custo_total" in df.columns else 0
    desvio_medio      = df["desvio_pct"].mean() if "desvio_pct" in df.columns else 0
    atingimento_medio = df["atingimento_pct"].mean() if "atingimento_pct" in df.columns else 0

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Receita Realizada", f"R$ {receita_realizada:,.0f}")
    col2.metric("Receita Prevista",  f"R$ {receita_prevista:,.0f}")
    col3.metric("Custo Total",       f"R$ {custo_total:,.0f}")
    col4.metric("Desvio Médio",      f"{desvio_medio:.1f}%")
    col5.metric("Atingimento Médio", f"{atingimento_medio:.1f}%")

    # Participação % por sub-área calculada do df filtrado
    if "sigla_sub_area" in df.columns and "receita_liquida" in df.columns:
        st.subheader("Participação por Sub-Área")
        df_part = (
            df.groupby("sigla_sub_area")["receita_liquida"]
            .sum()
            .reset_index()
            .rename(columns={"receita_liquida": "receita"})
        )
        col_a, col_b = st.columns(2)
        fig_pizza = px.pie(df_part, names="sigla_sub_area", values="receita", title="Receita por Sub-Área")
        col_a.plotly_chart(fig_pizza, use_container_width=True)
        col_b.dataframe(df_part, use_container_width=True)

# ── ABA 2: SÉRIE TEMPORAL ────────────────────────────────────────────────────

with tab_temporal:
    st.subheader("Evolução Mensal — Orçado vs Realizado")

    if "mes_ano" in df.columns:
        cols_agg = [c for c in ["receita_prevista", "receita_liquida", "custo_total"] if c in df.columns]
        df_serie = df.groupby("mes_ano")[cols_agg].sum().reset_index().sort_values("mes_ano")

        fig_linha = px.line(
            df_serie,
            x="mes_ano",
            y=cols_agg,
            markers=True,
            title="Série Temporal Mensal",
            labels={"mes_ano": "Mês", "value": "R$", "variable": "Métrica"},
        )
        st.plotly_chart(fig_linha, use_container_width=True)
        st.dataframe(df_serie, use_container_width=True)
    else:
        st.info("Coluna de data não encontrada.")

# ── ABA 3: ÁREA ──────────────────────────────────────────────────────────────

with tab_area:
    st.subheader("Orçado vs Realizado por Área")

    if "area" in df.columns:
        cols_agg = [c for c in ["receita_prevista", "receita_liquida", "custo_total"] if c in df.columns]
        df_area = df.groupby("area")[cols_agg].sum().reset_index()

        fig_bar = px.bar(
            df_area,
            x="area",
            y=cols_agg,
            barmode="group",
            title="Comparativo por Área",
            labels={"value": "R$", "variable": "Métrica"},
        )
        st.plotly_chart(fig_bar, use_container_width=True)
        st.dataframe(df_area, use_container_width=True)

    if "sigla_sub_area" in df.columns:
        st.subheader("Receita por Sub-Área")
        cols_agg2 = [c for c in ["receita_prevista", "receita_liquida"] if c in df.columns]
        df_sub = df.groupby("sigla_sub_area")[cols_agg2].sum().reset_index()

        fig_sub = px.bar(
            df_sub,
            x="sigla_sub_area",
            y=cols_agg2,
            barmode="group",
            title="Receita por Sub-Área",
            labels={"value": "R$", "variable": "Métrica"},
        )
        st.plotly_chart(fig_sub, use_container_width=True)
        st.dataframe(df_sub, use_container_width=True)

# ── ABA 4: PROJETOS ──────────────────────────────────────────────────────────

with tab_projetos:
    st.subheader("Receita por Projeto")

    if "projeto" in df.columns:
        cols_agg = [c for c in ["receita_prevista", "receita_liquida", "custo_total"] if c in df.columns]
        df_proj = df.groupby("projeto")[cols_agg].sum().reset_index().sort_values("receita_liquida", ascending=False)

        fig_proj = px.bar(
            df_proj,
            x="projeto",
            y=cols_agg,
            barmode="group",
            title="Receita por Projeto",
            labels={"value": "R$", "variable": "Métrica"},
        )
        st.plotly_chart(fig_proj, use_container_width=True)
        st.dataframe(df_proj, use_container_width=True)

    if "mes_ano" in df.columns and "projeto" in df.columns and "receita_liquida" in df.columns:
        st.subheader("Evolução de Receita por Projeto")
        df_proj_tempo = df.groupby(["mes_ano", "projeto"])["receita_liquida"].sum().reset_index()
        fig_proj_linha = px.line(
            df_proj_tempo,
            x="mes_ano",
            y="receita_liquida",
            color="projeto",
            markers=True,
            title="Receita Mensal por Projeto",
            labels={"mes_ano": "Mês", "receita_liquida": "R$"},
        )
        st.plotly_chart(fig_proj_linha, use_container_width=True)

# ── ABA 5: DESVIOS ───────────────────────────────────────────────────────────

with tab_desvios:
    st.subheader("Ranking de Desvios por Projeto")

    if "projeto" in df.columns and "desvio_pct" in df.columns:
        df_desv = (
            df.groupby("projeto")[["desvio_pct", "atingimento_pct"]]
            .mean()
            .reset_index()
            .sort_values("desvio_pct")
        )

        fig_desv = px.bar(
            df_desv,
            x="desvio_pct",
            y="projeto",
            orientation="h",
            title="Desvio Médio por Projeto (%)",
            color="desvio_pct",
            color_continuous_scale="RdYlGn",
            labels={"desvio_pct": "Desvio (%)", "projeto": "Projeto"},
        )
        st.plotly_chart(fig_desv, use_container_width=True)
        st.dataframe(df_desv, use_container_width=True)

    if "area" in df.columns and "custo_total" in df.columns:
        st.subheader("Custos por Área")
        cols_custo = [c for c in ["custo_total", "allowance", "contingencia"] if c in df.columns]
        df_custo = df.groupby("area")[cols_custo].sum().reset_index()

        fig_custo = px.bar(
            df_custo,
            x="area",
            y=cols_custo,
            barmode="group",
            title="Breakdown de Custos por Área",
            labels={"value": "R$", "variable": "Tipo"},
        )
        st.plotly_chart(fig_custo, use_container_width=True)
        st.dataframe(df_custo, use_container_width=True)

# ── ABA 6: DADOS BRUTOS ──────────────────────────────────────────────────────

with tab_dados:
    st.subheader("Base Operacional Limpa")
    st.write(f"{len(df):,} registros | {df.shape[1]} colunas")
    st.dataframe(df, use_container_width=True, height=500)

    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Baixar CSV filtrado",
        data=csv,
        file_name="base_operacional_filtrada.csv",
        mime="text/csv",
    )
