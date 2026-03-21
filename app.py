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

    if "Mês/Ano" in df.columns:
        df["Mês/Ano"] = pd.to_datetime(df["Mês/Ano"], errors="coerce")
    return df


df_base = carregar_base()

# ─────────────────────────────────────────────────────────────────────────────
# PERSISTÊNCIA DOS FILTROS NA URL (sobrevive ao F5)
# ─────────────────────────────────────────────────────────────────────────────

params = st.query_params

areas_disponiveis = sorted(df_base["Área"].dropna().unique().tolist()) if "Área" in df_base.columns else []
projetos_disponiveis = sorted(df_base["Projeto"].dropna().unique().tolist()) if "Projeto" in df_base.columns else []

# Lê filtros salvos na URL ou usa todos como padrão
areas_default = params.get_all("area") or areas_disponiveis
projetos_default = params.get_all("projeto") or projetos_disponiveis

# Garante que os valores da URL ainda existem nos dados
areas_default = [a for a in areas_default if a in areas_disponiveis] or areas_disponiveis
projetos_default = [p for p in projetos_default if p in projetos_disponiveis] or projetos_disponiveis

# Data
if "Mês/Ano" in df_base.columns and not df_base["Mês/Ano"].isna().all():
    min_data = df_base["Mês/Ano"].min().date()
    max_data = df_base["Mês/Ano"].max().date()
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

if area_sel and "Área" in df.columns:
    df = df[df["Área"].isin(area_sel)]

if projeto_sel and "Projeto" in df.columns:
    df = df[df["Projeto"].isin(projeto_sel)]

if periodo_sel and len(periodo_sel) == 2 and "Mês/Ano" in df.columns:
    df = df[
        (df["Mês/Ano"] >= pd.Timestamp(periodo_sel[0])) &
        (df["Mês/Ano"] <= pd.Timestamp(periodo_sel[1]))
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

    receita_realizada = df["Receita Líquida"].sum() if "Receita Líquida" in df.columns else 0
    receita_prevista  = df["Receita Prevista"].sum() if "Receita Prevista" in df.columns else 0
    custo_total       = df["Custo Total"].sum() if "Custo Total" in df.columns else 0
    desvio_medio      = df["Desvio %"].mean() if "Desvio %" in df.columns else 0
    atingimento_medio = df["Atingimento %"].mean() if "Atingimento %" in df.columns else 0

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Receita Realizada", f"R$ {receita_realizada:,.0f}")
    col2.metric("Receita Prevista",  f"R$ {receita_prevista:,.0f}")
    col3.metric("Custo Total",       f"R$ {custo_total:,.0f}")
    col4.metric("Desvio Médio",      f"{desvio_medio:.1f}%")
    col5.metric("Atingimento Médio", f"{atingimento_medio:.1f}%")

    # Participação % por sub-área calculada do df filtrado
    if "Sigla da Subarea" in df.columns and "Receita Líquida" in df.columns:
        st.subheader("Participação por Sub-Área")
        df_part = (
            df.groupby("Sigla da Subarea")["Receita Líquida"]
            .sum()
            .reset_index()
            .rename(columns={"Receita Líquida": "receita"})
        )
        col_a, col_b = st.columns(2)
        fig_pizza = px.pie(df_part, names="Sigla da Subarea", values="receita", title="Receita por Sub-Área")
        col_a.plotly_chart(fig_pizza, use_container_width=True)
        col_b.dataframe(df_part, use_container_width=True)

# ── ABA 2: SÉRIE TEMPORAL ────────────────────────────────────────────────────

with tab_temporal:
    st.subheader("Evolução Mensal — Orçado vs Realizado")

    if "Mês/Ano" in df.columns:
        cols_agg = [c for c in ["Receita Prevista", "Receita Líquida", "Custo Total"] if c in df.columns]
        df_serie = df.groupby("Mês/Ano")[cols_agg].sum().reset_index().sort_values("Mês/Ano")

        fig_linha = px.line(
            df_serie,
            x="Mês/Ano",
            y=cols_agg,
            markers=True,
            title="Série Temporal Mensal",
            labels={"Mês/Ano": "Mês", "value": "R$", "variable": "Métrica"},
        )
        st.plotly_chart(fig_linha, use_container_width=True)
        st.dataframe(df_serie, use_container_width=True)
        
        csv_serie = df_serie.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Baixar CSV - Série Temporal",
            data=csv_serie,
            file_name="serie_temporal.csv",
            mime="text/csv",
        )
    else:
        st.info("Coluna de data não encontrada.")

# ── ABA 3: ÁREA ──────────────────────────────────────────────────────────────

with tab_area:
    st.subheader("Orçado vs Realizado por Área")

    if "Área" in df.columns:
        cols_agg = [c for c in ["Receita Prevista", "Receita Líquida", "Custo Total"] if c in df.columns]
        df_area = df.groupby("Área")[cols_agg].sum().reset_index()

        fig_bar = px.bar(
            df_area,
            x="Área",
            y=cols_agg,
            barmode="group",
            title="Comparativo por Área",
            labels={"value": "R$", "variable": "Métrica"},
        )
        st.plotly_chart(fig_bar, use_container_width=True)
        st.dataframe(df_area, use_container_width=True)
        
        csv_area = df_area.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Baixar CSV - Área",
            data=csv_area,
            file_name="orcado_vs_realizado_area.csv",
            mime="text/csv",
        )

    if "Sigla da Subarea" in df.columns:
        st.subheader("Receita por Sub-Área")
        cols_agg2 = [c for c in ["Receita Prevista", "Receita Líquida"] if c in df.columns]
        df_sub = df.groupby("Sigla da Subarea")[cols_agg2].sum().reset_index()

        fig_sub = px.bar(
            df_sub,
            x="Sigla da Subarea",
            y=cols_agg2,
            barmode="group",
            title="Receita por Sub-Área",
            labels={"value": "R$", "variable": "Métrica"},
        )
        st.plotly_chart(fig_sub, use_container_width=True)
        st.dataframe(df_sub, use_container_width=True)
        
        csv_sub = df_sub.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Baixar CSV - Sub-Área",
            data=csv_sub,
            file_name="receita_por_subarea.csv",
            mime="text/csv",
        )

# ── ABA 4: PROJETOS ──────────────────────────────────────────────────────────

with tab_projetos:
    st.subheader("Receita por Projeto")

    if "Projeto" in df.columns:
        cols_agg = [c for c in ["Receita Prevista", "Receita Líquida", "Custo Total"] if c in df.columns]
        df_proj = df.groupby("Projeto")[cols_agg].sum().reset_index().sort_values("Receita Líquida", ascending=False)

        fig_proj = px.bar(
            df_proj,
            x="Projeto",
            y=cols_agg,
            barmode="group",
            title="Receita por Projeto",
            labels={"value": "R$", "variable": "Métrica"},
        )
        st.plotly_chart(fig_proj, use_container_width=True)
        st.dataframe(df_proj, use_container_width=True)
        
        csv_proj = df_proj.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Baixar CSV - Projetos",
            data=csv_proj,
            file_name="receita_por_projeto.csv",
            mime="text/csv",
        )

    if "Mês/Ano" in df.columns and "Projeto" in df.columns and "Receita Líquida" in df.columns:
        st.subheader("Evolução de Receita por Projeto")
        df_proj_tempo = df.groupby(["Mês/Ano", "Projeto"])["Receita Líquida"].sum().reset_index()
        fig_proj_linha = px.line(
            df_proj_tempo,
            x="Mês/Ano",
            y="Receita Líquida",
            color="Projeto",
            markers=True,
            title="Receita Mensal por Projeto",
            labels={"Mês/Ano": "Mês", "Receita Líquida": "R$"},
        )
        st.plotly_chart(fig_proj_linha, use_container_width=True)

# ── ABA 5: DESVIOS ───────────────────────────────────────────────────────────

with tab_desvios:
    st.subheader("Ranking de Desvios por Projeto")

    if "Projeto" in df.columns and "Desvio %" in df.columns:
        df_desv = (
            df.groupby("Projeto")[["Desvio %", "Atingimento %"]]
            .mean()
            .reset_index()
            .sort_values("Desvio %")
        )

        fig_desv = px.bar(
            df_desv,
            x="Desvio %",
            y="Projeto",
            orientation="h",
            title="Desvio Médio por Projeto (%)",
            color="Desvio %",
            color_continuous_scale="RdYlGn",
            labels={"Desvio %": "Desvio (%)", "Projeto": "Projeto"},
        )
        st.plotly_chart(fig_desv, use_container_width=True)
        st.dataframe(df_desv, use_container_width=True)
        
        csv_desv = df_desv.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Baixar CSV - Desvios",
            data=csv_desv,
            file_name="ranking_desvios_projeto.csv",
            mime="text/csv",
        )

    if "Área" in df.columns and "Custo Total" in df.columns:
        st.subheader("Custos por Área")
        cols_custo = [c for c in ["Custo Total", "Allowance", "Contingência"] if c in df.columns]
        df_custo = df.groupby("Área")[cols_custo].sum().reset_index()

        fig_custo = px.bar(
            df_custo,
            x="Área",
            y=cols_custo,
            barmode="group",
            title="Breakdown de Custos por Área",
            labels={"value": "R$", "variable": "Tipo"},
        )
        st.plotly_chart(fig_custo, use_container_width=True)
        st.dataframe(df_custo, use_container_width=True)
        
        csv_custo = df_custo.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Baixar CSV - Custos",
            data=csv_custo,
            file_name="custos_por_area.csv",
            mime="text/csv",
        )

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
