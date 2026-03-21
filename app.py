"""
Dashboard Executivo — Paraná
============================
Visualização dos outputs gerados pelo ETL pipeline (etl_parana.py).

Para rodar:
    streamlit run app.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÃO DA PÁGINA
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Dashboard Executivo — Paraná",
    page_icon="📊",
    layout="wide",
)

ROOT = Path(__file__).resolve().parent
PASTA_SAIDA = ROOT / "output" / "parana"

# ─────────────────────────────────────────────────────────────────────────────
# CARREGAMENTO DE DADOS
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data
def carregar(nome_arquivo: str) -> pd.DataFrame:
    caminho = PASTA_SAIDA / nome_arquivo
    if not caminho.exists():
        return pd.DataFrame()
    return pd.read_excel(caminho)


@st.cache_data
def carregar_base_operacional() -> pd.DataFrame:
    caminho = PASTA_SAIDA / "01_base_operacional_limpa.csv"
    if not caminho.exists():
        caminho = PASTA_SAIDA / "01_base_operacional_limpa.xlsx"
        if not caminho.exists():
            return pd.DataFrame()
        return pd.read_excel(caminho)
    df = pd.read_csv(caminho)
    if "mes_ano" in df.columns:
        df["mes_ano"] = pd.to_datetime(df["mes_ano"], errors="coerce")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR — FILTROS GLOBAIS
# ─────────────────────────────────────────────────────────────────────────────

st.sidebar.title("Filtros")

df_base = carregar_base_operacional()

areas_disponiveis = sorted(df_base["area"].dropna().unique()) if "area" in df_base.columns else []
area_sel = st.sidebar.multiselect("Área", areas_disponiveis, default=areas_disponiveis)

projetos_disponiveis = sorted(df_base["projeto"].dropna().unique()) if "projeto" in df_base.columns else []
projeto_sel = st.sidebar.multiselect("Projeto", projetos_disponiveis, default=projetos_disponiveis)

if "mes_ano" in df_base.columns and not df_base["mes_ano"].isna().all():
    min_data = df_base["mes_ano"].min()
    max_data = df_base["mes_ano"].max()
    data_sel = st.sidebar.date_input(
        "Período",
        value=(min_data, max_data),
        min_value=min_data,
        max_value=max_data,
    )
else:
    data_sel = None

# Aplica filtros na base operacional
df_filtrado = df_base.copy()
if area_sel and "area" in df_filtrado.columns:
    df_filtrado = df_filtrado[df_filtrado["area"].isin(area_sel)]
if projeto_sel and "projeto" in df_filtrado.columns:
    df_filtrado = df_filtrado[df_filtrado["projeto"].isin(projeto_sel)]
if data_sel and len(data_sel) == 2 and "mes_ano" in df_filtrado.columns:
    df_filtrado = df_filtrado[
        (df_filtrado["mes_ano"] >= pd.Timestamp(data_sel[0])) &
        (df_filtrado["mes_ano"] <= pd.Timestamp(data_sel[1]))
    ]

# ─────────────────────────────────────────────────────────────────────────────
# TÍTULO
# ─────────────────────────────────────────────────────────────────────────────

st.title("📊 Dashboard Executivo — Paraná")
st.markdown("---")

# ─────────────────────────────────────────────────────────────────────────────
# ABAS
# ─────────────────────────────────────────────────────────────────────────────

tab_kpi, tab_temporal, tab_area, tab_projetos, tab_desvios, tab_dados = st.tabs([
    "📌 KPIs",
    "📅 Série Temporal",
    "🏢 Área",
    "📁 Projetos",
    "⚠️ Desvios",
    "🗃️ Dados",
])

# ── ABA 1: KPIs ──────────────────────────────────────────────────────────────

with tab_kpi:
    df_kpi = carregar("03_kpis_executivos.xlsx")

    if df_kpi.empty:
        # Calcula KPIs diretamente da base filtrada
        receita_realizada = df_filtrado["receita_liquida"].sum() if "receita_liquida" in df_filtrado.columns else 0
        receita_prevista = df_filtrado["receita_prevista"].sum() if "receita_prevista" in df_filtrado.columns else 0
        custo_total = df_filtrado["custo_total"].sum() if "custo_total" in df_filtrado.columns else 0
        desvio_medio = df_filtrado["desvio_pct"].mean() if "desvio_pct" in df_filtrado.columns else 0
        atingimento_medio = df_filtrado["atingimento_pct"].mean() if "atingimento_pct" in df_filtrado.columns else 0

        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Receita Realizada", f"R$ {receita_realizada:,.0f}")
        col2.metric("Receita Prevista", f"R$ {receita_prevista:,.0f}")
        col3.metric("Custo Total", f"R$ {custo_total:,.0f}")
        col4.metric("Desvio Médio (%)", f"{desvio_medio:.1f}%")
        col5.metric("Atingimento Médio (%)", f"{atingimento_medio:.1f}%")
    else:
        st.subheader("KPIs Executivos")
        st.dataframe(df_kpi, width="stretch")

    # Participação percentual por sub-área
    df_part = carregar("10_participacao_percentual.xlsx")
    if not df_part.empty:
        st.subheader("Participação Percentual")
        col_a, col_b = st.columns(2)
        colunas = df_part.columns.tolist()

        # Tenta montar pizza por área
        col_area_candidatas = [c for c in colunas if "area" in c.lower()]
        col_val_candidatas = [c for c in colunas if any(k in c.lower() for k in ["pct", "percent", "receita", "valor"])]

        if col_area_candidatas and col_val_candidatas:
            fig_pizza = px.pie(
                df_part,
                names=col_area_candidatas[0],
                values=col_val_candidatas[0],
                title="Participação por Área",
            )
            col_a.plotly_chart(fig_pizza, width="stretch")

        col_b.dataframe(df_part, width="stretch")

# ── ABA 2: SÉRIE TEMPORAL ────────────────────────────────────────────────────

with tab_temporal:
    df_serie = carregar("04_serie_temporal_mensal.xlsx")

    if df_serie.empty and not df_filtrado.empty and "mes_ano" in df_filtrado.columns:
        df_serie = (
            df_filtrado.groupby("mes_ano")[["receita_prevista", "receita_liquida", "custo_total"]]
            .sum()
            .reset_index()
        )

    if not df_serie.empty:
        st.subheader("Evolução Mensal — Orçado vs Realizado")

        col_data = next((c for c in df_serie.columns if "mes" in c.lower() or "data" in c.lower()), df_serie.columns[0])
        col_orcado = next((c for c in df_serie.columns if "orcad" in c.lower() or "previst" in c.lower()), None)
        col_realizado = next((c for c in df_serie.columns if "realiz" in c.lower() or "liquid" in c.lower()), None)

        linhas = []
        if col_orcado:
            linhas.append(col_orcado)
        if col_realizado:
            linhas.append(col_realizado)
        if not linhas:
            linhas = [c for c in df_serie.columns if c != col_data][:3]

        fig_linha = px.line(
            df_serie,
            x=col_data,
            y=linhas,
            markers=True,
            title="Série Temporal Mensal",
            labels={col_data: "Mês"},
        )
        st.plotly_chart(fig_linha, width="stretch")
        st.dataframe(df_serie, width="stretch")
    else:
        st.info("Sem dados de série temporal disponíveis.")

    # Orçamento mensal completo
    df_orc_mensal = carregar("11_orcamento_mensal_completo.xlsx")
    if not df_orc_mensal.empty:
        st.subheader("Orçamento Mensal Completo")
        st.dataframe(df_orc_mensal, width="stretch")

# ── ABA 3: ÁREA ──────────────────────────────────────────────────────────────

with tab_area:
    df_area = carregar("05_orcado_vs_realizado_area.xlsx")

    if df_area.empty and not df_filtrado.empty and "area" in df_filtrado.columns:
        df_area = (
            df_filtrado.groupby("area")[["receita_prevista", "receita_liquida"]]
            .sum()
            .reset_index()
        )

    if not df_area.empty:
        st.subheader("Orçado vs Realizado por Área")
        col_area = next((c for c in df_area.columns if "area" in c.lower()), df_area.columns[0])
        cols_vals = [c for c in df_area.columns if c != col_area][:4]

        fig_bar = px.bar(
            df_area,
            x=col_area,
            y=cols_vals,
            barmode="group",
            title="Comparativo por Área",
        )
        st.plotly_chart(fig_bar, width="stretch")
        st.dataframe(df_area, width="stretch")
    else:
        st.info("Sem dados por área disponíveis.")

    # Sub-área
    df_subarea = carregar("07_receita_por_subarea.xlsx")
    if not df_subarea.empty:
        st.subheader("Receita por Sub-Área")
        col_sub = next((c for c in df_subarea.columns if "sub" in c.lower() or "sigla" in c.lower()), df_subarea.columns[0])
        col_val_sub = next((c for c in df_subarea.columns if c != col_sub), None)
        if col_val_sub:
            fig_sub = px.bar(df_subarea, x=col_sub, y=col_val_sub, title="Receita por Sub-Área", color=col_sub)
            st.plotly_chart(fig_sub, width="stretch")
        st.dataframe(df_subarea, width="stretch")

# ── ABA 4: PROJETOS ──────────────────────────────────────────────────────────

with tab_projetos:
    df_proj = carregar("06_receita_por_projeto.xlsx")

    if df_proj.empty and not df_filtrado.empty and "projeto" in df_filtrado.columns:
        df_proj = (
            df_filtrado.groupby("projeto")[["receita_prevista", "receita_liquida", "custo_total"]]
            .sum()
            .reset_index()
        )

    if not df_proj.empty:
        st.subheader("Receita por Projeto")
        col_proj = next((c for c in df_proj.columns if "projeto" in c.lower()), df_proj.columns[0])
        col_val_proj = next((c for c in df_proj.columns if "receita" in c.lower() or "liquid" in c.lower()), None)
        if not col_val_proj:
            col_val_proj = [c for c in df_proj.columns if c != col_proj][0]

        fig_proj = px.bar(
            df_proj.sort_values(col_val_proj, ascending=False),
            x=col_proj,
            y=col_val_proj,
            title="Ranking de Receita por Projeto",
            color=col_proj,
        )
        st.plotly_chart(fig_proj, width="stretch")
        st.dataframe(df_proj, width="stretch")
    else:
        st.info("Sem dados por projeto disponíveis.")

    # Janelas temporais
    df_janelas = carregar("12_janelas_temporais.xlsx")
    if not df_janelas.empty:
        st.subheader("Janelas Temporais")
        st.dataframe(df_janelas, width="stretch")

# ── ABA 5: DESVIOS ───────────────────────────────────────────────────────────

with tab_desvios:
    df_desv = carregar("09_ranking_desvios_projeto.xlsx")

    if df_desv.empty and not df_filtrado.empty and "projeto" in df_filtrado.columns:
        df_desv = (
            df_filtrado.groupby("projeto")[["desvio_pct", "atingimento_pct"]]
            .mean()
            .reset_index()
            .sort_values("desvio_pct")
        )

    if not df_desv.empty:
        st.subheader("⚠️ Ranking de Desvios por Projeto")
        col_proj_d = next((c for c in df_desv.columns if "projeto" in c.lower()), df_desv.columns[0])
        col_desv = next((c for c in df_desv.columns if "desvio" in c.lower()), None)
        if not col_desv:
            col_desv = [c for c in df_desv.columns if c != col_proj_d][0]

        fig_desv = px.bar(
            df_desv.sort_values(col_desv),
            x=col_desv,
            y=col_proj_d,
            orientation="h",
            title="Desvios por Projeto (ordenado)",
            color=col_desv,
            color_continuous_scale="RdYlGn",
        )
        st.plotly_chart(fig_desv, width="stretch")
        st.dataframe(df_desv, width="stretch")
    else:
        st.info("Sem dados de desvios disponíveis.")

    # Custos por dimensão
    df_custos = carregar("08_custos_por_dimensao.xlsx")
    if not df_custos.empty:
        st.subheader("Custos por Dimensão")
        col_dim = df_custos.columns[0]
        col_val_c = next((c for c in df_custos.columns if "custo" in c.lower() or "valor" in c.lower()), None)
        if col_val_c:
            fig_custo = px.bar(df_custos, x=col_dim, y=col_val_c, title="Custos por Dimensão", color=col_dim)
            st.plotly_chart(fig_custo, width="stretch")
        st.dataframe(df_custos, width="stretch")

# ── ABA 6: DADOS BRUTOS ──────────────────────────────────────────────────────

with tab_dados:
    st.subheader("Base Operacional Limpa")

    if not df_filtrado.empty:
        st.write(f"{len(df_filtrado):,} registros | {df_filtrado.shape[1]} colunas")
        st.dataframe(df_filtrado, width="stretch", height=500)

        csv = df_filtrado.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="⬇️ Baixar CSV filtrado",
            data=csv,
            file_name="base_operacional_filtrada.csv",
            mime="text/csv",
        )
    else:
        st.info("Nenhum dado disponível. Execute o ETL primeiro.")
