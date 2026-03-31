"""Visualização Streamlit usando dados ETL em vitoria/etl.py."""

from pathlib import Path

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from vitoria.etl import carregar_dados

ROOT = Path(__file__).resolve().parent.parent

st.set_page_config(page_title="Dashboard Executivo — Vitoria", page_icon="📊", layout="wide")

CORES_AREA = {"SL01": "#012169", "SL02": "#86BC25"}

st.title("Dashboard Executivo — Vitoria")
st.markdown("---")

try:
    df_op, df_orc = carregar_dados()
except Exception as e:
    st.error(f"Erro ao carregar dados ETL: {e}")
    st.stop()

if df_op.empty:
    st.error("Dados operacionais vazios após ETL. Execute vitoria/etl.py primeiro.")
    st.stop()

# KPI básico
rl_tot = df_op["receita_liquida"].sum()
rp_tot = df_op["receita_prevista"].sum()
ct_tot = df_op["custo_total"].sum()

st.metric("Receita Líquida", f"R$ {rl_tot:,.2f}")
st.metric("Receita Prevista", f"R$ {rp_tot:,.2f}")
st.metric("Custo Total", f"R$ {ct_tot:,.2f}")

# Gráfico simples
if "mes_ref" in df_op.columns:
    ts = df_op.groupby("mes_ref")["receita_liquida", "receita_prevista"].sum().reset_index().sort_values("mes_ref")
    ts["mes"] = ts["mes_ref"].dt.strftime("%b/%y")
    fig = px.line(ts, x="mes", y=["receita_liquida", "receita_prevista"], markers=True,
                  labels={"value": "Receita", "mes": "Mês", "variable": "Tipo"})
    fig.update_layout(title="Série Temporal de Receita")
    st.plotly_chart(fig, use_container_width=True)

# Tabela de dados
st.subheader("Dados Operacionais Preparados")
st.dataframe(df_op.head(50))
