"""
Dashboard Executivo — Delloite
==============================
Lê diretamente os arquivos .txt da pasta entrada/ e gera
um dashboard executivo interativo sem dependência de ETL prévio.

Para rodar:
    streamlit run app_parana.py
"""

import re
import unicodedata
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÃO DA PÁGINA
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Dashboard Executivo — Delloite",
    page_icon="📊",
    layout="wide",
)

ROOT = Path(__file__).resolve().parent
PASTA_ENTRADA = ROOT / "entrada"
ENCODINGS = ["utf-16", "utf-16-le", "utf-8-sig", "utf-8", "latin1"]

MESES_PT = {
    "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
    "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12,
}

# Paleta
COR_ORCADO     = "#4472C4"
COR_REALIZADO  = "#70AD47"
COR_DESVIO_NEG = "#E74C3C"
COR_DESVIO_POS = "#27AE60"
COR_DLT_GREEN  = "#86BC25"
COR_DLT_BLUE   = "#012169"

CORES_SUBAREA = {"CO": "#4472C4", "AI": "#86BC25", "En": "#FF9800"}
CORES_AREA    = {"SL01": COR_DLT_BLUE, "SL02": COR_DLT_GREEN}

# Fórmulas para tooltips
FORMULA: dict[str, str] = {
    "desvio_abs":       "Receita Líquida − Receita Prevista",
    "desvio_pct":       "(Receita Líquida − Receita Prevista) ÷ Receita Prevista × 100",
    "atingimento_pct":  "Receita Líquida ÷ Receita Prevista × 100",
    "receita_ajustada": "Receita Líquida − Allowance − Contingência",
    "custo_total":      "Allowance + Contingência",
    "margem_pct":       "Margem ÷ Receita Orçada × 100",
}

# ─────────────────────────────────────────────────────────────────────────────
# CSS  (apenas separadores de seção e alertas — sem cards customizados)
# ─────────────────────────────────────────────────────────────────────────────

st.markdown("""
<style>
.sec-header {
    font-size: 15px; font-weight: 600; color: #012169;
    margin: 18px 0 6px 0;
    border-bottom: 2px solid #86BC25; padding-bottom: 3px;
}
.alert-warn {
    background: #fff8e1; border-left: 4px solid #FFC107;
    padding: 10px 14px; border-radius: 4px; margin: 4px 0; font-size: 13px;
}
.alert-danger {
    background: #fdecea; border-left: 4px solid #E74C3C;
    padding: 10px 14px; border-radius: 4px; margin: 4px 0; font-size: 13px;
}
</style>
""", unsafe_allow_html=True)


def sec(title: str):
    st.markdown(f'<div class="sec-header">{title}</div>', unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# NOMES LEGÍVEIS PARA COLUNAS
# ─────────────────────────────────────────────────────────────────────────────

NOMES_COL: dict[str, str] = {
    "area":             "Área",
    "projeto":          "Projeto",
    "funcionario":      "Funcionário",
    "centro_custo":     "Centro de Custo",
    "sigla_sub_area":   "Sub Área",
    "id_quinzena":      "Quinzena",
    "mes_ano":          "Mês/Ano",
    "mes_ref":          "Mês",
    "receita_liquida":  "Receita Líquida",
    "receita_prevista": "Receita Prevista",
    "allowance":        "Allowance",
    "contingencia":     "Contingência",
    "ajuste":           "Ajuste",
    "custo_total":      "Custo Total",
    "desvio_abs":       "Desvio (R$)",
    "desvio_pct":       "Desvio (%)",
    "atingimento_pct":  "Atingimento (%)",
    "receita_ajustada": "Receita Ajustada",
    "orcado_budget":    "Receita Orçada (Budget)",
    "orcado_receita":   "Receita Orçada",
    "Mês":              "Mês",
}

# column_config completo — rótulos legíveis + help nas calculadas
COL_CFG: dict[str, st.column_config.Column] = {
    "area":             st.column_config.TextColumn("Área"),
    "projeto":          st.column_config.TextColumn("Projeto"),
    "funcionario":      st.column_config.TextColumn("Funcionário"),
    "centro_custo":     st.column_config.TextColumn("Centro de Custo"),
    "sigla_sub_area":   st.column_config.TextColumn("Sub Área"),
    "id_quinzena":      st.column_config.NumberColumn("Quinzena", format="%d"),
    "mes_ref":          st.column_config.DateColumn("Mês", format="MMM/YY"),
    "receita_liquida":  st.column_config.NumberColumn("Receita Líquida",  format="R$ %.0f"),
    "receita_prevista": st.column_config.NumberColumn("Receita Prevista", format="R$ %.0f"),
    "allowance":        st.column_config.NumberColumn("Allowance",        format="R$ %.0f"),
    "contingencia":     st.column_config.NumberColumn("Contingência",     format="R$ %.0f"),
    "ajuste":           st.column_config.NumberColumn("Ajuste",           format="R$ %.0f"),
    "custo_total": st.column_config.NumberColumn(
        "Custo Total", format="R$ %.0f", help=FORMULA["custo_total"]),
    "desvio_abs": st.column_config.NumberColumn(
        "Desvio (R$)", format="R$ %.0f", help=FORMULA["desvio_abs"]),
    "desvio_pct": st.column_config.NumberColumn(
        "Desvio (%)", format="%.1f%%", help=FORMULA["desvio_pct"]),
    "atingimento_pct": st.column_config.NumberColumn(
        "Atingimento (%)", format="%.1f%%", help=FORMULA["atingimento_pct"]),
    "receita_ajustada": st.column_config.NumberColumn(
        "Receita Ajustada", format="R$ %.0f", help=FORMULA["receita_ajustada"]),
    "orcado_budget": st.column_config.NumberColumn(
        "Receita Orçada (Budget)", format="R$ %.0f"),
    "orcado_receita": st.column_config.NumberColumn(
        "Receita Orçada", format="R$ %.0f"),
}


def tbl(df: pd.DataFrame, **kwargs):
    """Exibe dataframe com column_config automático para todas as colunas conhecidas."""
    cfg = {c: COL_CFG[c] for c in df.columns if c in COL_CFG}
    st.dataframe(df, use_container_width=True, hide_index=True,
                 column_config=cfg, **kwargs)


# ─────────────────────────────────────────────────────────────────────────────
# FUNÇÕES UTILITÁRIAS DE DADOS
# ─────────────────────────────────────────────────────────────────────────────

def normalizar_coluna(nome: str) -> str:
    nome = str(nome).strip()
    nome = unicodedata.normalize("NFKD", nome).encode("ascii", "ignore").decode("ascii")
    nome = re.sub(r"[^a-zA-Z0-9\s_]", " ", nome)
    nome = re.sub(r"\s+", "_", nome.strip()).lower()
    return nome


def limpar_valor_brl(val) -> float:
    if pd.isna(val):
        return np.nan
    s = re.sub(r"R\$\s*", "", str(val).strip())
    s = s.replace(".", "").replace(",", ".").strip()
    try:
        return float(s)
    except ValueError:
        return np.nan


def _carregar_txt_raw(caminho: Path) -> pd.DataFrame:
    for enc in ENCODINGS:
        try:
            with open(caminho, encoding=enc, errors="replace") as f:
                amostra = f.read(4096)
            tab_c = amostra.count("\t")
            sep = "\t" if tab_c >= amostra.count(";") and tab_c >= amostra.count(",") else (
                  ";" if amostra.count(";") >= amostra.count(",") else ",")
            df = pd.read_csv(caminho, encoding=enc, sep=sep, on_bad_lines="skip")
            df = df.dropna(how="all").dropna(axis=1, how="all")
            if len(df) > 0 and len(df.columns) > 1:
                return df
        except Exception:
            continue
    return pd.DataFrame()


def _col(df: pd.DataFrame, *candidatos: str):
    for c in candidatos:
        matches = [col for col in df.columns if c in col]
        if matches:
            return matches[0]
    return None


def _parse_mes(s: str):
    m = re.match(r"([a-z]{3})[\W_]?(\d{2,4})", str(s).strip().lower())
    if m:
        mes_num = MESES_PT.get(m.group(1)[:3])
        if mes_num:
            ano = int(m.group(2))
            ano = 2000 + ano if ano < 100 else ano
            try:
                return pd.Timestamp(year=ano, month=mes_num, day=1)
            except Exception:
                pass
    return pd.NaT


# ─────────────────────────────────────────────────────────────────────────────
# CARREGAMENTO & TRANSFORMAÇÃO
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner="Carregando dados…")
def carregar_dados():
    # ── Operacional ───────────────────────────────────────────────────────────
    df_raw = _carregar_txt_raw(PASTA_ENTRADA / "data1.csv.txt")
    if df_raw.empty:
        return pd.DataFrame(), pd.DataFrame()

    df_raw.columns = [normalizar_coluna(c) for c in df_raw.columns]

    rename = {}
    for src, dst in [
        (_col(df_raw, "receita_liquida", "liquida"),     "receita_liquida"),
        (_col(df_raw, "receita_prevista", "prevista"),   "receita_prevista"),
        (_col(df_raw, "allowance"),                       "allowance"),
        (_col(df_raw, "contingencia", "contingenc"),      "contingencia"),
        (_col(df_raw, "ajuste"),                          "ajuste"),
        (_col(df_raw, "area"),                            "area"),
        (_col(df_raw, "projeto"),                         "projeto"),
        (_col(df_raw, "funcionario"),                     "funcionario"),
        (_col(df_raw, "centro_de_custo", "centro"),       "centro_custo"),
        (_col(df_raw, "id_quinzena", "quinzena"),         "id_quinzena"),
        (_col(df_raw, "sigla_sub_area", "sub_area"),      "sigla_sub_area"),
        (_col(df_raw, "mes_ano", "mes", "data", "m_s"),   "mes_ano"),
    ]:
        if src and src not in rename:
            rename[src] = dst

    df_op = df_raw.rename(columns=rename).copy()

    for c in ["receita_liquida", "receita_prevista", "allowance", "contingencia", "ajuste"]:
        if c in df_op.columns:
            df_op[c] = df_op[c].apply(limpar_valor_brl)

    if "mes_ano" in df_op.columns:
        df_op["mes_ano"] = pd.to_datetime(df_op["mes_ano"], errors="coerce", dayfirst=True)
        df_op["mes_ref"] = df_op["mes_ano"].dt.to_period("M").dt.to_timestamp()

    for c in ["area", "projeto", "funcionario", "sigla_sub_area", "centro_custo"]:
        if c in df_op.columns:
            df_op[c] = df_op[c].astype(str).str.strip()

    rl = df_op.get("receita_liquida",  pd.Series(dtype=float))
    rp = df_op.get("receita_prevista", pd.Series(dtype=float))
    al = df_op.get("allowance",    pd.Series(np.zeros(len(df_op)), index=df_op.index))
    co = df_op.get("contingencia", pd.Series(np.zeros(len(df_op)), index=df_op.index))

    df_op["custo_total"]      = al + co
    df_op["desvio_abs"]       = rl - rp
    df_op["desvio_pct"]       = np.where(rp != 0, (rl - rp) / rp * 100, np.nan)
    df_op["atingimento_pct"]  = np.where(rp != 0, rl / rp * 100, np.nan)
    df_op["receita_ajustada"] = rl - al - co

    # ── Orçamento ─────────────────────────────────────────────────────────────
    df_orc_raw = _carregar_txt_raw(PASTA_ENTRADA / "BookService.txt")
    df_orc = pd.DataFrame()

    if not df_orc_raw.empty:
        df_orc_raw.columns = [normalizar_coluna(c) for c in df_orc_raw.columns]
        col_a = _col(df_orc_raw, "area")
        col_t = _col(df_orc_raw, "type", "tipo")

        if col_a and col_t:
            df_orc_raw = df_orc_raw.rename(columns={col_a: "area", col_t: "tipo"})
            id_vars  = ["area", "tipo"]
            mes_cols = [c for c in df_orc_raw.columns
                        if c not in id_vars and re.match(r"[a-z]{3}\d{2}", c)]
            if not mes_cols:
                mes_cols = [c for c in df_orc_raw.columns if c not in id_vars]

            if mes_cols:
                df_orc = (
                    df_orc_raw[id_vars + mes_cols]
                    .melt(id_vars=id_vars, var_name="mes_col", value_name="valor")
                )
                df_orc["valor"]   = df_orc["valor"].apply(limpar_valor_brl)
                df_orc            = df_orc.dropna(subset=["valor"])
                df_orc["mes_ref"] = df_orc["mes_col"].apply(_parse_mes)
                df_orc            = df_orc.dropna(subset=["mes_ref"])
                df_orc["area"]    = df_orc["area"].astype(str).str.strip()
                df_orc["tipo"]    = df_orc["tipo"].astype(str).str.strip()

    return df_op, df_orc


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS DE FILTROS E KPIs
# ─────────────────────────────────────────────────────────────────────────────

def _vals(df: pd.DataFrame, col: str) -> list:
    return sorted(df[col].dropna().unique().tolist()) if col in df.columns else []


def filtros(df: pd.DataFrame, com_area=False, com_projeto=False,
            com_subarea=False, com_funcionario=False,
            com_data=False, sufixo="") -> pd.DataFrame:
    """
    Renderiza filtros diretamente (sem expander) apenas para as dimensões
    presentes nos dados. Retorna o dataframe filtrado.
    """
    n = sum([com_area, com_projeto, com_subarea, com_funcionario, com_data])
    if n == 0:
        return df

    cols = st.columns(n)
    idx  = 0
    areas = projetos = subareas = funcs = None
    start = end = None

    if com_area:
        opts = _vals(df, "area")
        with cols[idx]:
            areas = st.multiselect(
                "Área", opts, default=opts, key=f"fa_{sufixo}",
                help=(
                    "**Área organizacional** responsável pelo recurso ou receita.\n\n"
                    "Selecione uma ou mais áreas para filtrar todos os gráficos e "
                    "indicadores desta aba. Remover uma área exclui completamente "
                    "sua receita, custo e atingimento dos cálculos."
                ),
            )
        idx += 1

    if com_projeto:
        opts = _vals(df, "projeto")
        with cols[idx]:
            projetos = st.multiselect(
                "Projeto", opts, default=opts, key=f"fp_{sufixo}",
                help=(
                    "**Projeto** ao qual a receita, allowance ou contingência se "
                    "refere.\n\n"
                    "Filtra registros pelo código/nome do projeto. Útil para isolar "
                    "a performance de um projeto específico sem alterar os demais "
                    "filtros."
                ),
            )
        idx += 1

    if com_subarea:
        opts = _vals(df, "sigla_sub_area")
        with cols[idx]:
            subareas = st.multiselect("Sub Área", opts, default=opts, key=f"fs_{sufixo}")
        idx += 1

    if com_funcionario:
        opts = _vals(df, "funcionario")
        with cols[idx]:
            funcs = st.multiselect("Funcionário", opts, default=opts, key=f"ff_{sufixo}")
        idx += 1

    if com_data and "mes_ref" in df.columns and not df["mes_ref"].isna().all():
        min_d = df["mes_ref"].min().date()
        max_d = df["mes_ref"].max().date()
        with cols[idx]:
            intervalo = st.date_input(
                "Período", value=(min_d, max_d),
                min_value=min_d, max_value=max_d,
                key=f"fd_{sufixo}",
            )
        start, end = (intervalo if len(intervalo) == 2 else (min_d, max_d))
        idx += 1

    d = df.copy()
    if areas    is not None and "area"           in d.columns: d = d[d["area"].isin(areas)]
    if projetos is not None and "projeto"        in d.columns: d = d[d["projeto"].isin(projetos)]
    if subareas is not None and "sigla_sub_area" in d.columns: d = d[d["sigla_sub_area"].isin(subareas)]
    if funcs    is not None and "funcionario"    in d.columns: d = d[d["funcionario"].isin(funcs)]
    if start is not None and end is not None and "mes_ref" in d.columns:
        try:
            d = d[
                (d["mes_ref"] >= pd.Timestamp(start)) &
                (d["mes_ref"] <= pd.Timestamp(end))
            ]
        except Exception:
            pass
    return d


def _delta_mes(df: pd.DataFrame, col: str) -> tuple[float | None, str, str]:
    """
    Retorna (variação %, label_atual, label_anterior).
    Compara o mês mais recente nos dados com o mês imediatamente anterior.
    """
    if "mes_ref" not in df.columns or df.empty:
        return None, "", ""
    meses = sorted(df["mes_ref"].dropna().unique())
    if len(meses) < 2:
        return None, "", ""
    mes_atual = meses[-1]
    mes_ant   = meses[-2]
    v_atual   = df[df["mes_ref"] == mes_atual][col].sum()
    v_ant     = df[df["mes_ref"] == mes_ant][col].sum()
    lbl_atual = mes_atual.strftime("%b/%y")
    lbl_ant   = mes_ant.strftime("%b/%y")
    if v_ant == 0:
        return None, lbl_atual, lbl_ant
    return (v_atual - v_ant) / abs(v_ant) * 100, lbl_atual, lbl_ant


def _fmt_delta(result: tuple[float | None, str, str], inverso=False) -> str | None:
    """
    Formata delta para st.metric incluindo os meses comparados.
    Sinal positivo = verde, negativo = vermelho (inverso=True inverte a lógica).
    """
    pct, lbl_atual, lbl_ant = result
    if pct is None:
        return None
    if inverso:
        pct = -pct
    sinal = "+" if pct >= 0 else ""
    return f"{sinal}{pct:.1f}%  ({lbl_atual} vs {lbl_ant})"


def _aplicar_janela(df: pd.DataFrame, janela: str) -> pd.DataFrame:
    if "mes_ref" not in df.columns or df.empty:
        return df
    max_d = df["mes_ref"].max()
    offsets = {
        "M":   pd.DateOffset(months=1),
        "3M":  pd.DateOffset(months=3),
        "6M":  pd.DateOffset(months=6),
        "1Y":  pd.DateOffset(months=12),
    }
    if janela in offsets:
        return df[df["mes_ref"] >= max_d - offsets[janela]]
    if janela == "YTD":
        return df[df["mes_ref"] >= pd.Timestamp(year=max_d.year, month=1, day=1)]
    return df


def fmt_brl(v, dec=0) -> str:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "—"
    s = f"{v:,.{dec}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"


# ─────────────────────────────────────────────────────────────────────────────
# CARREGA DADOS
# ─────────────────────────────────────────────────────────────────────────────

df_op, df_orc = carregar_dados()

if df_op.empty:
    st.error("Não foi possível carregar os dados. Verifique a pasta `entrada/`.")
    st.stop()

# ─────────────────────────────────────────────────────────────────────────────
# TÍTULO
# ─────────────────────────────────────────────────────────────────────────────

st.title("📊 Dashboard Executivo — Delloite")
st.markdown("---")

# ─────────────────────────────────────────────────────────────────────────────
# ABAS
# ─────────────────────────────────────────────────────────────────────────────

(tab_resumo, tab_kpi, tab_temporal,
 tab_area, tab_proj, tab_desvios, tab_dados, tab_dicionario) = st.tabs([
    "📋 Resumo",
    "📌 KPIs Executivos",
    "📅 Série Temporal",
    "🏢 Áreas",
    "📁 Projetos",
    "⚠️ Desvios & Alertas",
    "🗃️ Dados",
    "📖 Dicionário",
])

# ══════════════════════════════════════════════════════════════════════════════
# ABA 1 — RESUMO
# ══════════════════════════════════════════════════════════════════════════════

with tab_resumo:
    df_res = filtros(df_op, com_data=True, sufixo="res")

    rl_tot = df_res["receita_liquida"].sum()
    rp_tot = df_res["receita_prevista"].sum()
    ct_tot = df_res["custo_total"].sum()
    dev    = rl_tot - rp_tot
    ating  = (rl_tot / rp_tot * 100) if rp_tot else 0.0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Receita Líquida",   fmt_brl(rl_tot),
              delta=_fmt_delta(_delta_mes(df_res, "receita_liquida")),
              help="Soma da Receita Líquida no período selecionado.")
    c2.metric("Receita Prevista",  fmt_brl(rp_tot),
              help="Soma da Receita Prevista no período selecionado.")
    c3.metric("Desvio",            fmt_brl(dev),
              delta=_fmt_delta(_delta_mes(df_res, "desvio_abs")),
              help=FORMULA["desvio_abs"])
    c4.metric("Atingimento",       f"{ating:.1f}%",
              delta=_fmt_delta(_delta_mes(df_res, "atingimento_pct")),
              help=FORMULA["atingimento_pct"])
    c5.metric("Custo Total",       fmt_brl(ct_tot),
              delta=_fmt_delta(_delta_mes(df_res, "custo_total"), inverso=True),
              help=FORMULA["custo_total"])

    st.markdown("")
    col_g, col_d = st.columns(2)

    with col_g:
        sec("Atingimento Geral")
        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=round(ating, 1),
            number={"suffix": "%", "font": {"size": 38}},
            delta={"reference": 100, "valueformat": ".1f", "suffix": "%"},
            gauge={
                "axis": {"range": [0, 150], "ticksuffix": "%"},
                "bar":  {"color": COR_DLT_GREEN},
                "steps": [
                    {"range": [0,   70], "color": "#fdecea"},
                    {"range": [70,  90], "color": "#fff8e1"},
                    {"range": [90, 110], "color": "#e8f5e9"},
                    {"range": [110, 150], "color": "#e3f2fd"},
                ],
                "threshold": {
                    "line": {"color": COR_DESVIO_NEG, "width": 3},
                    "thickness": 0.8, "value": 100,
                },
            },
            title={"text": "Atingimento de Receita (%)"},
        ))
        fig_gauge.update_layout(height=280, margin=dict(t=40, b=10, l=20, r=20))
        st.plotly_chart(fig_gauge, use_container_width=True, key="res_gauge")

    with col_d:
        sec("Composição por Área")
        if "area" in df_res.columns:
            fig_donut = px.pie(
                df_res.groupby("area")["receita_liquida"].sum().reset_index(),
                names="area", values="receita_liquida",
                hole=0.55, color="area", color_discrete_map=CORES_AREA,
            )
            fig_donut.update_traces(textinfo="percent+label")
            fig_donut.update_layout(
                height=280, margin=dict(t=10, b=10, l=10, r=10),
                showlegend=True, legend=dict(orientation="h", y=-0.1),
            )
            st.plotly_chart(fig_donut, use_container_width=True, key="res_donut")

    st.markdown("")
    col_top, col_bot = st.columns(2)

    with col_top:
        sec("Top 5 Projetos — Receita Líquida")
        if "projeto" in df_res.columns:
            top5 = (
                df_res.groupby("projeto")["receita_liquida"].sum()
                .nlargest(5).reset_index()
            )
            tbl(top5)

    with col_bot:
        sec("Projetos com Maior Desvio Negativo")
        if "projeto" in df_res.columns:
            bot5 = (
                df_res.groupby("projeto")["desvio_abs"].sum()
                .nsmallest(5).reset_index()
            )
            tbl(bot5)

    if "mes_ref" in df_res.columns:
        sec("Evolução Mensal")
        ts_sp = (
            df_res.groupby("mes_ref")[["receita_liquida", "receita_prevista"]]
            .sum().reset_index().sort_values("mes_ref")
        )
        ts_sp["Mês"] = ts_sp["mes_ref"].dt.strftime("%b/%y")
        fig_sp = px.line(
            ts_sp, x="Mês", y=["receita_liquida", "receita_prevista"],
            markers=True,
            color_discrete_map={
                "receita_liquida":  COR_REALIZADO,
                "receita_prevista": COR_ORCADO,
            },
            labels={"value": "R$", "variable": "Série"},
        )
        fig_sp.update_layout(height=250, margin=dict(t=10, b=30))
        st.plotly_chart(fig_sp, use_container_width=True, key="res_sparkline")


# ══════════════════════════════════════════════════════════════════════════════
# ABA 2 — KPIs EXECUTIVOS
# ══════════════════════════════════════════════════════════════════════════════

with tab_kpi:
    df_kp = filtros(df_op, com_area=True, com_data=True, sufixo="kpi")

    rl  = df_kp["receita_liquida"].sum()
    rp  = df_kp["receita_prevista"].sum()
    ct  = df_kp["custo_total"].sum()
    at  = (rl / rp * 100) if rp else 0.0
    ra  = df_kp.get("receita_ajustada", pd.Series(dtype=float)).sum()
    da  = rl - rp

    sec("Métricas Principais")
    c1, c2, c3 = st.columns(3)
    c4, c5, c6 = st.columns(3)

    c1.metric("Receita Líquida",   fmt_brl(rl),
              delta=_fmt_delta(_delta_mes(df_kp, "receita_liquida")),
              help="Soma da Receita Líquida no período.")
    c2.metric("Receita Prevista",  fmt_brl(rp),
              help="Soma da Receita Prevista no período.")
    c3.metric("Custo Total",       fmt_brl(ct),
              delta=_fmt_delta(_delta_mes(df_kp, "custo_total"), inverso=True),
              help=FORMULA["custo_total"])
    c4.metric("Desvio (R$)",       fmt_brl(da),
              delta=_fmt_delta(_delta_mes(df_kp, "desvio_abs")),
              help=FORMULA["desvio_abs"])
    c5.metric("Atingimento (%)",   f"{at:.1f}%",
              delta=_fmt_delta(_delta_mes(df_kp, "atingimento_pct")),
              help=FORMULA["atingimento_pct"])
    c6.metric("Receita Ajustada",  fmt_brl(ra),
              delta=_fmt_delta(_delta_mes(df_kp, "receita_ajustada")),
              help=FORMULA["receita_ajustada"])

    if "area" in df_kp.columns:
        sec("KPIs por Área")
        kpi_area = df_kp.groupby("area").agg(
            receita_liquida =("receita_liquida",  "sum"),
            receita_prevista=("receita_prevista", "sum"),
            custo_total     =("custo_total",      "sum"),
            desvio_abs      =("desvio_abs",       "sum"),
            atingimento_pct =("atingimento_pct",  "mean"),
        ).reset_index()
        kpi_area["desvio_pct"] = np.where(
            kpi_area["receita_prevista"] != 0,
            kpi_area["desvio_abs"] / kpi_area["receita_prevista"] * 100, np.nan,
        )
        tbl(kpi_area)

        col_a, col_b = st.columns(2)
        with col_a:
            sec("Receita vs Custo por Área")
            fig_bar = px.bar(
                kpi_area.melt(id_vars="area",
                              value_vars=["receita_liquida", "receita_prevista", "custo_total"],
                              var_name="Métrica", value_name="Valor"),
                x="area", y="Valor", color="Métrica", barmode="group",
                color_discrete_map={
                    "receita_liquida":  COR_REALIZADO,
                    "receita_prevista": COR_ORCADO,
                    "custo_total":      "#FF9800",
                },
                labels={"Valor": "R$", "area": "Área"},
            )
            fig_bar.update_layout(height=340, margin=dict(t=10, b=30))
            st.plotly_chart(fig_bar, use_container_width=True, key="kpi_bar_area")

        with col_b:
            sec("Atingimento por Área (%)")
            fig_at = px.bar(
                kpi_area, x="area", y="atingimento_pct",
                color="area", color_discrete_map=CORES_AREA,
                text_auto=".1f",
                labels={"atingimento_pct": "Atingimento (%)", "area": "Área"},
            )
            fig_at.add_hline(y=100, line_dash="dash", line_color=COR_DESVIO_NEG,
                             annotation_text="Meta 100%")
            fig_at.update_layout(height=340, margin=dict(t=10, b=30))
            st.plotly_chart(fig_at, use_container_width=True, key="kpi_ating_area")


# ══════════════════════════════════════════════════════════════════════════════
# ABA 3 — SÉRIE TEMPORAL
# ══════════════════════════════════════════════════════════════════════════════

with tab_temporal:
    df_ts = filtros(df_op, com_area=True, com_data=True, sufixo="ts")

    sec("Janela Temporal — Acumulado de Métricas")
    janela = st.radio(
        "Selecione a janela",
        ["M", "3M", "6M", "1Y", "YTD"],
        index=2, horizontal=True, key="janela",
    )

    df_j  = _aplicar_janela(df_ts, janela)
    j_rl  = df_j["receita_liquida"].sum()
    j_rp  = df_j["receita_prevista"].sum()
    j_ct  = df_j["custo_total"].sum()
    j_dev = j_rl - j_rp
    j_at  = (j_rl / j_rp * 100) if j_rp else 0.0

    cj1, cj2, cj3, cj4, cj5 = st.columns(5)
    cj1.metric(f"Receita Líquida ({janela})",   fmt_brl(j_rl),
               help="Soma da Receita Líquida na janela selecionada.")
    cj2.metric(f"Receita Prevista ({janela})",  fmt_brl(j_rp),
               help="Soma da Receita Prevista na janela selecionada.")
    cj3.metric(f"Desvio ({janela})",            fmt_brl(j_dev),
               delta=f"{'+' if j_dev >= 0 else ''}{j_dev / j_rp * 100:.1f}%" if j_rp else None,
               help=FORMULA["desvio_abs"])
    cj4.metric(f"Atingimento ({janela})",       f"{j_at:.1f}%",
               delta=f"{j_at - 100:.1f}% vs meta 100%",
               help=FORMULA["atingimento_pct"])
    cj5.metric(f"Custo Total ({janela})",       fmt_brl(j_ct),
               help=FORMULA["custo_total"])

    st.markdown("")

    if "mes_ref" not in df_ts.columns:
        st.info("Dados temporais não disponíveis.")
    else:
        ts = (
            df_ts.groupby("mes_ref")
            .agg(
                receita_liquida =("receita_liquida",  "sum"),
                receita_prevista=("receita_prevista", "sum"),
                custo_total     =("custo_total",      "sum"),
                desvio_abs      =("desvio_abs",       "sum"),
            )
            .reset_index().sort_values("mes_ref")
        )

        # Mescla orçamento do BookService
        if not df_orc.empty:
            areas_ativas = df_ts["area"].unique().tolist() if "area" in df_ts.columns else []
            orc_mask = df_orc["tipo"].str.lower() == "receita"
            if areas_ativas and "area" in df_orc.columns:
                orc_mask &= df_orc["area"].isin(areas_ativas)
            orc_r = (
                df_orc[orc_mask]
                .groupby("mes_ref")["valor"].sum().reset_index()
                .rename(columns={"valor": "orcado_receita"})
            )
            ts = ts.merge(orc_r, on="mes_ref", how="left")

        ts["Mês"] = ts["mes_ref"].dt.strftime("%b/%y")
        ts["atingimento_pct"] = np.where(
            ts["receita_prevista"] != 0,
            ts["receita_liquida"] / ts["receita_prevista"] * 100, np.nan,
        )

        sec("Evolução Mensal — Orçado × Realizado")
        y_linhas = ["receita_liquida", "receita_prevista"]
        if "orcado_receita" in ts.columns:
            y_linhas.append("orcado_receita")
        fig_linha = px.line(
            ts, x="Mês", y=y_linhas, markers=True,
            color_discrete_map={
                "receita_liquida":  COR_REALIZADO,
                "receita_prevista": COR_ORCADO,
                "orcado_receita":   "#9B59B6",
            },
            labels={"value": "R$", "variable": "Série"},
        )
        fig_linha.update_layout(height=320, margin=dict(t=10, b=30))
        st.plotly_chart(fig_linha, use_container_width=True, key="ts_linha")

        col_dv, col_ct = st.columns(2)

        with col_dv:
            sec("Desvio Mensal (Realizado − Previsto)")
            fig_dev = go.Figure(go.Bar(
                x=ts["Mês"], y=ts["desvio_abs"],
                marker_color=[COR_DESVIO_POS if v >= 0 else COR_DESVIO_NEG
                              for v in ts["desvio_abs"]],
                text=[fmt_brl(v) for v in ts["desvio_abs"]],
                textposition="outside",
            ))
            fig_dev.add_hline(y=0, line_color="gray", line_dash="dot")
            fig_dev.update_layout(height=290, margin=dict(t=10, b=30),
                                  yaxis_title="Desvio (R$)")
            st.plotly_chart(fig_dev, use_container_width=True, key="ts_desvio")

        with col_ct:
            sec("Custo Total Mensal")
            fig_area = px.area(
                ts, x="Mês", y="custo_total",
                color_discrete_sequence=["#FF9800"],
                labels={"custo_total": "Custo Total (R$)"},
            )
            fig_area.update_layout(height=290, margin=dict(t=10, b=30))
            st.plotly_chart(fig_area, use_container_width=True, key="ts_custo_area")

        sec("Atingimento Mensal (%)")
        fig_at_ts = px.line(
            ts, x="Mês", y="atingimento_pct", markers=True,
            color_discrete_sequence=[COR_DLT_GREEN],
            labels={"atingimento_pct": "Atingimento (%)"},
        )
        fig_at_ts.add_hline(y=100, line_dash="dash", line_color=COR_DESVIO_NEG,
                            annotation_text="Meta 100%")
        fig_at_ts.update_layout(height=260, margin=dict(t=10, b=30))
        st.plotly_chart(fig_at_ts, use_container_width=True, key="ts_ating")


# ══════════════════════════════════════════════════════════════════════════════
# ABA 4 — ÁREAS
# ══════════════════════════════════════════════════════════════════════════════

with tab_area:
    df_ar = filtros(df_op, com_area=True, com_data=True, sufixo="ar")

    sec("Orçado × Realizado por Área")
    if "area" in df_ar.columns:
        ag_area = df_ar.groupby("area").agg(
            receita_liquida =("receita_liquida",  "sum"),
            receita_prevista=("receita_prevista", "sum"),
            custo_total     =("custo_total",      "sum"),
            atingimento_pct =("atingimento_pct",  "mean"),
        ).reset_index()

        if not df_orc.empty and "area" in df_orc.columns:
            orc_area = (
                df_orc[df_orc["tipo"].str.lower() == "receita"]
                .groupby("area")["valor"].sum().reset_index()
                .rename(columns={"valor": "orcado_budget"})
            )
            ag_area = ag_area.merge(orc_area, on="area", how="left")
            y_bar = ["receita_prevista", "receita_liquida", "custo_total", "orcado_budget"]
        else:
            y_bar = ["receita_prevista", "receita_liquida", "custo_total"]

        col_b1, col_b2 = st.columns(2)
        with col_b1:
            fig_ar = px.bar(
                ag_area.melt(id_vars="area", value_vars=y_bar,
                             var_name="Métrica", value_name="Valor"),
                x="area", y="Valor", color="Métrica", barmode="group",
                color_discrete_map={
                    "receita_liquida":  COR_REALIZADO,
                    "receita_prevista": COR_ORCADO,
                    "custo_total":      "#FF9800",
                    "orcado_budget":    "#9B59B6",
                },
                labels={"Valor": "R$", "area": "Área"},
            )
            fig_ar.update_layout(height=340, margin=dict(t=10, b=30))
            st.plotly_chart(fig_ar, use_container_width=True, key="ar_orcado_real")

        with col_b2:
            sec("Atingimento por Área (%)")
            fig_at_ar = px.bar(
                ag_area, x="area", y="atingimento_pct",
                color="area", color_discrete_map=CORES_AREA, text_auto=".1f",
                labels={"atingimento_pct": "Atingimento (%)", "area": "Área"},
            )
            fig_at_ar.add_hline(y=100, line_dash="dash", line_color=COR_DESVIO_NEG,
                                annotation_text="Meta 100%")
            fig_at_ar.update_layout(height=340, margin=dict(t=10, b=30))
            st.plotly_chart(fig_at_ar, use_container_width=True, key="ar_ating")

        tbl(ag_area)

    # Evolução temporal por área
    if "area" in df_ar.columns and "mes_ref" in df_ar.columns:
        sec("Evolução Temporal por Área")
        ts_area = (
            df_ar.groupby(["mes_ref", "area"])["receita_liquida"]
            .sum().reset_index()
        )
        ts_area["Mês"] = ts_area["mes_ref"].dt.strftime("%b/%y")
        fig_ts_ar = px.line(
            ts_area, x="Mês", y="receita_liquida", color="area",
            markers=True, color_discrete_map=CORES_AREA,
            labels={"receita_liquida": "Receita Líquida (R$)", "area": "Área"},
        )
        fig_ts_ar.update_layout(height=300, margin=dict(t=10, b=30))
        st.plotly_chart(fig_ts_ar, use_container_width=True, key="ar_ts")

    # Sub-área — com seletor de área principal
    if "sigla_sub_area" in df_op.columns:
        sec("Receita por Sub Área")

        # Filtros independentes para esta seção
        areas_disp = _vals(df_ar, "area")
        c_sa1, c_sa2 = st.columns([1, 3])
        with c_sa1:
            area_sub_sel = st.selectbox(
                "Filtrar por Área Principal",
                ["Todas"] + areas_disp,
                key="sub_area_principal",
            )
        with c_sa2:
            subareas_disp = _vals(df_ar, "sigla_sub_area")
            subareas_sub_sel = st.multiselect(
                "Sub Área(s)", subareas_disp, default=subareas_disp,
                key="sub_area_filter",
            )

        df_sub = df_ar.copy()
        if area_sub_sel != "Todas":
            df_sub = df_sub[df_sub["area"] == area_sub_sel]
        if subareas_sub_sel:
            df_sub = df_sub[df_sub["sigla_sub_area"].isin(subareas_sub_sel)]

        ag_sub = (
            df_sub.groupby("sigla_sub_area")
            .agg(
                receita_liquida =("receita_liquida",  "sum"),
                receita_prevista=("receita_prevista", "sum"),
                atingimento_pct =("atingimento_pct",  "mean"),
            )
            .reset_index()
        )

        col_s1, col_s2 = st.columns(2)
        with col_s1:
            fig_sub = px.bar(
                ag_sub, x="sigla_sub_area", y="receita_liquida",
                color="sigla_sub_area", color_discrete_map=CORES_SUBAREA,
                text_auto=".2s",
                labels={
                    "receita_liquida": "Receita Líquida (R$)",
                    "sigla_sub_area": "Sub Área",
                },
            )
            fig_sub.update_layout(height=300, margin=dict(t=10, b=30))
            st.plotly_chart(fig_sub, use_container_width=True, key="ar_sub_bar")

        with col_s2:
            fig_sub_donut = px.pie(
                ag_sub, names="sigla_sub_area", values="receita_liquida",
                hole=0.5, color="sigla_sub_area",
                color_discrete_map=CORES_SUBAREA,
            )
            fig_sub_donut.update_layout(height=300, margin=dict(t=10, b=30))
            st.plotly_chart(fig_sub_donut, use_container_width=True, key="ar_sub_donut")

        tbl(ag_sub)

    # Tabela área × mês
    if "area" in df_ar.columns and "mes_ref" in df_ar.columns:
        sec("Tabela — Área × Mês")
        tbl_am = (
            df_ar.groupby(["area", df_ar["mes_ref"].dt.strftime("%b/%y").rename("Mês")])
            .agg(
                receita_liquida =("receita_liquida",  "sum"),
                receita_prevista=("receita_prevista", "sum"),
                custo_total     =("custo_total",      "sum"),
            )
            .reset_index()
        )
        tbl(tbl_am)


# ══════════════════════════════════════════════════════════════════════════════
# ABA 5 — PROJETOS
# ══════════════════════════════════════════════════════════════════════════════

with tab_proj:
    df_pr = filtros(df_op, com_area=True, com_projeto=True, com_data=True, sufixo="pr")

    if "projeto" not in df_pr.columns:
        st.info("Coluna 'projeto' não encontrada nos dados.")
    else:
        ag_proj = df_pr.groupby("projeto").agg(
            receita_liquida =("receita_liquida",  "sum"),
            receita_prevista=("receita_prevista", "sum"),
            custo_total     =("custo_total",      "sum"),
            desvio_abs      =("desvio_abs",       "sum"),
            atingimento_pct =("atingimento_pct",  "mean"),
        ).reset_index()
        ag_proj["desvio_pct"] = np.where(
            ag_proj["receita_prevista"] != 0,
            ag_proj["desvio_abs"] / ag_proj["receita_prevista"] * 100, np.nan,
        )
        if "area" in df_pr.columns:
            area_proj = (
                df_pr.groupby("projeto")["area"]
                .agg(lambda x: x.mode().iloc[0] if len(x) else "—")
                .reset_index()
            )
            ag_proj = ag_proj.merge(area_proj, on="projeto", how="left")

        sec("Ranking de Receita por Projeto")
        fig_rank = px.bar(
            ag_proj.sort_values("receita_liquida"),
            x="receita_liquida", y="projeto", orientation="h",
            color="area" if "area" in ag_proj.columns else "projeto",
            color_discrete_map=CORES_AREA, text_auto=".2s",
            labels={"receita_liquida": "Receita Líquida (R$)", "projeto": "Projeto",
                    "area": "Área"},
        )
        fig_rank.update_layout(height=420, margin=dict(t=10, b=30, l=10))
        st.plotly_chart(fig_rank, use_container_width=True, key="pr_rank")

        col_sc, col_tm = st.columns(2)
        with col_sc:
            sec("Atingimento × Desvio por Projeto")
            fig_scat = px.scatter(
                ag_proj, x="desvio_abs", y="atingimento_pct", text="projeto",
                color="area" if "area" in ag_proj.columns else "projeto",
                color_discrete_map=CORES_AREA, size="receita_liquida",
                labels={
                    "desvio_abs": "Desvio (R$)",
                    "atingimento_pct": "Atingimento (%)",
                    "area": "Área",
                },
            )
            fig_scat.add_vline(x=0, line_dash="dot", line_color="gray")
            fig_scat.add_hline(y=100, line_dash="dot", line_color=COR_DESVIO_NEG)
            fig_scat.update_traces(textposition="top center")
            fig_scat.update_layout(height=360, margin=dict(t=10, b=30))
            st.plotly_chart(fig_scat, use_container_width=True, key="pr_scatter")

        with col_tm:
            sec("Participação de Receita — Treemap")
            path_cols = ["area", "projeto"] if "area" in ag_proj.columns else ["projeto"]
            fig_tree = px.treemap(
                ag_proj, path=path_cols, values="receita_liquida",
                color="atingimento_pct",
                color_continuous_scale=[COR_DESVIO_NEG, "#FFC107", COR_DESVIO_POS],
                color_continuous_midpoint=100,
                hover_data={"atingimento_pct": ":.1f"},
            )
            fig_tree.update_layout(height=360, margin=dict(t=10, b=10))
            st.plotly_chart(fig_tree, use_container_width=True, key="pr_treemap")

        sec("Métricas por Projeto")
        tbl(ag_proj.sort_values("receita_liquida", ascending=False))


# ══════════════════════════════════════════════════════════════════════════════
# ABA 6 — DESVIOS & ALERTAS
# ══════════════════════════════════════════════════════════════════════════════

with tab_desvios:
    df_dv = filtros(df_op, com_area=True, com_projeto=True, com_data=True, sufixo="dv")

    if "projeto" in df_dv.columns:
        ag_dev = df_dv.groupby("projeto").agg(
            desvio_abs     =("desvio_abs",      "sum"),
            desvio_pct     =("desvio_pct",      "mean"),
            atingimento_pct=("atingimento_pct", "mean"),
            receita_liquida=("receita_liquida", "sum"),
        ).reset_index()
        if "area" in df_dv.columns:
            area_d = (
                df_dv.groupby("projeto")["area"]
                .agg(lambda x: x.mode().iloc[0] if len(x) else "—")
                .reset_index()
            )
            ag_dev = ag_dev.merge(area_d, on="projeto", how="left")

        sec("Ranking de Desvios por Projeto")
        ag_dev_sorted = ag_dev.sort_values("desvio_abs")
        fig_dv_bar = px.bar(
            ag_dev_sorted, x="desvio_abs", y="projeto", orientation="h",
            color="desvio_abs",
            color_continuous_scale=[[0, COR_DESVIO_NEG], [0.5, "#FFC107"], [1, COR_DESVIO_POS]],
            text=[fmt_brl(v) for v in ag_dev_sorted["desvio_abs"]],
            labels={"desvio_abs": "Desvio (R$)", "projeto": "Projeto"},
        )
        fig_dv_bar.add_vline(x=0, line_dash="dot", line_color="gray")
        fig_dv_bar.update_layout(height=420, margin=dict(t=10, b=30),
                                 coloraxis_showscale=False)
        fig_dv_bar.update_traces(textposition="outside")
        st.plotly_chart(fig_dv_bar, use_container_width=True, key="dv_rank")

        alertas = ag_dev[ag_dev["atingimento_pct"] < 90].sort_values("atingimento_pct")
        if not alertas.empty:
            sec(f"Projetos com Atingimento < 90%  ({len(alertas)} projeto(s))")
            for _, row in alertas.iterrows():
                css_cls = "alert-danger" if row["atingimento_pct"] < 70 else "alert-warn"
                st.markdown(
                    f'<div class="{css_cls}">🔴 <b>{row["projeto"]}</b> — '
                    f'Atingimento: <b>{row["atingimento_pct"]:.1f}%</b> | '
                    f'Desvio: <b>{fmt_brl(row["desvio_abs"])}</b></div>',
                    unsafe_allow_html=True,
                )
        else:
            st.success("Todos os projetos com atingimento ≥ 90%.")

    if all(c in df_dv.columns for c in ["allowance", "contingencia"]) and "area" in df_dv.columns:
        sec("Breakdown de Custos por Área — Allowance & Contingência")
        ag_custo = df_dv.groupby("area").agg(
            allowance   =("allowance",    "sum"),
            contingencia=("contingencia", "sum"),
        ).reset_index()
        fig_custo = px.bar(
            ag_custo.melt(id_vars="area", var_name="Tipo", value_name="Valor"),
            x="area", y="Valor", color="Tipo", barmode="stack",
            color_discrete_map={"allowance": "#4472C4", "contingencia": "#FF9800"},
            labels={"Valor": "R$", "area": "Área"},
        )
        fig_custo.update_layout(height=300, margin=dict(t=10, b=30))
        st.plotly_chart(fig_custo, use_container_width=True, key="dv_custo")

    if all(c in df_dv.columns for c in ["projeto", "mes_ref", "atingimento_pct"]):
        sec("Heatmap — Atingimento (%) por Projeto × Mês")
        df_heat = df_dv.copy()
        df_heat["Mês"] = df_heat["mes_ref"].dt.strftime("%b/%y")
        pivot = (
            df_heat.groupby(["projeto", "Mês"])["atingimento_pct"]
            .mean().reset_index()
            .pivot(index="projeto", columns="Mês", values="atingimento_pct")
        )
        if not pivot.empty:
            fig_heat = px.imshow(
                pivot,
                color_continuous_scale=[COR_DESVIO_NEG, "#FFC107", COR_DESVIO_POS],
                aspect="auto", zmin=60, zmax=140, text_auto=".0f",
                labels={"color": "Atingimento (%)"},
            )
            fig_heat.update_layout(height=380, margin=dict(t=10, b=30))
            st.plotly_chart(fig_heat, use_container_width=True, key="dv_heatmap")


# ══════════════════════════════════════════════════════════════════════════════
# ABA 7 — DADOS
# ══════════════════════════════════════════════════════════════════════════════

with tab_dados:
    df_dd = filtros(
        df_op,
        com_area=True, com_projeto=True, com_subarea=True,
        com_funcionario=True, com_data=True,
        sufixo="dd",
    )

    sec("Base Operacional Filtrada")
    st.caption(f"{len(df_dd):,} registros · {df_dd.shape[1]} colunas")

    todas_cols = df_dd.columns.tolist()
    # Nomes legíveis para o seletor
    nomes_legiv = {c: NOMES_COL.get(c, c) for c in todas_cols}
    cols_sel_legiv = st.multiselect(
        "Colunas visíveis",
        options=list(nomes_legiv.values()),
        default=list(nomes_legiv.values()),
        key="cols_dados",
    )
    # Mapeia de volta para nomes internos
    inv_map = {v: k for k, v in nomes_legiv.items()}
    cols_sel = [inv_map[c] for c in cols_sel_legiv if c in inv_map]
    df_vis = df_dd[cols_sel] if cols_sel else df_dd

    tbl(df_vis, height=460)

    sec("Estatísticas Descritivas")
    num_cols = df_dd.select_dtypes(include="number").columns.tolist()
    if num_cols:
        desc = df_dd[num_cols].describe().T.round(2)
        desc.index = [NOMES_COL.get(i, i) for i in desc.index]
        st.dataframe(desc, use_container_width=True)

    csv = df_vis.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇️ Baixar CSV filtrado",
        data=csv,
        file_name="base_operacional_filtrada.csv",
        mime="text/csv",
    )


# ══════════════════════════════════════════════════════════════════════════════
# ABA 8 — DICIONÁRIO DE DADOS
# ══════════════════════════════════════════════════════════════════════════════

with tab_dicionario:
    sec("Dicionário de Dados — Arquivo 1  (Base Operacional: Funcionário / Projeto / Quinzena)")
    st.caption(
        "Granularidade: uma linha por funcionário × projeto × quinzena × mês. "
        "É a base principal usada em todos os gráficos do dashboard."
    )

    dic_arq1 = pd.DataFrame([
        {
            "Campo":       "Ajuste",
            "Tipo":        "Texto",
            "Obrigatório": "Não",
            "Descrição":   (
                "Forma de reconhecer uma receita no projeto que não vem por débito de horas. "
                "Campo opcional — recomenda-se manter uma lista padronizada de justificativas."
            ),
        },
        {
            "Campo":       "Allowance",
            "Tipo":        "Numérico (R$)",
            "Obrigatório": "Sim",
            "Descrição":   (
                "Lançamentos contábeis que refletem a receita que será estornada no mês seguinte. "
                "Deve ser ≥ 0 e na mesma moeda que Receita Prevista."
            ),
        },
        {
            "Campo":       "Funcionário",
            "Tipo":        "Texto",
            "Obrigatório": "Sim",
            "Descrição":   "Identificação do tipo de colaborador relacionado à linha de receita/projeto.",
        },
        {
            "Campo":       "Centro de Custo",
            "Tipo":        "Texto (código)",
            "Obrigatório": "Sim",
            "Descrição":   "Centro de custo responsável pelo lançamento, alocação ou receita associada.",
        },
        {
            "Campo":       "Projeto",
            "Tipo":        "Texto",
            "Obrigatório": "Sim",
            "Descrição":   (
                "Identificação do projeto ao qual a receita, allowance ou contingência se refere. "
                "Recomenda-se usar um código único de projeto."
            ),
        },
        {
            "Campo":       "Contingência",
            "Tipo":        "Numérico (R$)",
            "Obrigatório": "Sim",
            "Descrição":   (
                "Lançamentos contábeis que refletem a receita que será estornada no mês seguinte. "
                "Deve ser ≥ 0."
            ),
        },
        {
            "Campo":       "Área",
            "Tipo":        "Texto",
            "Obrigatório": "Sim",
            "Descrição":   "Área organizacional responsável pelo recurso ou receita.",
        },
        {
            "Campo":       "Mês/Ano",
            "Tipo":        "Data (período mensal)",
            "Obrigatório": "Sim",
            "Descrição":   "Período de competência mensal da receita. Formato recomendado: YYYY-MM.",
        },
        {
            "Campo":       "ID Quinzena",
            "Tipo":        "Inteiro",
            "Obrigatório": "Sim",
            "Descrição":   "Identifica se os valores correspondem à 1ª ou 2ª quinzena do mês. Domínio fixo: {1, 2}.",
        },
        {
            "Campo":       "Receita Prevista",
            "Tipo":        "Numérico (R$)",
            "Obrigatório": "Sim",
            "Descrição":   "Receita orçada ou estimada para o período e contexto da linha. Deve ser ≥ 0.",
        },
        {
            "Campo":       "Receita Líquida",
            "Tipo":        "Numérico (R$)",
            "Obrigatório": "Sim",
            "Descrição":   (
                "Receita realizada após deduções, ajustes ou descontos. "
                "Geralmente menor ou igual à Receita Prevista."
            ),
        },
        {
            "Campo":       "Sigla Subárea",
            "Tipo":        "Texto",
            "Obrigatório": "Não",
            "Descrição":   "Sigla da subárea dentro da área principal.",
        },
    ])
    tbl(dic_arq1)

    st.markdown("")
    sec("Dicionário de Dados — Arquivo 2  (Orçamento por Área / Tipo / Mês)")
    st.caption(
        "Granularidade: uma linha por área × tipo de valor. "
        "As colunas de período (jun/25 a mai/26) representam o horizonte orçamentário anual."
    )

    dic_arq2 = pd.DataFrame([
        {
            "Campo":       "Área",
            "Tipo":        "Texto",
            "Obrigatório": "Sim",
            "Descrição":   "Área de negócio responsável pelo planejamento dos valores orçamentários.",
        },
        {
            "Campo":       "Type / Tipo",
            "Tipo":        "Texto (domínio controlado)",
            "Obrigatório": "Sim",
            "Descrição":   (
                "Classificação do tipo de valor lançado para aquele período "
                "(ex.: Receita Prevista, Allowance, Contingência)."
            ),
        },
        {
            "Campo":       "Colunas de Período  (jun/25 … mai/26)",
            "Tipo":        "Numérico (R$)",
            "Obrigatório": "Sim",
            "Descrição":   (
                "Cada coluna representa um mês do horizonte orçamentário anual. "
                "Valores devem ser ≥ 0. Internamente são convertidos para o formato YYYY-MM."
            ),
        },
    ])
    tbl(dic_arq2)

    st.markdown("")
    sec("Métricas Calculadas (campos derivados usados no dashboard)")
    dic_calc = pd.DataFrame([
        {
            "Métrica":   "Desvio Absoluto",
            "Fórmula":   FORMULA["desvio_abs"],
            "Descrição": "Diferença em R$ entre o que foi realizado e o que estava previsto.",
        },
        {
            "Métrica":   "Desvio (%)",
            "Fórmula":   FORMULA["desvio_pct"],
            "Descrição": "Variação percentual da receita realizada em relação à prevista.",
        },
        {
            "Métrica":   "Atingimento (%)",
            "Fórmula":   FORMULA["atingimento_pct"],
            "Descrição": "Percentual de execução da receita. Meta de referência: 100%.",
        },
        {
            "Métrica":   "Receita Ajustada",
            "Fórmula":   FORMULA["receita_ajustada"],
            "Descrição": "Receita Líquida deduzida de allowances e contingências.",
        },
        {
            "Métrica":   "Custo Total",
            "Fórmula":   FORMULA["custo_total"],
            "Descrição": "Soma dos lançamentos de allowance e contingência.",
        },
        {
            "Métrica":   "Margem (%)",
            "Fórmula":   FORMULA["margem_pct"],
            "Descrição": "Margem percentual em relação à receita orçada.",
        },
    ])
    tbl(dic_calc)
