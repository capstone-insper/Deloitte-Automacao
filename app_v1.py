"""
Dashboard Executivo — Deloitte
==============================
Lê diretamente os arquivos .txt da pasta entrada/ e gera
um dashboard executivo interativo sem dependência de ETL prévio.

Para rodar:
    streamlit run app_v1.py
    ou
    python -m streamlit run app_v1.py
"""
from __future__ import annotations
import hashlib
import html
import os
import re
import unicodedata
import warnings
from pathlib import Path
from urllib.parse import urlencode
from kpi_agent import (render_kpi_agent, _render_sidebar, _init_state,
                        carregar_insights, remover_insight,
                        _render_grafico, _render_tabela, _render_comparacao)
from database import verify_user, create_db, add_user, is_user_master, set_user_as_master, list_all_users, delete_user, user_exists

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

warnings.filterwarnings("ignore")

from dotenv import load_dotenv
load_dotenv()

# Inicializar banco de dados
create_db()

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÃO DA PÁGINA
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Dashboard Executivo — Deloitte",
    layout="wide",
)

# ─────────────────────────────────────────────────────────────────────────────
# AUTENTICAÇÃO
# ─────────────────────────────────────────────────────────────────────────────

import sqlite3

AUTH_SECRET = os.getenv("AUTH_SECRET")
if not AUTH_SECRET:
    raise RuntimeError("AUTH_SECRET não definido no .env — a aplicação não pode iniciar sem ele.")


def _make_login_token(username: str) -> str:
    return hashlib.sha256(f"{username}|{AUTH_SECRET}".encode("utf-8")).hexdigest()


def _get_saved_login() -> str | None:
    try:
        params = st.query_params
        username = params.get("user", "")
        token = params.get("token", "")
        if username and token == _make_login_token(username):
            return username
    except Exception:
        pass
    return None


def _set_saved_login(username: str | None):
    try:
        if username:
            st.query_params["user"] = username
            st.query_params["token"] = _make_login_token(username)
        else:
            if "user" in st.query_params:
                del st.query_params["user"]
            if "token" in st.query_params:
                del st.query_params["token"]
    except Exception:
        pass


if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.username = None
    st.session_state.is_master = False

if 'auth_page' not in st.session_state:
    st.session_state.auth_page = "login"

if not st.session_state.logged_in:
    saved_user = _get_saved_login()
    if saved_user:
        st.session_state.logged_in = True
        st.session_state.username = saved_user
        st.session_state.is_master = is_user_master(saved_user)

if not st.session_state.logged_in:
    if st.session_state.auth_page == "register":
        st.title("Criar Usuário — Deloitte")
        
        # Apenas masters podem criar usuários
        if not st.session_state.is_master:
            st.error("❌ Apenas administradores (Masters) podem criar novos usuários.")
            if st.button("Voltar ao Login", use_container_width=True):
                st.session_state.auth_page = "login"
                st.rerun()
            st.stop()
        
        st.info(f"✓ Logado como: **{st.session_state.username}** (Admin Master)")
        
        with st.form("create_user_form"):
            new_username = st.text_input("Nome do novo usuário")
            new_password = st.text_input("Senha inicial", type="password")
            confirm_password = st.text_input("Confirmar Senha", type="password")
            make_master = st.checkbox("Tornar este usuário como Master também?", value=False)
            
            col_reg, col_back = st.columns(2)
            with col_reg:
                if st.form_submit_button("Criar Usuário", use_container_width=True):
                    if not new_username.strip():
                        st.error("O nome de usuário não pode ser vazio.")
                    elif len(new_password) < 4:
                        st.error("A senha deve ter pelo menos 4 caracteres.")
                    elif new_password != confirm_password:
                        st.error("As senhas não coincidem.")
                    elif add_user(new_username.strip(), new_password):
                        # Se deve ser master, atualizar no banco de dados
                        if make_master:
                            set_user_as_master(new_username.strip(), True)
                            st.success(f"Usuário '{new_username.strip()}' criado com sucesso como **Master**!")
                        else:
                            st.success(f"Usuário '{new_username.strip()}' criado com sucesso!")
                        st.session_state.auth_page = "login"
                        st.rerun()
                    else:
                        st.error(f"O usuário '{new_username.strip()}' já existe. Escolha outro nome.")
            with col_back:
                if st.form_submit_button("Voltar ao Login", use_container_width=True):
                    st.session_state.auth_page = "login"
                    st.rerun()
    else:
        st.title("Login - Dashboard Executivo — Deloitte")
        
        with st.form("login_form"):
            username = st.text_input("Usuário")
            password = st.text_input("Senha", type="password")
            
            # Se um master estiver logado, mostrar 2 colunas; senão, apenas 1
            if st.session_state.is_master and st.session_state.logged_in:
                col_login, col_new = st.columns(2)
            else:
                col_login = st.columns(1)[0]
            
            with col_login:
                if st.form_submit_button("Entrar", use_container_width=True):
                    if verify_user(username, password):
                        st.session_state.logged_in = True
                        st.session_state.username = username
                        st.session_state.is_master = is_user_master(username)
                        _set_saved_login(username)
                        st.success("Login realizado com sucesso!")
                        st.rerun()
                    else:
                        st.error("Usuário ou senha incorretos.")
            
            # Botão de criar usuário aparece apenas para masters já logados
            if st.session_state.is_master and st.session_state.logged_in:
                with col_new:
                    if st.form_submit_button("Criar Usuário (Admin)", use_container_width=True):
                        st.session_state.auth_page = "register"
                        st.rerun()
    st.stop()  # Para a execução se não logado

# ─────────────────────────────────────────────────────────────────────────────
# MENU DE ADMINISTRAÇÃO PARA MASTERS
# ─────────────────────────────────────────────────────────────────────────────

_ADMIN_CSS = """
<style>
/* Expander header */
[data-testid="stSidebar"] [data-testid="stExpander"] summary {
    font-size: 11px !important; color: #505870 !important;
    padding: 5px 8px !important;
}
[data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stVerticalBlockBorderWrapper"] {
    padding-top: 0 !important; padding-bottom: 0 !important;
}
/* Remove padding extra das linhas de coluna */
[data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stHorizontalBlock"] {
    gap: 4px !important; align-items: center !important;
    padding-bottom: 0 !important; margin-bottom: 0 !important;
}
/* Botão Deletar — texto vermelho sublinhado, sem borda nem fundo */
[data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="baseButton-secondary"] {
    font-size: 12px !important;
    padding: 0 !important;
    min-height: 0 !important; height: auto !important;
    line-height: 1.4 !important;
    color: #c03030 !important;
    background: transparent !important;
    border: none !important;
    box-shadow: none !important;
    font-weight: 400 !important;
    width: auto !important;
    display: inline-flex !important;
    text-decoration: underline !important;
    cursor: pointer !important;
}
[data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="baseButton-secondary"]:hover {
    color: #e04040 !important;
    background: transparent !important;
    border: none !important;
    box-shadow: none !important;
}
/* Badges */
.adm-badge-m {
    font-size: 9px; color: #c8880a;
    background: rgba(200,136,10,0.12); border: 1px solid rgba(200,136,10,0.25);
    border-radius: 10px; padding: 1px 6px; font-weight: 600;
}
.adm-badge-u {
    font-size: 9px; color: #383e50;
    background: rgba(255,255,255,0.02); border: 1px solid #1a1e2e;
    border-radius: 10px; padding: 1px 5px;
}
/* Cabeçalho de seção */
.adm-section {
    font-size: 9px; color: #3a4050; text-transform: uppercase;
    letter-spacing: 1px; font-weight: 600;
    margin: 10px 0 2px 0; border-bottom: 1px solid #14161e; padding-bottom: 3px;
}
</style>
"""

def renderizar_admin_panel():
    """Renderiza o painel de administração para masters."""
    if not st.session_state.is_master:
        return

    with st.sidebar:
        st.markdown(_ADMIN_CSS, unsafe_allow_html=True)
        with st.expander("⚙️  Admin", expanded=False):
            users = list_all_users()

            # ── Lista de usuários ──────────────────────────────────────
            st.markdown('<div class="adm-section">Usuários</div>', unsafe_allow_html=True)
            for user in users:
                col_name, col_del = st.columns([4, 2], gap="small")
                with col_name:
                    badge_html = (
                        f' <span class="adm-badge-m">master</span>'
                        if user['is_master'] else ''
                    )
                    st.markdown(
                        f'<p style="margin:0;padding:10px 2px;font-size:13px;color:#7a8aa0;line-height:1.2">'
                        f'{user["username"]}{badge_html}</p>',
                        unsafe_allow_html=True,
                    )
                with col_del:
                    if user['username'] != st.session_state.username:
                        if st.button("Deletar", key=f"del_{user['username']}"):
                            if delete_user(user['username']):
                                st.rerun()

            # ── Promover a master ──────────────────────────────────────
            non_masters = [u['username'] for u in users if not u['is_master']]
            if non_masters:
                st.markdown('<div class="adm-section" style="margin-top:18px">Promover a master</div>', unsafe_allow_html=True)
                sel = st.selectbox("", non_masters, key="promote_user_select", label_visibility="collapsed")
                if st.button("↑ Promover", key="promote_button", use_container_width=True, type="primary"):
                    if set_user_as_master(sel, True):
                        st.rerun()

            # ── Remover master ─────────────────────────────────────────
            other_masters = [u['username'] for u in users
                             if u['is_master'] and u['username'] != st.session_state.username]
            if other_masters:
                st.markdown('<div class="adm-section" style="margin-top:18px">Remover master</div>', unsafe_allow_html=True)
                sel_r = st.selectbox("", other_masters, key="remove_master_select", label_visibility="collapsed")
                if st.button("↓ Remover", key="remove_master_btn", use_container_width=True, type="primary"):
                    if set_user_as_master(sel_r, False):
                        st.rerun()

renderizar_admin_panel()

# ─────────────────────────────────────────────────────────────────────────────
# CABEÇALHO COM INFORMAÇÕES DO USUÁRIO
# ─────────────────────────────────────────────────────────────────────────────

col_header_left, col_header_right = st.columns([1, 0.15])
with col_header_left:
    st.markdown(f"### Bem-vindo, **{st.session_state.username}**")
with col_header_right:
    if st.button("🚪 Sair", use_container_width=True):
        st.session_state.logged_in = False
        st.session_state.username = None
        st.session_state.is_master = False
        _set_saved_login(None)
        st.rerun()

st.markdown("---")

ROOT = Path(__file__).resolve().parent
PASTA_ENTRADA = ROOT / "entrada"
ENCODINGS = ["utf-16", "utf-16-le", "utf-8-sig", "utf-8", "latin1"]

# Inicializar estado do agente KPI
from kpi_agent import _init_state
_init_state()

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
/* Navegação customizada com aparência de abas do Streamlit.
   Usa botões, não links, para não abrir uma nova guia no navegador. */
.dlt-tabbar-line {
    border-bottom: 1px solid rgba(49, 51, 63, 0.20);
    margin-top: -0.25rem;
    margin-bottom: 1.1rem;
}
/* Em versões recentes do Streamlit, widgets com key recebem uma classe st-key-... .
   O seletor abaixo limita o visual de abas apenas aos botões da navegação. */
div[class*="st-key-navtab_"] button {
    min-height: 2.75rem;
    padding: 0.75rem 0.55rem 0.65rem 0.55rem;
    border: 0 !important;
    border-radius: 0 !important;
    border-bottom: 2px solid transparent !important;
    background: transparent !important;
    color: rgba(49, 51, 63, 0.78) !important;
    box-shadow: none !important;
    font-size: 0.92rem;
    font-weight: 400;
    white-space: nowrap;
}
div[class*="st-key-navtab_"] button:hover {
    color: #012169 !important;
    background: rgba(49, 51, 63, 0.04) !important;
}
div[class*="st-key-navtab_"] button[kind="primary"] {
    color: #012169 !important;
    border-bottom: 2px solid #86BC25 !important;
    font-weight: 600 !important;
}
div[class*="st-key-navtab_"] button:focus,
div[class*="st-key-navtab_"] button:active {
    box-shadow: none !important;
    outline: none !important;
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
    for c in df.columns:
        if c not in cfg:
            cfg[c] = st.column_config.TextColumn(rotulo_coluna(c))
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


def rotulo_coluna(nome: str) -> str:
    nome = str(nome).strip()
    if nome in NOMES_COL:
        return NOMES_COL[nome]
    return " ".join(word.capitalize() for word in nome.split("_"))


def rotulos_para(colunas: list[str]) -> dict[str, str]:
    return {c: rotulo_coluna(c) for c in colunas}


def rotular_fig(fig):
    for trace in fig.data:
        if trace.name in NOMES_COL:
            trace.name = NOMES_COL[trace.name]
        if getattr(trace, "legendgroup", None) in NOMES_COL:
            trace.legendgroup = NOMES_COL[trace.legendgroup]
    return fig


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

    OBS: o filtro de período por data foi substituído pelo seletor global de
    Ano Fiscal (sidebar). O parâmetro `com_data` é mantido por compatibilidade,
    mas agora apenas exibe um pequeno aviso indicando o FY ativo.
    """
    # exibe aviso de período ativo se solicitado
    if com_data:
        modo = st.session_state.get("modo_periodo", "fy")
        if modo == "fy":
            fy_atual = st.session_state.get("fy_sel")
            posicoes = st.session_state.get("fy_posicoes") or []
            if fy_atual is not None and posicoes:
                meses_str = ", ".join(MESES_FY[p - 1] for p in posicoes)
                st.caption(
                    f"Período: **{fy_label(fy_atual)}** "
                    f"({len(posicoes)} mês{'es' if len(posicoes) != 1 else ''}: "
                    f"{meses_str}) · comparações vs **{fy_label(fy_atual - 1)}** "
                    f"(mesmos meses)"
                )
        else:
            st.caption("Período: **Histórico completo** (sem filtro temporal)")

    n_filtros = sum([com_area, com_projeto, com_subarea, com_funcionario])
    if n_filtros == 0:
        return df

    cols = st.columns(n_filtros)
    idx  = 0
    areas = projetos = subareas = funcs = None

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

    d = df.copy()
    if areas    is not None and "area"           in d.columns: d = d[d["area"].isin(areas)]
    if projetos is not None and "projeto"        in d.columns: d = d[d["projeto"].isin(projetos)]
    if subareas is not None and "sigla_sub_area" in d.columns: d = d[d["sigla_sub_area"].isin(subareas)]
    if funcs    is not None and "funcionario"    in d.columns: d = d[d["funcionario"].isin(funcs)]
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


# ─────────────────────────────────────────────────────────────────────────────
# ANO FISCAL DELOITTE  (FY = Junho → Maio)
# Ex.: FY26 = Jun/2025 → Mai/2026
# ─────────────────────────────────────────────────────────────────────────────

# Nome dos meses na ordem do FY (Jun = posição 1, Mai = posição 12)
MESES_FY: list[str] = [
    "Jun", "Jul", "Ago", "Set", "Out", "Nov",
    "Dez", "Jan", "Fev", "Mar", "Abr", "Mai",
]


def fy_de_data(d) -> int:
    """Retorna o Ano Fiscal Deloitte (FY) ao qual a data pertence."""
    d = pd.Timestamp(d)
    return d.year + 1 if d.month >= 6 else d.year


def fy_intervalo(fy: int) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Retorna (inicio, fim) do FY: 1º Jun do ano (fy-1) até último dia de Mai do ano fy."""
    inicio = pd.Timestamp(year=fy - 1, month=6, day=1)
    fim = pd.Timestamp(year=fy, month=5, day=31)
    return inicio, fim


def fy_disponiveis(df: pd.DataFrame) -> list[int]:
    """Lista FYs presentes nos dados (decrescente)."""
    if "mes_ref" not in df.columns or df.empty:
        return []
    fys = df["mes_ref"].dropna().apply(fy_de_data).unique().tolist()
    return sorted(fys, reverse=True)


def fy_label(fy: int) -> str:
    """Rótulo legível: FY26 (Jun/25 – Mai/26)."""
    yy_ini = f"{(fy - 1) % 100:02d}"
    yy_fim = f"{fy % 100:02d}"
    return f"FY{yy_fim} (Jun/{yy_ini} – Mai/{yy_fim})"


def pos_no_fy(d) -> int:
    """Posição do mês dentro do FY (Jun=1, Jul=2, …, Mai=12)."""
    d = pd.Timestamp(d)
    return ((d.month - 6) % 12) + 1


def aplicar_fy(df: pd.DataFrame, fy: int, posicoes: list[int] | None = None) -> pd.DataFrame:
    """Filtra df para o FY especificado.
    Se `posicoes` for fornecido, mantém apenas os meses cujas posições no FY
    estão na lista (Jun=1, Mai=12). Se None, mantém o FY inteiro."""
    if "mes_ref" not in df.columns or df.empty:
        return df.iloc[0:0].copy() if not df.empty else df
    inicio, fim = fy_intervalo(fy)
    d = df[(df["mes_ref"] >= inicio) & (df["mes_ref"] <= fim)].copy()
    if posicoes is not None and not d.empty:
        d = d[d["mes_ref"].apply(pos_no_fy).isin(posicoes)]
    return d


def posicoes_disponiveis_no_fy(df: pd.DataFrame, fy: int) -> list[int]:
    """Posições (1..12) com dados no FY informado, em ordem cronológica do FY."""
    if "mes_ref" not in df.columns or df.empty:
        return []
    sub = aplicar_fy(df, fy)
    if sub.empty:
        return []
    pos = sub["mes_ref"].dropna().apply(pos_no_fy).unique().tolist()
    return sorted(pos)


def n_meses_no_fy(df: pd.DataFrame, fy: int) -> int:
    """Quantidade de meses distintos com dados no FY informado."""
    if "mes_ref" not in df.columns or df.empty:
        return 0
    sub = aplicar_fy(df, fy)
    return int(sub["mes_ref"].dropna().nunique())


def selecoes_de_sufixo(sufixo: str) -> dict:
    """Lê do session_state as seleções dos filtros associados ao sufixo da aba."""
    sels: dict = {}
    for chave, col in [
        (f"fa_{sufixo}", "area"),
        (f"fp_{sufixo}", "projeto"),
        (f"fs_{sufixo}", "sigla_sub_area"),
        (f"ff_{sufixo}", "funcionario"),
    ]:
        v = st.session_state.get(chave)
        if v is not None:
            sels[col] = v
    return sels


def aplicar_selecoes(df: pd.DataFrame, selecoes: dict) -> pd.DataFrame:
    """Aplica filtros de seleção (multiselect) a um DataFrame."""
    d = df.copy()
    for col, vals in selecoes.items():
        if col in d.columns and vals is not None:
            d = d[d[col].isin(vals)]
    return d


def delta_fy(df_full: pd.DataFrame, sufixo: str, col: str) -> tuple[float | None, str, str]:
    """
    Calcula variação % do FY atual vs FY anterior, usando as MESMAS posições de
    meses (Jun=1 … Mai=12) selecionadas pelo usuário e respeitando os filtros
    (area/projeto/subarea/funcionario) da aba.
    Retorna (None, "", "") quando o modo for "Histórico completo" ou faltar dado.
    """
    if st.session_state.get("modo_periodo") != "fy":
        return None, "", ""
    fy_atual = st.session_state.get("fy_sel")
    posicoes = st.session_state.get("fy_posicoes") or []
    if fy_atual is None or not posicoes:
        return None, "", ""
    sels = selecoes_de_sufixo(sufixo)
    df_a = aplicar_selecoes(aplicar_fy(df_full, fy_atual, posicoes), sels)
    df_p = aplicar_selecoes(aplicar_fy(df_full, fy_atual - 1, posicoes), sels)
    v_a = df_a[col].sum() if col in df_a.columns else 0
    v_p = df_p[col].sum() if col in df_p.columns else 0
    lbl_a = f"FY{fy_atual % 100:02d}"
    lbl_p = f"FY{(fy_atual - 1) % 100:02d}"
    if v_p == 0:
        return None, lbl_a, lbl_p
    return (v_a - v_p) / abs(v_p) * 100, lbl_a, lbl_p


def fmt_brl(v, dec=0) -> str:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "—"
    s = f"{v:,.{dec}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"


# ─────────────────────────────────────────────────────────────────────────────
# CARREGA DADOS
# ─────────────────────────────────────────────────────────────────────────────

df_op_full, df_orc = carregar_dados()

if df_op_full.empty:
    st.error("Não foi possível carregar os dados. Verifique a pasta `entrada/`.")
    st.stop()

# ─────────────────────────────────────────────────────────────────────────────
# TÍTULO + PERÍODO DE ANÁLISE (controles globais no topo do dashboard)
# ─────────────────────────────────────────────────────────────────────────────

st.title("Dashboard Executivo — Deloitte")

if st.session_state.is_master:
    if st.button("⚙️ Gerenciar Usuários", use_container_width=True):
        st.session_state.auth_page = "admin"
        st.rerun()


fy_opts = fy_disponiveis(df_op_full)

# Defaults
fy_sel: int | None = None
posicoes_sel: list[int] = []
modo_periodo = "fy"  # "fy" ou "historico"

with st.container(border=True):
    st.markdown(
        "**Período de análise** &nbsp;·&nbsp; "
        "<span style='color:#666;font-size:12px;'>Calendário Deloitte: "
        "Junho → Maio (ex.: FY26 = Jun/2025 a Mai/2026)</span>",
        unsafe_allow_html=True,
    )

    if not fy_opts:
        st.warning("Nenhum dado temporal disponível.")
        modo_periodo = "historico"
    else:
        col_modo, col_fy, col_meses = st.columns([1.2, 1, 3.2])

        with col_modo:
            modo_lbl = st.radio(
                "Modo",
                ["Ano Fiscal específico", "Histórico completo"],
                key="modo_periodo_lbl",
                help=(
                    "**Ano Fiscal específico**: filtra o dashboard para um FY e os meses "
                    "escolhidos, comparando automaticamente com os mesmos meses do FY anterior.\n\n"
                    "**Histórico completo**: mostra todos os dados disponíveis sem filtro temporal "
                    "e desativa as comparações de FY."
                ),
            )
            modo_periodo = "fy" if modo_lbl.startswith("Ano Fiscal") else "historico"

        if modo_periodo == "fy":
            with col_fy:
                fy_sel = st.selectbox(
                    "Ano fiscal",
                    fy_opts,
                    format_func=fy_label,
                    index=0,
                    key="fy_sel_widget",
                    help="Lista todos os FYs com dados disponíveis (mais recente primeiro).",
                )

            pos_disp = posicoes_disponiveis_no_fy(df_op_full, fy_sel)
            opcoes_meses = [(p, MESES_FY[p - 1]) for p in pos_disp]
            labels_disp = [nome for _, nome in opcoes_meses]

            with col_meses:
                sel_labels = st.multiselect(
                    "Meses do FY (Jun = 1º mês)",
                    options=labels_disp,
                    default=labels_disp,
                    key="meses_sel_widget",
                    help=(
                        "Selecione os meses do FY que deseja analisar. A comparação "
                        "automática usará os MESMOS meses do FY anterior."
                    ),
                )
            posicoes_sel = [p for (p, lbl) in opcoes_meses if lbl in sel_labels]
        else:
            with col_fy:
                min_d = df_op_full["mes_ref"].dropna().min()
                max_d = df_op_full["mes_ref"].dropna().max()
                if pd.notna(min_d) and pd.notna(max_d):
                    st.metric(
                        "Histórico disponível",
                        f"{min_d.strftime('%b/%y')} → {max_d.strftime('%b/%y')}",
                    )
            with col_meses:
                st.info(
                    "Mostrando **todo o histórico** disponível. "
                    "Comparações de FY ficam desativadas neste modo."
                )

# Persiste no session_state para uso pelas funções de delta e filtros
st.session_state["modo_periodo"] = modo_periodo
st.session_state["fy_sel"] = fy_sel
st.session_state["fy_posicoes"] = posicoes_sel

# Aplica o filtro global ao DataFrame operacional usado pelas abas.
# df_op_full permanece disponível para o cálculo das comparações (FY anterior).
if modo_periodo == "fy" and fy_sel is not None and posicoes_sel:
    df_op = aplicar_fy(df_op_full, fy_sel, posicoes_sel)
else:
    df_op = df_op_full.copy()

# Variáveis auxiliares para textos e overlays nas abas
n_meses_fy = len(posicoes_sel) if modo_periodo == "fy" else 0

# Resumo do que está ativo, logo abaixo dos controles
if modo_periodo == "fy" and fy_sel is not None and posicoes_sel:
    meses_str = ", ".join(MESES_FY[p - 1] for p in posicoes_sel)
    st.caption(
        f"Visão de **{fy_label(fy_sel)}** ({n_meses_fy} mês"
        f"{'es' if n_meses_fy != 1 else ''}: {meses_str}) · "
        f"comparações vs **{fy_label(fy_sel - 1)}** (mesmos meses)"
    )
elif modo_periodo == "fy" and not posicoes_sel:
    st.warning("Selecione pelo menos um mês do FY para visualizar os dados.")

if df_op.empty and modo_periodo == "fy" and posicoes_sel:
    st.warning("Não há dados para o FY/meses selecionados.")

st.markdown("---")

# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR COM HISTÓRICO DO ASSISTENTE
# ─────────────────────────────────────────────────────────────────────────────

_init_state()
_render_sidebar()

# Compatibilidade com cliques vindos do histórico do assistente.
# Importante: não renderizar o assistente antes das abas e não usar st.stop(),
# porque isso escondia o dashboard quando uma nova conversa era criada.
if st.session_state.get("_jump_to_assistant", False):
    st.session_state._jump_to_assistant = False
    st.session_state["active_dashboard_tab"] = "Assistente Deloitte"
    try:
        st.query_params["tab"] = "Assistente Deloitte"
    except Exception:
        pass

# ─────────────────────────────────────────────────────────────────────────────
# ABAS COM APARÊNCIA ORIGINAL E ESTADO PERSISTENTE
# ─────────────────────────────────────────────────────────────────────────────
# Observação: st.tabs() puro não permite controlar a aba ativa depois de um
# st.rerun(). Como o chat precisa fazer rerun, usamos botões estilizados como
# abas. Assim, o clique muda a aba na mesma guia do navegador e o estado fica
# preservado em st.session_state.

TAB_LABELS = [
    "Resumo",
    "KPIs Executivos",
    "Série Temporal",
    "Áreas",
    "Projetos",
    "Desvios & Alertas",
    "Dados",
    "Dicionário",
    "Meus Insights",
    "Assistente Deloitte",
]

query_tab = st.query_params.get("tab", "")
if isinstance(query_tab, list):
    query_tab = query_tab[0] if query_tab else ""

if query_tab in TAB_LABELS:
    selected_tab = query_tab
elif st.session_state.get("active_dashboard_tab") in TAB_LABELS:
    selected_tab = st.session_state["active_dashboard_tab"]
else:
    selected_tab = TAB_LABELS[0]

st.session_state["active_dashboard_tab"] = selected_tab

# Mantém o parâmetro de aba na URL para que qualquer rerun preserve a aba ativa.
try:
    st.query_params["tab"] = selected_tab
except Exception:
    pass

def _tab_key(label: str) -> str:
    slug = unicodedata.normalize("NFKD", label).encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", slug).strip("_").lower()
    return f"navtab_{slug or 'aba'}"

def _select_tab(label: str) -> None:
    st.session_state["active_dashboard_tab"] = label
    try:
        st.query_params["tab"] = label
    except Exception:
        pass

# Botões em colunas: não usam href, então não abrem nova guia.
# O CSS acima deixa esses botões com aparência próxima às abas originais.
tab_cols = st.columns([1.0, 1.35, 1.25, 0.85, 0.95, 1.35, 0.75, 0.95, 1.25, 1.6], gap="small")
for col, label in zip(tab_cols, TAB_LABELS):
    with col:
        if st.button(
            label,
            key=_tab_key(label),
            type="primary" if label == selected_tab else "secondary",
            use_container_width=True,
        ):
            _select_tab(label)
            st.rerun()

st.markdown('<div class="dlt-tabbar-line"></div>', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# ABA 1 — RESUMO
# ══════════════════════════════════════════════════════════════════════════════

if selected_tab == "Resumo":
    df_res = filtros(df_op, com_data=True, sufixo="res")

    rl_tot = df_res["receita_liquida"].sum()
    rp_tot = df_res["receita_prevista"].sum()
    ct_tot = df_res["custo_total"].sum()
    dev    = rl_tot - rp_tot
    ating  = (rl_tot / rp_tot * 100) if rp_tot else 0.0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Receita Líquida",   fmt_brl(rl_tot),
              delta=_fmt_delta(delta_fy(df_op_full, "res", "receita_liquida")),
              help="Soma da Receita Líquida no Ano Fiscal selecionado.")
    c2.metric("Receita Prevista",  fmt_brl(rp_tot),
              delta=_fmt_delta(delta_fy(df_op_full, "res", "receita_prevista")),
              help="Soma da Receita Prevista no Ano Fiscal selecionado.")
    c3.metric("Desvio",            fmt_brl(dev),
              delta=_fmt_delta(delta_fy(df_op_full, "res", "desvio_abs")),
              help=FORMULA["desvio_abs"])
    c4.metric("Atingimento",       f"{ating:.1f}%",
              help=FORMULA["atingimento_pct"])
    c5.metric("Custo Total",       fmt_brl(ct_tot),
              delta=_fmt_delta(delta_fy(df_op_full, "res", "custo_total"), inverso=True),
              help=FORMULA["custo_total"])

    st.markdown("")
    col_g, col_d = st.columns(2)

    with col_g:
        sec("Atingimento Geral de Receita (Realizada / Prevista)")
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
        sec("Composição da Receita Líquida por Área")
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
        sec("Top 5 Projetos por Receita Líquida")
        if "projeto" in df_res.columns:
            top5 = (
                df_res.groupby("projeto")["receita_liquida"].sum()
                .nlargest(5).reset_index()
            )
            tbl(top5)

    with col_bot:
        sec("Top 5 Projetos com Maior Desvio Negativo (Realizado − Previsto)")
        if "projeto" in df_res.columns:
            bot5 = (
                df_res.groupby("projeto")["desvio_abs"].sum()
                .nsmallest(5).reset_index()
            )
            tbl(bot5)

    if "mes_ref" in df_res.columns:
        sec("Evolução Mensal de Receita Líquida × Prevista (R$)")
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
            labels={**rotulos_para(["receita_liquida", "receita_prevista"]),
                    "value": "R$", "variable": "Série"},
        )
        fig_sp.update_layout(height=250, margin=dict(t=10, b=30))
        rotular_fig(fig_sp)
        st.plotly_chart(fig_sp, use_container_width=True, key="res_sparkline")

    st.download_button(
        label="Baixar CSV",
        data=df_res.to_csv(index=False).encode("utf-8"),
        file_name="resumo.csv",
        mime="text/csv",
        key="dl_res",
    )


# ══════════════════════════════════════════════════════════════════════════════
# ABA 2 — KPIs EXECUTIVOS
# ══════════════════════════════════════════════════════════════════════════════

if selected_tab == "KPIs Executivos":
    df_kp = filtros(df_op, com_area=True, com_data=True, sufixo="kpi")

    rl  = df_kp["receita_liquida"].sum()
    rp  = df_kp["receita_prevista"].sum()
    ct  = df_kp["custo_total"].sum()
    at  = (rl / rp * 100) if rp else 0.0
    ra  = df_kp.get("receita_ajustada", pd.Series(dtype=float)).sum()
    da  = rl - rp

    sec("Métricas Financeiras Principais (FY)")
    c1, c2, c3 = st.columns(3)
    c4, c5, c6 = st.columns(3)

    c1.metric("Receita Líquida",   fmt_brl(rl),
              delta=_fmt_delta(delta_fy(df_op_full, "kpi", "receita_liquida")),
              help="Soma da Receita Líquida no Ano Fiscal selecionado.")
    c2.metric("Receita Prevista",  fmt_brl(rp),
              delta=_fmt_delta(delta_fy(df_op_full, "kpi", "receita_prevista")),
              help="Soma da Receita Prevista no Ano Fiscal selecionado.")
    c3.metric("Custo Total",       fmt_brl(ct),
              delta=_fmt_delta(delta_fy(df_op_full, "kpi", "custo_total"), inverso=True),
              help=FORMULA["custo_total"])
    c4.metric("Desvio (R$)",       fmt_brl(da),
              delta=_fmt_delta(delta_fy(df_op_full, "kpi", "desvio_abs")),
              help=FORMULA["desvio_abs"])
    c5.metric("Atingimento (%)",   f"{at:.1f}%",
              help=FORMULA["atingimento_pct"])
    c6.metric("Receita Ajustada",  fmt_brl(ra),
              delta=_fmt_delta(delta_fy(df_op_full, "kpi", "receita_ajustada")),
              help=FORMULA["receita_ajustada"])

    if "area" in df_kp.columns:
        sec("Indicadores Financeiros por Área (FY)")
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
            sec("Receita Realizada × Prevista × Custo Total por Área")
            df_bar = kpi_area.melt(id_vars="area",
                                     value_vars=["receita_liquida", "receita_prevista", "custo_total"],
                                     var_name="Métrica", value_name="Valor")
            df_bar["Métrica"] = df_bar["Métrica"].map(rotulo_coluna)
            fig_bar = px.bar(
                df_bar,
                x="area", y="Valor", color="Métrica", barmode="group",
                color_discrete_map={
                    rotulo_coluna("receita_liquida"):  COR_REALIZADO,
                    rotulo_coluna("receita_prevista"): COR_ORCADO,
                    rotulo_coluna("custo_total"):      "#FF9800",
                },
                labels={"Valor": "R$", "area": "Área"},
            )
            fig_bar.update_layout(height=340, margin=dict(t=10, b=30))
            rotular_fig(fig_bar)
            st.plotly_chart(fig_bar, use_container_width=True, key="kpi_bar_area")

        with col_b:
            sec("Atingimento de Receita por Área (%)")
            fig_at = px.bar(
                kpi_area, x="area", y="atingimento_pct",
                color="area", color_discrete_map=CORES_AREA,
                text_auto=".1f",
                labels={"atingimento_pct": "Atingimento (%)", "area": "Área"},
            )
            fig_at.add_hline(y=100, line_dash="dash", line_color=COR_DESVIO_NEG,
                             annotation_text="Meta 100%")
            fig_at.update_layout(height=340, margin=dict(t=10, b=30), showlegend=False)
            rotular_fig(fig_at)
            st.plotly_chart(fig_at, use_container_width=True, key="kpi_atingimento_area")

    st.download_button(
        label="Baixar CSV",
        data=df_kp.to_csv(index=False).encode("utf-8"),
        file_name="kpis_executivos.csv",
        mime="text/csv",
        key="dl_kpi",
    )


# ══════════════════════════════════════════════════════════════════════════════
# ABA 3 — SÉRIE TEMPORAL
# ══════════════════════════════════════════════════════════════════════════════

if selected_tab == "Série Temporal":
    df_ts = filtros(df_op, com_area=True, com_data=True, sufixo="ts")

    if modo_periodo == "fy" and fy_sel is not None:
        fy_lbl_atual = fy_label(fy_sel)
        fy_lbl_ant = fy_label(fy_sel - 1)
        meses_str = ", ".join(MESES_FY[p - 1] for p in posicoes_sel)
        sec(f"Acumulado no Ano Fiscal — {fy_lbl_atual}")
        st.caption(
            f"Variações comparam **{fy_lbl_atual}** com **{fy_lbl_ant}** "
            f"nos mesmos meses do FY ({meses_str})."
        )
    else:
        sec("Acumulado no Período — Histórico Completo")
        st.caption("Comparações de FY desativadas neste modo.")

    j_rl  = df_ts["receita_liquida"].sum()
    j_rp  = df_ts["receita_prevista"].sum()
    j_ct  = df_ts["custo_total"].sum()
    j_dev = j_rl - j_rp
    j_at  = (j_rl / j_rp * 100) if j_rp else 0.0

    cj1, cj2, cj3, cj4, cj5 = st.columns(5)
    cj1.metric("Receita Líquida",   fmt_brl(j_rl),
               delta=_fmt_delta(delta_fy(df_op_full, "ts", "receita_liquida")),
               help="Soma da Receita Líquida no período selecionado.")
    cj2.metric("Receita Prevista",  fmt_brl(j_rp),
               delta=_fmt_delta(delta_fy(df_op_full, "ts", "receita_prevista")),
               help="Soma da Receita Prevista no período selecionado.")
    cj3.metric("Desvio",            fmt_brl(j_dev),
               delta=_fmt_delta(delta_fy(df_op_full, "ts", "desvio_abs")),
               help=FORMULA["desvio_abs"])
    cj4.metric("Atingimento",       f"{j_at:.1f}%",
               delta=f"{j_at - 100:.1f}% vs meta 100%",
               help=FORMULA["atingimento_pct"])
    cj5.metric("Custo Total",       fmt_brl(j_ct),
               delta=_fmt_delta(delta_fy(df_op_full, "ts", "custo_total"), inverso=True),
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

        # Posição no FY (1 = Junho, 12 = Maio) para alinhar com FY anterior
        ts["pos_fy"] = ts["mes_ref"].dt.month.apply(lambda m: ((m - 6) % 12) + 1)
        ts["Mês"] = ts["mes_ref"].dt.strftime("%b/%y")
        ts["atingimento_pct"] = np.where(
            ts["receita_prevista"] != 0,
            ts["receita_liquida"] / ts["receita_prevista"] * 100, np.nan,
        )

        # Série do FY anterior (mesmas seleções), para sobreposição
        ts_ant = pd.DataFrame()
        if modo_periodo == "fy" and fy_sel is not None and posicoes_sel:
            sels_ts = selecoes_de_sufixo("ts")
            df_ts_ant = aplicar_selecoes(
                aplicar_fy(df_op_full, fy_sel - 1, posicoes_sel), sels_ts
            )
            if not df_ts_ant.empty and "mes_ref" in df_ts_ant.columns:
                ts_ant = (
                    df_ts_ant.groupby("mes_ref")["receita_liquida"]
                    .sum().reset_index().sort_values("mes_ref")
                )
                ts_ant["pos_fy"] = ts_ant["mes_ref"].dt.month.apply(
                    lambda m: ((m - 6) % 12) + 1
                )

        sec("Evolução Mensal de Receita — Realizado × Previsto × Orçado")
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
            labels={**rotulos_para(y_linhas), "value": "R$", "variable": "Série"},
        )
        # Overlay do FY anterior, alinhado por posição no FY
        if not ts_ant.empty:
            ts_ant_aligned = ts_ant.merge(
                ts[["pos_fy", "Mês"]], on="pos_fy", how="inner"
            )
            if not ts_ant_aligned.empty:
                fig_linha.add_scatter(
                    x=ts_ant_aligned["Mês"],
                    y=ts_ant_aligned["receita_liquida"],
                    mode="lines+markers",
                    line=dict(dash="dot", color="#888"),
                    name=f"Receita Líquida — {fy_label(fy_sel - 1)}",
                )
        fig_linha.update_layout(height=320, margin=dict(t=10, b=30))
        rotular_fig(fig_linha)
        st.plotly_chart(fig_linha, use_container_width=True, key="ts_linha")

        col_dv, col_ct = st.columns(2)

        with col_dv:
            sec("Desvio Mensal de Receita (Realizado − Previsto, R$)")
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
            sec("Custo Total Mensal — Allowance + Contingência (R$)")
            fig_area = px.area(
                ts, x="Mês", y="custo_total",
                color_discrete_sequence=["#FF9800"],
                labels={"custo_total": "Custo Total (R$)"},
            )
            fig_area.update_layout(height=290, margin=dict(t=10, b=30))
            st.plotly_chart(fig_area, use_container_width=True, key="ts_custo_area")

        sec("Atingimento Mensal de Receita — Realizada / Prevista (%)")
        fig_at_ts = px.line(
            ts, x="Mês", y="atingimento_pct", markers=True,
            color_discrete_sequence=[COR_DLT_GREEN],
            labels={"atingimento_pct": "Atingimento (%)"},
        )
        fig_at_ts.add_hline(y=100, line_dash="dash", line_color=COR_DESVIO_NEG,
                            annotation_text="Meta 100%")
        fig_at_ts.update_layout(height=260, margin=dict(t=10, b=30))
        st.plotly_chart(fig_at_ts, use_container_width=True, key="ts_ating")

    st.download_button(
        label="Baixar CSV",
        data=df_ts.to_csv(index=False).encode("utf-8"),
        file_name="serie_temporal.csv",
        mime="text/csv",
        key="dl_ts",
    )


# ══════════════════════════════════════════════════════════════════════════════
# ABA 4 — ÁREAS
# ══════════════════════════════════════════════════════════════════════════════

if selected_tab == "Áreas":
    df_ar = filtros(df_op, com_area=True, com_data=True, sufixo="ar")

    sec("Orçado × Realizado por Área — Receita e Custo")
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
            df_ar_bar = ag_area.melt(id_vars="area", value_vars=y_bar,
                                     var_name="Métrica", value_name="Valor")
            df_ar_bar["Métrica"] = df_ar_bar["Métrica"].map(rotulo_coluna)
            fig_ar = px.bar(
                df_ar_bar,
                x="area", y="Valor", color="Métrica", barmode="group",
                color_discrete_map={
                    rotulo_coluna("receita_liquida"):  COR_REALIZADO,
                    rotulo_coluna("receita_prevista"): COR_ORCADO,
                    rotulo_coluna("custo_total"):      "#FF9800",
                    rotulo_coluna("orcado_budget"):    "#9B59B6",
                },
                labels={"Valor": "R$", "area": "Área"},
                title="Receita Orçada × Realizada × Custo por Área",
            )
            fig_ar.update_layout(height=340, margin=dict(t=40, b=30))
            rotular_fig(fig_ar)
            st.plotly_chart(fig_ar, use_container_width=True, key="ar_orc_real")

        with col_b2:
            sec("Atingimento de Receita por Área (%)")
            fig_at_ar = px.bar(
                ag_area, x="area", y="atingimento_pct",
                color="area", color_discrete_map=CORES_AREA, text_auto=".1f",
                labels={"atingimento_pct": "Atingimento (%)", "area": "Área"},
            )
            fig_at_ar.add_hline(y=100, line_dash="dash", line_color=COR_DESVIO_NEG,
                                annotation_text="Meta 100%")
            fig_at_ar.update_layout(height=340, margin=dict(t=10, b=30), showlegend=False)
            rotular_fig(fig_at_ar)
            st.plotly_chart(fig_at_ar, use_container_width=True, key="ar_ating")

        tbl(ag_area)

    # Evolução temporal por área
    if "area" in df_ar.columns and "mes_ref" in df_ar.columns:
        sec("Evolução Mensal de Receita Líquida por Área")
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
        sec("Receita Líquida por Sub Área")

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

    # Área × Mês — visualização em mapa de calor (substitui a tabela detalhada)
    if "area" in df_ar.columns and "mes_ref" in df_ar.columns:
        sec("Receita Líquida Mensal por Área — Mapa de Calor (R$)")

        df_am_long = (
            df_ar.groupby(["area", "mes_ref"])
            .agg(
                receita_liquida =("receita_liquida",  "sum"),
                receita_prevista=("receita_prevista", "sum"),
                custo_total     =("custo_total",      "sum"),
            )
            .reset_index()
            .sort_values("mes_ref")
        )
        df_am_long["Mês"] = df_am_long["mes_ref"].dt.strftime("%b/%y")

        # Métrica selecionável para o mapa de calor
        opcoes_metrica = {
            "Receita Líquida":  "receita_liquida",
            "Receita Prevista": "receita_prevista",
            "Custo Total":      "custo_total",
        }
        metrica_lbl = st.radio(
            "Métrica exibida no mapa de calor",
            list(opcoes_metrica.keys()),
            horizontal=True,
            key="ar_heat_metrica",
        )
        col_metrica = opcoes_metrica[metrica_lbl]

        # Pivota Área × Mês preservando a ordem cronológica dos meses
        ordem_meses = (
            df_am_long.drop_duplicates("mes_ref")
            .sort_values("mes_ref")["Mês"].tolist()
        )
        pivot_am = (
            df_am_long.pivot(index="area", columns="Mês", values=col_metrica)
            .reindex(columns=ordem_meses)
        )

        if pivot_am.empty:
            st.info("Sem dados suficientes para montar o mapa de calor.")
        else:
            fig_heat_am = px.imshow(
                pivot_am,
                color_continuous_scale="Blues",
                aspect="auto",
                text_auto=".2s",
                labels={
                    "x": "Mês",
                    "y": "Área",
                    "color": f"{metrica_lbl} (R$)",
                },
            )
            fig_heat_am.update_layout(height=320, margin=dict(t=20, b=30))
            st.plotly_chart(fig_heat_am, use_container_width=True, key="ar_heat_am")

        # Download CSV completo com as três métricas, mantendo o detalhamento da tabela original
        tbl_am_csv = df_am_long[
            ["area", "Mês", "receita_liquida", "receita_prevista", "custo_total"]
        ].rename(columns={
            "area": "Área",
            "receita_liquida": "Receita Líquida (R$)",
            "receita_prevista": "Receita Prevista (R$)",
            "custo_total": "Custo Total (R$)",
        })
        st.download_button(
            label="Baixar CSV — Área × Mês (completo)",
            data=tbl_am_csv.to_csv(index=False).encode("utf-8"),
            file_name="area_x_mes.csv",
            mime="text/csv",
            key="dl_ar_am",
        )

    st.download_button(
        label="Baixar CSV",
        data=df_ar.to_csv(index=False).encode("utf-8"),
        file_name="areas.csv",
        mime="text/csv",
        key="dl_ar",
    )


# ══════════════════════════════════════════════════════════════════════════════
# ABA 5 — PROJETOS
# ══════════════════════════════════════════════════════════════════════════════

if selected_tab == "Projetos":
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

        sec("Ranking de Receita Líquida por Projeto (R$)")
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
            sec("Atingimento (%) × Desvio (R$) por Projeto")
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
            sec("Participação na Receita Líquida — Treemap por Área e Projeto")
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

        sec("Tabela Detalhada de Métricas Financeiras por Projeto")
        tbl(ag_proj.sort_values("receita_liquida", ascending=False))

    st.download_button(
        label="Baixar CSV",
        data=df_pr.to_csv(index=False).encode("utf-8"),
        file_name="projetos.csv",
        mime="text/csv",
        key="dl_pr",
    )


# ══════════════════════════════════════════════════════════════════════════════
# ABA 6 — DESVIOS & ALERTAS
# ══════════════════════════════════════════════════════════════════════════════

if selected_tab == "Desvios & Alertas":
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

        sec("Ranking de Desvio Absoluto (R$) por Projeto — Realizado − Previsto")
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
                    f'<div class="{css_cls}"> <b>{row["projeto"]}</b> — '
                    f'Atingimento: <b>{row["atingimento_pct"]:.1f}%</b> | '
                    f'Desvio: <b>{fmt_brl(row["desvio_abs"])}</b></div>',
                    unsafe_allow_html=True,
                )
        else:
            st.success("Todos os projetos com atingimento ≥ 90%.")

    if all(c in df_dv.columns for c in ["allowance", "contingencia"]) and "area" in df_dv.columns:
        sec("Decomposição do Custo Total por Área — Allowance × Contingência (R$)")
        ag_custo = df_dv.groupby("area").agg(
            allowance   =("allowance",    "sum"),
            contingencia=("contingencia", "sum"),
        ).reset_index()
        df_custo = ag_custo.melt(id_vars="area", var_name="Tipo", value_name="Valor")
        df_custo["Tipo"] = df_custo["Tipo"].map(rotulo_coluna)
        fig_custo = px.bar(
            df_custo,
            x="area", y="Valor", color="Tipo", barmode="stack",
            color_discrete_map={rotulo_coluna("allowance"): "#4472C4", rotulo_coluna("contingencia"): "#FF9800"},
            labels={"Valor": "R$", "area": "Área"},
        )
        fig_custo.update_layout(height=300, margin=dict(t=10, b=30))
        rotular_fig(fig_custo)
        st.plotly_chart(fig_custo, use_container_width=True, key="dv_custo")

    if all(c in df_dv.columns for c in ["projeto", "mes_ref", "atingimento_pct"]):
        sec("Mapa de Calor — Atingimento de Receita (%) por Projeto × Mês")
        df_heat = df_dv.copy()
        df_heat["Mês"] = df_heat["mes_ref"].dt.strftime("%b/%y")
        # Ordem cronológica dos meses (preserva a sequência real de mes_ref)
        ordem_meses = (
            df_heat.dropna(subset=["mes_ref"])
            .drop_duplicates("mes_ref")
            .sort_values("mes_ref")["Mês"].tolist()
        )
        pivot = (
            df_heat.groupby(["projeto", "Mês"])["atingimento_pct"]
            .mean().reset_index()
            .pivot(index="projeto", columns="Mês", values="atingimento_pct")
            .reindex(columns=ordem_meses)
        )
        if not pivot.empty:
            fig_heat = px.imshow(
                pivot,
                color_continuous_scale=[COR_DESVIO_NEG, "#FFC107", COR_DESVIO_POS],
                aspect="auto", zmin=60, zmax=140, text_auto=".0f",
                labels={"color": "Atingimento (%)"},
            )
            fig_heat.update_xaxes(categoryorder="array", categoryarray=ordem_meses)
            fig_heat.update_layout(height=380, margin=dict(t=10, b=30))
            st.plotly_chart(fig_heat, use_container_width=True, key="dv_heatmap")

    st.download_button(
        label="Baixar CSV",
        data=df_dv.to_csv(index=False).encode("utf-8"),
        file_name="desvios_alertas.csv",
        mime="text/csv",
        key="dl_dv",
    )


# ══════════════════════════════════════════════════════════════════════════════
# ABA 7 — DADOS
# ══════════════════════════════════════════════════════════════════════════════

if selected_tab == "Dados":
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
    nomes_legiv = {c: rotulo_coluna(c) for c in todas_cols}
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
        desc.index = [rotulo_coluna(i) for i in desc.index]
        st.dataframe(desc, use_container_width=True)

    csv = df_vis.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Baixar CSV filtrado",
        data=csv,
        file_name="base_operacional_filtrada.csv",
        mime="text/csv",
    )


# ══════════════════════════════════════════════════════════════════════════════
# ABA 8 — DICIONÁRIO DE DADOS
# ══════════════════════════════════════════════════════════════════════════════

if selected_tab == "Dicionário":
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

# ══════════════════════════════════════════════════════════════════════════════
# ABA 9 — MEUS INSIGHTS
# ══════════════════════════════════════════════════════════════════════════════

if selected_tab == "Meus Insights":
    from kpi_agent import _render_resumo as _kpi_render_resumo

    st.markdown("""
    <style>
    .insight-card-header {
        background: #12121f; border: 1px solid #1e2238;
        border-radius: 10px 10px 0 0; padding: 12px 16px 8px 16px;
    }
    .insight-tipo-badge {
        display: inline-block; font-size: 10px; font-weight: 700;
        text-transform: uppercase; letter-spacing: 0.6px;
        padding: 2px 8px; border-radius: 20px; margin-bottom: 6px;
    }
    .badge-grafico   { background: #86BC2520; color: #86BC25; border: 1px solid #86BC2540; }
    .badge-tabela    { background: #1555c020; color: #7ba4e8; border: 1px solid #1555c040; }
    .badge-comparacao{ background: #c0810020; color: #e8a840; border: 1px solid #c0810040; }
    .badge-resumo    { background: #80208020; color: #d080d0; border: 1px solid #80208040; }
    .insight-card-title { font-size: 14px; font-weight: 600; color: #c8d0e0; }
    .insight-card-date  { font-size: 11px; color: #3a3f50; margin-top: 2px; }
    .insights-empty-wrap {
        display: flex; flex-direction: column; align-items: center;
        justify-content: center; height: 55vh; gap: 18px;
    }
    .insights-empty-text { font-size: 18px; color: #888; font-weight: 500; }
    </style>
    """, unsafe_allow_html=True)

    insights = carregar_insights()

    _TIPO_LABEL = {
        "grafico":    ("Gráfico",    "badge-grafico"),
        "tabela":     ("Tabela",     "badge-tabela"),
        "comparacao": ("Comparativo","badge-comparacao"),
        "resumo":     ("Resumo",     "badge-resumo"),
    }

    if not insights:
        st.markdown("""
        <div class="insights-empty-wrap">
            <div class="insights-empty-text">Ainda não há nada por aqui.</div>
        </div>
        """, unsafe_allow_html=True)
        _, col_c, _ = st.columns([2, 2, 2])
        with col_c:
            if st.button("Construir novos insights.", use_container_width=True, key="btn_ir_assistente"):
                st.session_state["active_dashboard_tab"] = "Assistente Deloitte"
                try:
                    st.query_params["tab"] = "Assistente Deloitte"
                except Exception:
                    pass
                st.rerun()
    else:
        n = len(insights)
        header_col, btn_col = st.columns([4, 1])
        with header_col:
            st.markdown(f"### Meus Insights &nbsp;<span style='font-size:14px;color:#555;font-weight:400'>({n} item{'s' if n>1 else ''})</span>", unsafe_allow_html=True)
        with btn_col:
            if st.button("＋ Novo insight", key="btn_novo_insight", use_container_width=True):
                st.session_state["active_dashboard_tab"] = "Assistente Deloitte"
                try:
                    st.query_params["tab"] = "Assistente Deloitte"
                except Exception:
                    pass
                st.rerun()

        # Resumos ocupam largura total; gráficos/tabelas/comparativos em grid 2 colunas
        resumos_idx   = [i for i, s in enumerate(insights) if s.get("tipo") == "resumo"]
        restantes_idx = [i for i, s in enumerate(insights) if s.get("tipo") != "resumo"]

        for idx in resumos_idx:
            spec = insights[idx]
            tipo_label, tipo_badge = _TIPO_LABEL.get("resumo", ("Resumo", "badge-resumo"))
            st.markdown(f"""
            <div class="insight-card-header">
                <span class="insight-tipo-badge {tipo_badge}">{tipo_label}</span>
                <div class="insight-card-title">{spec.get("titulo","Resumo")}</div>
                <div class="insight-card-date">Salvo em {spec.get("saved_at","")}</div>
            </div>""", unsafe_allow_html=True)
            _kpi_render_resumo(spec.get("titulo",""), spec.get("conteudo",""))
            if st.button("🗑 Remover", key=f"rm_insight_{idx}"):
                remover_insight(idx)
                st.rerun()
            st.markdown("---")

        for row_start in range(0, len(restantes_idx), 2):
            cols = st.columns(2, gap="large")
            for col_idx, col in enumerate(cols):
                if row_start + col_idx >= len(restantes_idx):
                    break
                idx  = restantes_idx[row_start + col_idx]
                spec = insights[idx]
                tipo = spec.get("tipo", "grafico")
                tipo_label, tipo_badge = _TIPO_LABEL.get(tipo, ("Item", "badge-grafico"))
                with col:
                    st.markdown(f"""
                    <div class="insight-card-header">
                        <span class="insight-tipo-badge {tipo_badge}">{tipo_label}</span>
                        <div class="insight-card-title">{spec.get("titulo","")}</div>
                        <div class="insight-card-date">Salvo em {spec.get("saved_at","")}</div>
                    </div>""", unsafe_allow_html=True)

                    if tipo == "grafico":
                        _render_grafico(spec, df_op, key=f"ins_chart_{idx}")
                    elif tipo == "tabela":
                        _render_tabela(spec, df_op, key=f"ins_tab_{idx}")
                    elif tipo == "comparacao":
                        _render_comparacao(spec, df_op, key=f"ins_comp_{idx}")

                    if st.button("🗑 Remover", key=f"rm_insight_{idx}", use_container_width=True):
                        remover_insight(idx)
                        st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
# ABA 10 — ASSISTENTE DE KPIs (IA)
# ══════════════════════════════════════════════════════════════════════════════

if selected_tab == "Assistente Deloitte":
    render_kpi_agent(df=df_op)