"""
kpi_agent.py
============
Agente de KPIs financeiros para o Dashboard Executivo — Deloitte.
Usa Groq (gratuito) com o modelo llama-3.3-70b-versatile.

Variável de ambiente obrigatória (defina no .env):
    GROQ_API_KEY=gsk_sua-chave-aqui

Histórico salvo em: historico_chat.json (mesma pasta do app_v1.py)
"""
from __future__ import annotations
import os
import json
import re
from datetime import datetime
from pathlib import Path

from groq import Groq
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()


def _get_historico_path() -> Path:
    username = st.session_state.get('username', 'default')
    safe_username = re.sub(r"[^a-zA-Z0-9_-]", "_", str(username)).strip() or "default"
    return Path(__file__).parent / f"historico_chat_{safe_username}.json"


def _get_insights_path() -> Path:
    username = st.session_state.get('username', 'default')
    safe_username = re.sub(r"[^a-zA-Z0-9_-]", "_", str(username)).strip() or "default"
    return Path(__file__).parent / f"insights_{safe_username}.json"


def carregar_insights() -> list[dict]:
    path = _get_insights_path()
    if not path.exists():
        return []
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def _salvar_insights(insights: list[dict]):
    with open(_get_insights_path(), "w", encoding="utf-8") as f:
        json.dump(insights, f, ensure_ascii=False, indent=2)


def adicionar_insight(spec: dict):
    insights = carregar_insights()
    spec_com_data = dict(spec)
    spec_com_data["saved_at"] = datetime.now().strftime("%d/%m/%Y %H:%M")
    insights.append(spec_com_data)
    _salvar_insights(insights)


def remover_insight(idx: int):
    insights = carregar_insights()
    if 0 <= idx < len(insights):
        insights.pop(idx)
    _salvar_insights(insights)

# ─────────────────────────────────────────────────────────────────────────────
# SYSTEM PROMPT
# ─────────────────────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = """
Você é um assistente financeiro especialista em KPIs, integrado ao Dashboard
Executivo da área de AI & Data da Deloitte. Você domina finanças corporativas
e conhece profundamente os dados e regras de negócio deste projeto específico.

## FÓRMULAS E CONCEITOS FINANCEIROS

| KPI                | Fórmula                                                        |
|--------------------|----------------------------------------------------------------|
| Desvio Absoluto    | Receita Líquida − Receita Prevista                             |
| Desvio (%)         | (Receita Líquida − Receita Prevista) / Receita Prevista × 100  |
| Atingimento (%)    | Receita Líquida / Receita Prevista × 100                       |
| Receita Ajustada   | Receita Líquida − Allowance − Contingência                     |
| Custo Total        | Allowance + Contingência                                       |
| Margem (%)         | Margem / Receita Orçada × 100                                  |
| EBITDA             | Lucro Operacional + Depreciação + Amortização                  |
| Margem Bruta       | (Receita − CPV) / Receita × 100                                |
| Margem Líquida     | Lucro Líquido / Receita × 100                                  |
| ROE                | Lucro Líquido / Patrimônio Líquido × 100                       |
| ROI                | (Ganho − Custo) / Custo × 100                                  |
| CAC                | (Custo Marketing + Vendas) / Novos Clientes                    |
| LTV                | Ticket Médio × Frequência × Tempo de Retenção                  |
| Churn Rate         | Clientes Perdidos / Clientes no Início × 100                   |
| MRR                | Receita Mensal Recorrente                                      |
| ARR                | MRR × 12                                                       |
| NPS                | % Promotores − % Detratores                                    |
| Break-even         | Custos Fixos / Margem de Contribuição Unitária                 |

## CAMPOS DO PROJETO — BASE OPERACIONAL (data1_csv.txt)
- Ajuste: receita reconhecida fora de débito de horas (campo opcional)
- Allowance: lançamento contábil a ser estornado no mês seguinte (>= 0)
- Contingência: reserva de receita a estornar (>= 0)
- Funcionário: tipo de colaborador — STAFF ou EXECUTIVO
- Centro de Custo: CC1, CC2, CC3
- Projeto: P1 até P10
- Área: SL01, SL02
- Sub Área: CO (Consultoria), AI (Inteligência Artificial), En (Engenharia)
- Mês/Ano: período de competência mensal
- ID Quinzena: 1 = dias 1-15 / 2 = dias 16-fim do mês
- Receita Prevista: orçado / estimado para o período
- Receita Líquida: realizado após deduções e ajustes

## CAMPOS DO PROJETO — BASE ORÇAMENTÁRIA (BookService.txt)
- Area: SL01, SL02
- Type: Receita | Custo | Margin
- Colunas mensais de jun/25 a mai/26 com valores orçados

## REGRAS DE NEGÓCIO
- Receita Ajustada = Receita Líquida − Allowance − Contingência
- Custo Total = Allowance + Contingência
- Atingimento: meta de referência é 100%
- Desvio positivo (Líquida > Prevista) = favorável
- Quinzenas são a granularidade mínima; mês = soma das duas quinzenas

## INSTRUÇÕES DE COMPORTAMENTO
- Responda sempre em português brasileiro
- Os dados reais do dashboard estão no contexto — USE-OS para responder
- Mostre o cálculo passo a passo quando solicitado
- Quando o usuário pedir para "adicionar um KPI" ou "incluir uma métrica",
  retorne obrigatoriamente um bloco JSON no seguinte formato:
  {"acao": "adicionar_kpi", "nome": "...", "formula": "...", "valor": "...", "contexto": "..."}
- Quando o usuário pedir para "gerar um gráfico", "plotar", "mostrar gráfico", "visualizar" ou similar,
  retorne obrigatoriamente um bloco JSON no seguinte formato:
  {"acao": "gerar_grafico", "tipo": "bar", "x": "coluna_x", "y": "coluna_y", "color": null, "titulo": "...", "agregacao": "sum"}
  Tipos válidos: bar (barras), line (linha), pie (pizza), scatter (dispersão).
  Agregações válidas: sum (soma), mean (média), count (contagem).
  Use apenas colunas listadas em COLUNAS DISPONÍVEIS no contexto.
  O campo "color" aceita uma coluna categórica ou null.
- Para perguntas conceituais, seja direto e didático, com exemplos numéricos
- Máximo 250 palavras por resposta, salvo quando o usuário pedir detalhes
"""

# ─────────────────────────────────────────────────────────────────────────────
# CSS
# ─────────────────────────────────────────────────────────────────────────────

_CHAT_CSS = """
<style>
/* ══ SIDEBAR ══ */
[data-testid="stSidebar"] {
    background: #0d0d1a !important;
    border-right: 1px solid #1a1a2e !important;
}
[data-testid="stSidebar"] .block-container {
    padding: 18px 10px 18px 10px !important;
}
[data-testid="stSidebar"] [data-testid="stVerticalBlockBorderWrapper"],
[data-testid="stSidebar"] [data-testid="stVerticalBlock"] { gap: 0 !important; }

/* Logo */
.sidebar-logo {
    display: flex; align-items: center; gap: 10px;
    padding: 0 2px 16px 2px; margin-bottom: 12px;
    border-bottom: 1px solid #1a1a2e;
}
.sidebar-logo-icon {
    width: 38px; height: 38px;
    background: linear-gradient(135deg, #86BC25 0%, #5a8a10 100%);
    border-radius: 50%; display: flex; align-items: center;
    justify-content: center; font-size: 18px; color: #fff; font-weight: 800;
    flex-shrink: 0;
}
.sidebar-logo-name {
    font-size: 17px; font-weight: 700; color: #c8d0e0; line-height: 1.1;
}
.sidebar-logo-name span { color: #86BC25; }

/* Nova conversa - primary */
[data-testid="stSidebar"] [data-testid="baseButton-primary"] {
    background: linear-gradient(135deg, #1555c0 0%, #012169 100%) !important;
    color: #fff !important; border: none !important;
    border-radius: 10px !important; font-size: 13px !important;
    font-weight: 600 !important; letter-spacing: 0.3px !important;
    padding: 9px 14px !important;
    box-shadow: 0 2px 10px rgba(1,33,105,0.45) !important;
    transition: all 0.18s ease !important;
}
[data-testid="stSidebar"] [data-testid="baseButton-primary"]:hover {
    background: linear-gradient(135deg, #1e6ae8 0%, #0a2a80 100%) !important;
    box-shadow: 0 4px 16px rgba(1,33,105,0.6) !important;
    transform: translateY(-1px) !important;
}

/* Section label */
.history-section-label {
    font-size: 10px; color: #3a3f50; text-transform: uppercase;
    letter-spacing: 0.9px; margin: 14px 0 2px 4px;
}

/* Conversation item buttons */
[data-testid="stSidebar"] [data-testid="baseButton-secondary"] {
    background: transparent !important; border: none !important;
    color: #6a7488 !important; text-align: left !important;
    padding: 9px 10px !important; border-radius: 8px !important;
    font-size: 13px !important; font-weight: 400 !important;
    transition: background 0.15s, color 0.15s !important;
    white-space: nowrap !important; overflow: hidden !important;
    text-overflow: ellipsis !important;
}
[data-testid="stSidebar"] [data-testid="baseButton-secondary"]:hover {
    background: rgba(255,255,255,0.05) !important;
    color: #b0bcd0 !important;
}

/* Active conversation — targets button immediately after .active-conv-indicator */
*:has(> .active-conv-indicator) ~ * [data-testid="stColumn"]:first-child [data-testid="baseButton-secondary"],
*:has(.active-conv-indicator) + * [data-testid="stColumn"]:first-child [data-testid="baseButton-secondary"] {
    background: rgba(134,188,37,0.1) !important;
    color: #c8d8e8 !important;
    border-left: 3px solid #86BC25 !important;
    border-radius: 0 8px 8px 0 !important;
    padding-left: 12px !important;
    font-weight: 500 !important;
}

/* Delete button — sempre centralizado independente do tamanho da sidebar */
[data-testid="stSidebar"] [data-testid="stColumn"]:last-child,
[data-testid="stSidebar"] [data-testid="stColumn"]:last-child > *,
[data-testid="stSidebar"] [data-testid="stColumn"]:last-child .stButton {
    display: flex !important;
    align-items: center !important;
    justify-content: center !important;
    width: 100% !important;
}
[data-testid="stSidebar"] [data-testid="stColumn"]:last-child [data-testid="baseButton-secondary"] {
    color: #3a4050 !important;
    padding: 0 !important;
    font-size: 13px !important;
    line-height: 1 !important;
    width: 26px !important;
    max-width: 26px !important;
    min-width: 26px !important;
    height: 26px !important;
    min-height: 26px !important;
    border-radius: 6px !important;
    display: flex !important;
    align-items: center !important;
    justify-content: center !important;
    flex-shrink: 0 !important;
    background: rgba(255,255,255,0.04) !important;
    border: 1px solid #252838 !important;
}
[data-testid="stSidebar"] [data-testid="stColumn"]:last-child [data-testid="baseButton-secondary"]:hover {
    color: #e05050 !important;
    background: rgba(200,40,40,0.15) !important;
    border-color: rgba(200,40,40,0.3) !important;
}

/* Conv item date */
.conv-date-row {
    font-size: 10px; color: #383e4e;
    margin: -4px 0 6px 10px; padding: 0;
    line-height: 1;
}

/* ══ MAIN AREA ══ */

/* Conversation title */
.conv-title {
    font-size: 17px; font-weight: 600; color: #c8d0e0;
    padding-bottom: 12px; border-bottom: 1px solid #1a1a2e;
    margin-bottom: 20px;
}

/* ── Messages ── */
.chat-wrapper { display: flex; flex-direction: column; gap: 0; padding: 4px 0 32px 0; }

/* Bloco base */
.msg-block {
    display: flex; flex-direction: row; align-items: flex-start; gap: 12px;
    padding: 10px 6px; border-radius: 6px;
    transition: background 0.1s;
}
.msg-block:hover { background: rgba(255,255,255,0.02); }

/* Usuário — invertido (avatar e conteúdo à direita) */
.msg-block.user-block { flex-direction: row-reverse; }

.msg-avatar {
    width: 36px; height: 36px; border-radius: 8px;
    display: flex; align-items: center; justify-content: center;
    font-size: 13px; font-weight: 700; flex-shrink: 0; margin-top: 2px;
}
.msg-avatar.ai   { background: #86BC25; color: #fff; }
.msg-avatar.user { background: #012169; color: #fff; }

.msg-right { flex: 1; min-width: 0; }

/* Cabeçalho: nome + hora */
.msg-meta { display: flex; align-items: baseline; gap: 8px; margin-bottom: 4px; }
/* Usuário: nome e hora alinhados à direita, na ordem correta (Você  12:27) */
.msg-block.user-block .msg-meta { justify-content: flex-end; }

.msg-sender { font-size: 14px; font-weight: 600; }
.msg-sender.ai-name   { color: #86BC25; }
.msg-sender.user-name { color: #7ba4e8; }
.msg-ts { font-size: 11px; color: #3a3f50; }

/* Texto da mensagem — IA: bolha verde igual ao ícone */
.msg-text {
    font-size: 14px; line-height: 1.72;
    color: #fff; word-break: break-word;
    background: linear-gradient(135deg, #86BC25 0%, #5a8a10 100%);
    padding: 11px 15px;
    border-radius: 3px 14px 14px 14px;
    display: inline-block;
    max-width: 85%;
}
.msg-text strong { color: #fff; font-weight: 700; }
/* Usuário: bolha à direita */
.msg-text.user-text {
    background: #012169;
    color: #dde8ff;
    padding: 10px 14px;
    border-radius: 14px 3px 14px 14px;
    display: inline-block;
    max-width: 75%;
    text-align: left;
    float: right;
    clear: both;
}
/* Clearfix após bolha do usuário */
.msg-block.user-block .msg-right::after {
    content: ""; display: table; clear: both;
}
.msg-text ul { margin: 6px 0 6px 18px; padding: 0; }
.msg-text li { margin-bottom: 3px; }
.msg-text strong { color: #dce8f8; font-weight: 700; }

/* ── KPIs / Charts ── */
.kpi-section-title {
    font-size: 11px; color: #86BC25; font-weight: 600;
    text-transform: uppercase; letter-spacing: 0.5px;
    margin: 20px 0 8px 0; border-bottom: 1px solid #86BC2525;
    padding-bottom: 4px;
}

/* ── Empty state ── */
.empty-state { text-align: center; padding: 60px 24px 24px 24px; }
.empty-state .icon  { font-size: 40px; margin-bottom: 14px; }
.empty-state .title { font-size: 17px; color: #505868; margin-bottom: 6px; font-weight: 600; }
.empty-state .sub   { font-size: 13px; color: #3a3f50; }

/* ── Suggestion cards (empty state) ── */
.suggestion-card button {
    border-radius: 10px !important;
    background: #12121f !important;
    border: 1px solid #1e2238 !important;
    color: #7a86a0 !important;
    font-size: 13px !important;
    padding: 14px 12px !important;
    text-align: left !important;
    line-height: 1.4 !important;
    transition: all 0.15s !important;
    white-space: normal !important;
    height: auto !important;
    min-height: 56px !important;
}
.suggestion-card button:hover {
    background: #181828 !important;
    border-color: #86BC2560 !important;
    color: #a8b4cc !important;
}
</style>
"""

# ─────────────────────────────────────────────────────────────────────────────
# PERSISTÊNCIA EM JSON
# ─────────────────────────────────────────────────────────────────────────────

def _salvar_historico():
    dados = []
    for c in st.session_state.conversations:
        dados.append({
            "id":         c["id"],
            "title":      c["title"],
            "messages":   c["messages"],
            "kpis":       c["kpis"],
            "graficos":   c.get("graficos", []),
            "created_at": c["created_at"].isoformat(),
        })
    with open(_get_historico_path(), "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)


def _carregar_historico() -> list[dict]:
    historico_path = _get_historico_path()
    if not historico_path.exists():
        return []
    try:
        with open(historico_path, encoding="utf-8") as f:
            dados = json.load(f)
        result = []
        for c in dados:
            result.append({
                "id":         c["id"],
                "title":      c.get("title", "Conversa"),
                "messages":   c.get("messages", []),
                "kpis":       c.get("kpis", []),
                "graficos":   c.get("graficos", []),
                "created_at": datetime.fromisoformat(c["created_at"]),
            })
        return result
    except Exception:
        return []

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _resumo_df(df: pd.DataFrame | None) -> str:
    if df is None or df.empty:
        return "Nenhum dado disponível no momento."
    colunas_num = df.select_dtypes(include="number").columns.tolist()
    colunas_cat = df.select_dtypes(exclude="number").columns.tolist()
    linhas = [
        f"Total de registros: {len(df)}",
        f"COLUNAS DISPONÍVEIS — numéricas: {colunas_num}",
        f"COLUNAS DISPONÍVEIS — categóricas/data: {colunas_cat}",
    ]
    if "mes_ref" in df.columns:
        datas = df["mes_ref"].dropna().sort_values()
        if not datas.empty:
            linhas.append(f"Período: {datas.min().strftime('%b/%Y')} → {datas.max().strftime('%b/%Y')}")
    linhas.append("\n--- TOTAIS CONSOLIDADOS ---")
    for col in ["receita_liquida", "receita_prevista", "allowance", "contingencia",
                "desvio_abs", "atingimento_pct", "receita_ajustada"]:
        if col in df.columns:
            s = df[col].dropna()
            if col == "atingimento_pct":
                linhas.append(f"{col}: média={s.mean():.1f}%")
            else:
                linhas.append(f"{col}: soma=R${s.sum():,.0f} | média=R${s.mean():,.0f}")
    if "area" in df.columns:
        linhas.append("\n--- POR ÁREA ---")
        for area, grp in df.groupby("area"):
            rl = grp["receita_liquida"].sum() if "receita_liquida" in grp else 0
            rp = grp["receita_prevista"].sum() if "receita_prevista" in grp else 0
            al = grp["allowance"].sum() if "allowance" in grp else 0
            co = grp["contingencia"].sum() if "contingencia" in grp else 0
            dev_pct = ((rl - rp) / rp * 100) if rp != 0 else 0
            ating   = (rl / rp * 100) if rp != 0 else 0
            linhas.append(
                f"Área {area}: rl=R${rl:,.0f} | rp=R${rp:,.0f} | desvio_abs=R${rl-rp:,.0f} | "
                f"desvio_pct={dev_pct:.1f}% | atingimento={ating:.1f}% | raj=R${rl-al-co:,.0f}"
            )
    if "sigla_sub_area" in df.columns:
        linhas.append("\n--- POR SUB ÁREA ---")
        for sub, grp in df.groupby("sigla_sub_area"):
            rl = grp["receita_liquida"].sum() if "receita_liquida" in grp else 0
            rp = grp["receita_prevista"].sum() if "receita_prevista" in grp else 0
            dev_pct = ((rl - rp) / rp * 100) if rp != 0 else 0
            linhas.append(f"Sub área {sub}: rl=R${rl:,.0f} | rp=R${rp:,.0f} | desvio={dev_pct:.1f}%")
    if "mes_ref" in df.columns and "receita_liquida" in df.columns:
        linhas.append("\n--- POR MÊS (últimos 6) ---")
        df_mes = df.groupby("mes_ref")[["receita_liquida","receita_prevista"]].sum().sort_index().tail(6)
        for mes, row in df_mes.iterrows():
            rl, rp = row.get("receita_liquida", 0), row.get("receita_prevista", 0)
            dev_pct = ((rl - rp) / rp * 100) if rp != 0 else 0
            linhas.append(f"{mes.strftime('%b/%Y')}: rl=R${rl:,.0f} | rp=R${rp:,.0f} | desvio={dev_pct:.1f}%")
    if "projeto" in df.columns and "receita_liquida" in df.columns:
        linhas.append("\n--- TOP 5 PROJETOS ---")
        for proj, val in df.groupby("projeto")["receita_liquida"].sum().sort_values(ascending=False).head(5).items():
            linhas.append(f"Projeto {proj}: R${val:,.0f}")
    return "\n".join(linhas)


def _chamar_api(messages: list[dict], contexto_df: str) -> str:
    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        return "⚠️ **GROQ_API_KEY** não configurada. Adicione no `.env`:\n```\nGROQ_API_KEY=gsk_...\n```"
    client = Groq(api_key=api_key)
    msgs_api = [{"role": "system", "content": _SYSTEM_PROMPT + "\n\n## DADOS REAIS\n" + contexto_df}]
    msgs_api += [{"role": m["role"], "content": m["content"]} for m in messages]
    resp = client.chat.completions.create(model="llama-3.3-70b-versatile", messages=msgs_api, max_tokens=1000)
    return resp.choices[0].message.content


def _resumir_valor_complexo(valor) -> str:
    """Converte valores compostos em um resumo aceito pelo st.metric."""
    if isinstance(valor, dict):
        if not valor:
            return "—"
        partes = [f"{chave}: {val}" for chave, val in valor.items()]
        resumo = " | ".join(partes)
        return resumo if len(resumo) <= 80 else f"{len(valor)} valores"
    if isinstance(valor, list):
        if not valor:
            return "—"
        resumo = " | ".join(str(item) for item in valor)
        return resumo if len(resumo) <= 80 else f"{len(valor)} valores"
    return "—" if valor is None else str(valor)


def _normalizar_kpi(kpi: dict) -> dict:
    """Garante que o KPI salvo possa ser renderizado sem quebrar o app."""
    kpi = dict(kpi)
    valor = kpi.get("valor", "—")
    if isinstance(valor, (dict, list)):
        kpi.setdefault("detalhes_valor", valor)
        kpi["valor"] = _resumir_valor_complexo(valor)
    return kpi


def _extrair_json(texto: str) -> dict | None:
    """Extrai e parseia o primeiro objeto JSON do texto.
    Suporta JSON inline e JSON dentro de blocos markdown ```json ... ```."""
    # Tenta primeiro extrair de bloco markdown ```json ... ```
    match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", texto, re.DOTALL)
    if match:
        json_str = match.group(1)
    else:
        # Fallback: do primeiro { até o último }
        try:
            json_str = texto[texto.index("{"):texto.rindex("}")+1]
        except ValueError:
            return None
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        return None


def _tentar_parse_kpi(texto: str) -> dict | None:
    candidato = _extrair_json(texto)
    if candidato is None or candidato.get("acao") != "adicionar_kpi":
        return None
    return _normalizar_kpi(candidato)


def _tentar_parse_grafico(texto: str) -> dict | None:
    candidato = _extrair_json(texto)
    if candidato is None or candidato.get("acao") != "gerar_grafico":
        return None
    return candidato


def _valor_metric_seguro(valor):
    """Tipos aceitos pelo st.metric: número, texto ou None. Nunca dict/list."""
    if valor is None:
        return "—"
    if isinstance(valor, (int, float, str)):
        return valor
    return _resumir_valor_complexo(valor)


def _render_detalhes_kpi(kpi: dict):
    detalhes = kpi.get("detalhes_valor")
    if detalhes is None:
        return

    if isinstance(detalhes, dict):
        df_detalhes = pd.DataFrame(
            [{"Item": chave, "Valor": valor} for chave, valor in detalhes.items()]
        )
        st.dataframe(df_detalhes, hide_index=True, use_container_width=True)
        return

    if isinstance(detalhes, list):
        df_detalhes = pd.DataFrame({"Valor": detalhes})
        st.dataframe(df_detalhes, hide_index=True, use_container_width=True)
        return

    st.caption(str(detalhes))


def _render_grafico(spec: dict, df: pd.DataFrame | None, key: str):
    import plotly.express as px
    if df is None or df.empty:
        st.warning("Sem dados para renderizar o gráfico.")
        return

    tipo      = spec.get("tipo", "bar")
    x_col     = spec.get("x")
    y_col     = spec.get("y")
    color_col = spec.get("color") or None
    titulo    = spec.get("titulo", "Gráfico")
    agg_func  = spec.get("agregacao", "sum")

    if x_col not in df.columns or y_col not in df.columns:
        st.warning(f"Coluna(s) '{x_col}' / '{y_col}' não encontrada(s) nos dados.")
        return

    group_cols = [x_col] + ([color_col] if color_col and color_col in df.columns else [])
    try:
        df_plot = df.groupby(group_cols)[y_col].agg(agg_func).reset_index()
    except Exception as e:
        st.warning(f"Erro ao agregar dados: {e}")
        return

    color_arg = color_col if color_col and color_col in df_plot.columns else None

    try:
        if tipo == "line":
            fig = px.line(df_plot, x=x_col, y=y_col, color=color_arg, title=titulo, markers=True)
        elif tipo == "pie":
            fig = px.pie(df_plot, names=x_col, values=y_col, title=titulo)
        elif tipo == "scatter":
            fig = px.scatter(df_plot, x=x_col, y=y_col, color=color_arg, title=titulo)
        else:
            fig = px.bar(df_plot, x=x_col, y=y_col, color=color_arg, title=titulo, barmode="group")
        fig.update_layout(height=350, margin=dict(t=50, b=30, l=30, r=30))
        st.plotly_chart(fig, use_container_width=True, key=key)
    except Exception as e:
        st.warning(f"Erro ao gerar gráfico: {e}")


def _md_to_html(text: str) -> str:
    text = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)
    lines, out, in_list = text.split("\n"), [], False
    for line in lines:
        s = line.strip()
        if s.startswith(("* ", "- ", "• ")):
            if not in_list:
                out.append("<ul>")
                in_list = True
            out.append(f"<li>{s[2:]}</li>")
        else:
            if in_list:
                out.append("</ul>")
                in_list = False
            out.append(f"{line}<br>" if s else "<br>")
    if in_list:
        out.append("</ul>")
    return "\n".join(out)


def _render_bubble(role: str, content: str, time_str: str = ""):
    is_user  = role == "user"
    side     = "user" if is_user else "ai"
    name     = "Você" if is_user else "Assistente Deloitte"
    letter   = "V"    if is_user else "IA"
    name_cls = "user-name" if is_user else "ai-name"
    text_cls = "user-text" if is_user else ""
    blk_cls  = "user-block" if is_user else ""
    ts_html  = f'<span class="msg-ts">{time_str}</span>' if time_str else ""
    st.markdown(
        f"""<div class="msg-block {blk_cls}">
            <div class="msg-avatar {side}">{letter}</div>
            <div class="msg-right">
                <div class="msg-meta">
                    <span class="msg-sender {name_cls}">{name}</span>
                    {ts_html}
                </div>
                <div class="msg-text {text_cls}">{_md_to_html(content)}</div>
            </div>
        </div>""",
        unsafe_allow_html=True,
    )


_DIAS_PT = ["Segunda-feira", "Terça-feira", "Quarta-feira",
            "Quinta-feira", "Sexta-feira", "Sábado", "Domingo"]

def _tempo_relativo(ts: datetime) -> str:
    diff_days = (datetime.now().date() - ts.date()).days
    if diff_days == 0:  return "Hoje"
    if diff_days == 1:  return "Ontem"
    if diff_days < 7:   return _DIAS_PT[ts.weekday()]
    return ts.strftime("%d/%m/%Y")


# ─────────────────────────────────────────────────────────────────────────────
# ESTADO
# ─────────────────────────────────────────────────────────────────────────────

def _init_state():
    username = st.session_state.get('username', 'default')
    if "current_user" not in st.session_state or st.session_state.current_user != username:
        st.session_state.current_user = username
        st.session_state.conversations = _carregar_historico()
        st.session_state.active_conv_id = (
            st.session_state.conversations[0]["id"]
            if st.session_state.conversations else None
        )
    if "conversations" not in st.session_state:
        st.session_state.conversations = _carregar_historico()
    if "active_conv_id" not in st.session_state:
        st.session_state.active_conv_id = (
            st.session_state.conversations[0]["id"]
            if st.session_state.conversations else None
        )


def _nova_conversa() -> str:
    conv_id = f"conv_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
    conv = {"id": conv_id, "title": "Nova conversa", "messages": [], "kpis": [], "graficos": [], "created_at": datetime.now()}
    st.session_state.conversations.insert(0, conv)
    st.session_state.active_conv_id = conv_id
    _salvar_historico()
    return conv_id


def _get_active() -> dict | None:
    for c in st.session_state.conversations:
        if c["id"] == st.session_state.active_conv_id:
            return c
    return None


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────

def _render_sidebar():
    _init_state()
    with st.sidebar:
        st.markdown(_CHAT_CSS, unsafe_allow_html=True)

        # Logo
        st.markdown("""
        <div class="sidebar-logo">
            <div class="sidebar-logo-icon">✦</div>
            <div class="sidebar-logo-name">Assistente <span>Deloitte</span></div>
        </div>
        """, unsafe_allow_html=True)

        # Botão Nova conversa (primary para CSS diferenciar)
        if st.button("＋  Nova conversa", key="btn_nova", use_container_width=True, type="primary"):
            _nova_conversa()
            st.session_state["active_dashboard_tab"] = "Assistente Deloitte"
            try:
                st.query_params["tab"] = "Assistente Deloitte"
            except Exception:
                pass
            st.session_state._jump_to_assistant = True
            st.rerun()

        if not st.session_state.conversations:
            st.markdown('<div style="font-size:12px;color:#2e3340;margin-top:16px;text-align:center">Nenhuma conversa ainda.</div>', unsafe_allow_html=True)
            return

        # Agrupamento por data
        hoje, ontem, semana, anteriores = [], [], [], []
        for c in st.session_state.conversations:
            diff = (datetime.now().date() - c["created_at"].date()).days
            if diff == 0:   hoje.append(c)
            elif diff == 1: ontem.append(c)
            elif diff < 7:  semana.append(c)
            else:           anteriores.append(c)

        def _render_group(label: str, group: list):
            if not group:
                return
            st.markdown(f'<div class="history-section-label">{label}</div>', unsafe_allow_html=True)
            for c in group:
                is_active = c["id"] == st.session_state.active_conv_id
                title     = c["title"]
                tempo     = _tempo_relativo(c["created_at"])

                # Marcador CSS para estado ativo (CSS :has() detecta e estiliza o botão seguinte)
                if is_active:
                    st.markdown('<div class="active-conv-indicator"></div>', unsafe_allow_html=True)

                col_btn, col_del = st.columns([6, 1])
                with col_btn:
                    if st.button(f"💬  {title[:30]}", key=f"sel_{c['id']}", use_container_width=True):
                        st.session_state.active_conv_id = c["id"]
                        st.session_state["active_dashboard_tab"] = "Assistente Deloitte"
                        try:
                            st.query_params["tab"] = "Assistente Deloitte"
                        except Exception:
                            pass
                        st.session_state._jump_to_assistant = True
                        st.rerun()
                with col_del:
                    if st.button("✕", key=f"del_{c['id']}"):
                        st.session_state.conversations = [x for x in st.session_state.conversations if x["id"] != c["id"]]
                        if st.session_state.active_conv_id == c["id"]:
                            st.session_state.active_conv_id = (
                                st.session_state.conversations[0]["id"]
                                if st.session_state.conversations else None
                            )
                        _salvar_historico()
                        st.rerun()

                # Data abaixo do item
                st.markdown(f'<div class="conv-date-row">{tempo}</div>', unsafe_allow_html=True)

        _render_group("Hoje", hoje)
        _render_group("Ontem", ontem)
        _render_group("Esta semana", semana)
        _render_group("Anteriores", anteriores)


# ─────────────────────────────────────────────────────────────────────────────
# COMPONENTE PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def render_kpi_agent(df: pd.DataFrame | None = None):
    _init_state()
    st.markdown(_CHAT_CSS, unsafe_allow_html=True)

    if not st.session_state.active_conv_id:
        _nova_conversa()

    conv = _get_active()
    if conv is None:
        _nova_conversa()
        conv = _get_active()

    titulo_conv = conv.get("title", "Nova conversa") if conv else "Nova conversa"
    st.markdown(f'<div class="conv-title">{titulo_conv}</div>', unsafe_allow_html=True)

    # KPIs adicionados
    if conv["kpis"]:
        st.markdown('<div class="kpi-section-title">KPIs adicionados pela IA</div>', unsafe_allow_html=True)
        cols = st.columns(min(len(conv["kpis"]), 4))
        for i, kpi in enumerate(conv["kpis"]):
            kpi = _normalizar_kpi(kpi)
            with cols[i % 4]:
                st.metric(
                    label=kpi.get("nome", "KPI"),
                    value=_valor_metric_seguro(kpi.get("valor", "—")),
                    help=f"Fórmula: {kpi.get('formula','—')}\n\nContexto: {kpi.get('contexto','—')}",
                )
                _render_detalhes_kpi(kpi)
        if st.button("Limpar KPIs", key="btn_limpar_kpis"):
            conv["kpis"] = []
            _salvar_historico()
            st.rerun()
        st.divider()

    # Estado vazio
    if not conv["messages"]:
        st.markdown("""
        <div class="empty-state">
            <div class="icon">💬</div>
            <div class="title">Como posso ajudar?</div>
            <div class="sub">Pergunte sobre KPIs, solicite cálculos ou peça novas métricas.</div>
        </div>""", unsafe_allow_html=True)

        sugestoes = [
            "Qual o desvio percentual de SL01?",
            "Gere um gráfico de receita líquida por área",
            "O que é allowance?",
        ]
        cols = st.columns(len(sugestoes))
        for i, perg in enumerate(sugestoes):
            with cols[i]:
                if st.button(perg, key=f"quick_{i}", use_container_width=True):
                    st.session_state["_kpi_quick"] = perg
                    st.rerun()

    if "_kpi_quick" in st.session_state:
        _processar_mensagem(st.session_state.pop("_kpi_quick"), conv, df)
        st.rerun()

    # Mensagens
    if conv["messages"]:
        st.markdown('<div class="chat-wrapper">', unsafe_allow_html=True)
        for msg in conv["messages"]:
            _render_bubble(msg["role"], msg["content"], msg.get("time", ""))
        st.markdown('</div>', unsafe_allow_html=True)

    # Gráficos gerados pela IA
    graficos = conv.get("graficos", [])
    if graficos:
        st.markdown('<div class="kpi-section-title">Gráficos gerados pela IA</div>', unsafe_allow_html=True)
        for i, spec in enumerate(graficos):
            _render_grafico(spec, df, key=f"ai_chart_{conv['id']}_{i}")
            _, col_btn, _ = st.columns([1, 2, 1])
            with col_btn:
                if st.button("＋ Adicionar a Meus Insights", key=f"insight_{conv['id']}_{i}", use_container_width=True):
                    adicionar_insight(spec)
                    st.toast("Gráfico adicionado a Meus Insights!", icon="✅")
        if st.button("Limpar Gráficos", key="btn_limpar_graficos"):
            conv["graficos"] = []
            _salvar_historico()
            st.rerun()

    # Input
    prompt = st.chat_input("Mensagem para o Assistente de KPIs...")
    if prompt:
        _processar_mensagem(prompt, conv, df)
        st.rerun()


def _processar_mensagem(prompt: str, conv: dict, df: pd.DataFrame | None):
    now_str = datetime.now().strftime("%H:%M")
    conv["messages"].append({"role": "user", "content": prompt, "time": now_str})
    if len(conv["messages"]) == 1:
        conv["title"] = prompt[:42] + ("…" if len(prompt) > 42 else "")

    with st.spinner("Analisando..."):
        resposta = _chamar_api(conv["messages"], _resumo_df(df))

    kpi_json = _tentar_parse_kpi(resposta)
    if kpi_json:
        conv["kpis"].append(kpi_json)

    grafico_json = _tentar_parse_grafico(resposta)
    if grafico_json:
        conv.setdefault("graficos", []).append(grafico_json)
        resposta = re.sub(r"```(?:json)?\s*\{.*?\}\s*```", "", resposta, flags=re.DOTALL)
        resposta = re.sub(r"\{[^{}]*\"acao\"\s*:\s*\"gerar_grafico\"[^{}]*\}", "", resposta, flags=re.DOTALL)
        resposta = resposta.strip() or "Gráfico gerado abaixo."

    resp_time = datetime.now().strftime("%H:%M")
    conv["messages"].append({"role": "assistant", "content": resposta, "time": resp_time})
    _salvar_historico()