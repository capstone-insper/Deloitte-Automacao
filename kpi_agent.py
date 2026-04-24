"""
kpi_agent.py
============
Agente de KPIs financeiros para o Dashboard Executivo — Deloitte.
Usa Groq (gratuito) com o modelo llama-3.3-70b-versatile.

Variável de ambiente obrigatória (defina no .env):
    GROQ_API_KEY=gsk_sua-chave-aqui

Obter chave gratuita em: https://console.groq.com
"""

import os
import json

from groq import Groq
import pandas as pd
import numpy as np
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

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
- Os dados reais do dashboard estão no contexto abaixo — USE-OS para responder
- Sempre que houver dados por área, por projeto ou por mês no contexto, use-os diretamente
- Mostre o cálculo passo a passo quando solicitado
- Quando o usuário pedir para "adicionar um KPI" ou "incluir uma métrica",
  retorne obrigatoriamente um bloco JSON no seguinte formato (antes ou depois
  da explicação textual):
  {"acao": "adicionar_kpi", "nome": "...", "formula": "...", "valor": "...", "contexto": "..."}
- Para perguntas conceituais, seja direto e didático, com exemplos numéricos
- Máximo 250 palavras por resposta, salvo quando o usuário pedir detalhes
"""

# ─────────────────────────────────────────────────────────────────────────────
# CSS DO CHAT
# ─────────────────────────────────────────────────────────────────────────────

_CHAT_CSS = """
<style>
/* Container geral do chat */
.chat-wrapper {
    display: flex;
    flex-direction: column;
    gap: 16px;
    padding: 8px 0 24px 0;
    max-width: 860px;
    margin: 0 auto;
}

/* Linha de mensagem */
.chat-row {
    display: flex;
    align-items: flex-end;
    gap: 10px;
}
.chat-row.user {
    flex-direction: row-reverse;
}

/* Avatar */
.avatar {
    width: 32px;
    height: 32px;
    border-radius: 50%;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 14px;
    flex-shrink: 0;
    font-weight: 600;
}
.avatar.ai   { background: #86BC25; color: #fff; }
.avatar.user { background: #012169; color: #fff; }

/* Bolha */
.bubble {
    max-width: 72%;
    padding: 12px 16px;
    border-radius: 18px;
    font-size: 14px;
    line-height: 1.65;
    word-break: break-word;
}
.bubble.ai {
    background: #1e1e2e;
    color: #e0e0e0;
    border-bottom-left-radius: 4px;
}
.bubble.user {
    background: #012169;
    color: #fff;
    border-bottom-right-radius: 4px;
}

/* Perguntas rápidas */
.quick-grid {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin: 8px 0 20px 0;
}
.quick-label {
    font-size: 13px;
    color: #888;
    margin-bottom: 6px;
}

/* KPI cards section */
.kpi-section-title {
    font-size: 13px;
    color: #86BC25;
    font-weight: 600;
    letter-spacing: 0.5px;
    text-transform: uppercase;
    margin: 16px 0 8px 0;
    border-bottom: 1px solid #86BC2533;
    padding-bottom: 4px;
}
</style>
"""

# ─────────────────────────────────────────────────────────────────────────────
# RESUMO DE DADOS
# ─────────────────────────────────────────────────────────────────────────────

def _resumo_df(df: pd.DataFrame | None) -> str:
    if df is None or df.empty:
        return "Nenhum dado disponível no momento."

    linhas = [f"Total de registros: {len(df)}"]

    if "mes_ref" in df.columns:
        datas = df["mes_ref"].dropna().sort_values()
        if not datas.empty:
            linhas.append(
                f"Período: {datas.min().strftime('%b/%Y')} → {datas.max().strftime('%b/%Y')}"
            )

    linhas.append("\n--- TOTAIS CONSOLIDADOS ---")
    metricas = ["receita_liquida", "receita_prevista", "allowance",
                "contingencia", "desvio_abs", "atingimento_pct", "receita_ajustada"]
    for col in metricas:
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
            dev_abs = rl - rp
            dev_pct = (dev_abs / rp * 100) if rp != 0 else 0
            ating = (rl / rp * 100) if rp != 0 else 0
            al = grp["allowance"].sum() if "allowance" in grp else 0
            co = grp["contingencia"].sum() if "contingencia" in grp else 0
            raj = rl - al - co
            linhas.append(
                f"Área {area}: receita_liquida=R${rl:,.0f} | receita_prevista=R${rp:,.0f} | "
                f"desvio_abs=R${dev_abs:,.0f} | desvio_pct={dev_pct:.1f}% | "
                f"atingimento={ating:.1f}% | receita_ajustada=R${raj:,.0f}"
            )

    if "sigla_sub_area" in df.columns:
        linhas.append("\n--- POR SUB ÁREA ---")
        for sub, grp in df.groupby("sigla_sub_area"):
            rl = grp["receita_liquida"].sum() if "receita_liquida" in grp else 0
            rp = grp["receita_prevista"].sum() if "receita_prevista" in grp else 0
            dev_pct = ((rl - rp) / rp * 100) if rp != 0 else 0
            ating = (rl / rp * 100) if rp != 0 else 0
            linhas.append(
                f"Sub área {sub}: receita_liquida=R${rl:,.0f} | receita_prevista=R${rp:,.0f} | "
                f"desvio_pct={dev_pct:.1f}% | atingimento={ating:.1f}%"
            )

    if "mes_ref" in df.columns and "receita_liquida" in df.columns:
        linhas.append("\n--- POR MÊS (últimos 6) ---")
        df_mes = (
            df.groupby("mes_ref")[["receita_liquida", "receita_prevista"]]
            .sum().sort_index().tail(6)
        )
        for mes, row in df_mes.iterrows():
            rl = row.get("receita_liquida", 0)
            rp = row.get("receita_prevista", 0)
            dev_pct = ((rl - rp) / rp * 100) if rp != 0 else 0
            linhas.append(
                f"{mes.strftime('%b/%Y')}: receita_liquida=R${rl:,.0f} | "
                f"receita_prevista=R${rp:,.0f} | desvio_pct={dev_pct:.1f}%"
            )

    if "projeto" in df.columns and "receita_liquida" in df.columns:
        linhas.append("\n--- TOP 5 PROJETOS POR RECEITA LÍQUIDA ---")
        df_proj = (
            df.groupby("projeto")["receita_liquida"]
            .sum().sort_values(ascending=False).head(5)
        )
        for proj, val in df_proj.items():
            linhas.append(f"Projeto {proj}: receita_liquida=R${val:,.0f}")

    return "\n".join(linhas)


# ─────────────────────────────────────────────────────────────────────────────
# CHAMADA DE API
# ─────────────────────────────────────────────────────────────────────────────

def _chamar_api(messages: list[dict], contexto_df: str) -> str:
    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        return (
            "⚠️ A variável **GROQ_API_KEY** não está configurada.\n\n"
            "1. Acesse https://console.groq.com\n"
            "2. Clique em **API Keys → Create API Key**\n"
            "3. Adicione no arquivo `.env`:\n"
            "```\nGROQ_API_KEY=gsk_sua-chave-aqui\n```"
        )

    client = Groq(api_key=api_key)
    system_com_contexto = (
        _SYSTEM_PROMPT + "\n\n## DADOS REAIS DO DASHBOARD\n" + contexto_df
    )
    msgs_api = [{"role": "system", "content": system_com_contexto}]
    for m in messages:
        msgs_api.append({"role": m["role"], "content": m["content"]})

    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=msgs_api,
        max_tokens=1000,
    )
    return response.choices[0].message.content


def _tentar_parse_kpi(texto: str) -> dict | None:
    try:
        inicio = texto.index("{")
        fim = texto.rindex("}") + 1
        candidato = json.loads(texto[inicio:fim])
        if candidato.get("acao") == "adicionar_kpi":
            return candidato
    except (ValueError, json.JSONDecodeError):
        pass
    return None


def _render_bubble(role: str, content: str):
    """Renderiza uma bolha de chat com avatar e alinhamento correto."""
    is_user = role == "user"
    avatar_class = "user" if is_user else "ai"
    bubble_class  = "user" if is_user else "ai"
    avatar_letter = "V" if is_user else "IA"
    row_class     = "user" if is_user else "ai"

    # Converte markdown básico para HTML simples
    import re
    html = content
    # negrito
    html = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", html)
    # bullets
    lines = html.split("\n")
    out = []
    for line in lines:
        if line.strip().startswith("* ") or line.strip().startswith("- "):
            out.append(f"<li>{line.strip()[2:]}</li>")
        else:
            out.append(line + "<br>" if line.strip() else "")
    html = "\n".join(out)
    html = re.sub(r"(<li>.*</li>\n?)+", lambda m: f"<ul style='margin:6px 0 6px 16px;padding:0'>{m.group()}</ul>", html)

    st.markdown(
        f"""
        <div class="chat-row {row_class}">
            <div class="avatar {avatar_class}">{avatar_letter}</div>
            <div class="bubble {bubble_class}">{html}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
# COMPONENTE STREAMLIT PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def render_kpi_agent(df: pd.DataFrame | None = None):
    if "kpi_agent_messages" not in st.session_state:
        st.session_state.kpi_agent_messages = []
    if "kpi_agent_kpis" not in st.session_state:
        st.session_state.kpi_agent_kpis = []

    # Injeta CSS
    st.markdown(_CHAT_CSS, unsafe_allow_html=True)

    st.markdown(
        '<div class="sec-header">Assistente de KPIs — IA Financeira</div>',
        unsafe_allow_html=True,
    )
    st.caption(
        "Pergunte sobre qualquer KPI, solicite cálculos ou peça para adicionar "
        "novas métricas ao painel. O agente conhece as fórmulas financeiras "
        "e os dados reais do dashboard."
    )

    # ── KPIs adicionados pela IA ──────────────────────────────────────────────
    if st.session_state.kpi_agent_kpis:
        st.markdown('<div class="kpi-section-title">KPIs adicionados pela IA</div>', unsafe_allow_html=True)
        cols = st.columns(min(len(st.session_state.kpi_agent_kpis), 4))
        for i, kpi in enumerate(st.session_state.kpi_agent_kpis):
            with cols[i % 4]:
                st.metric(
                    label=kpi.get("nome", "KPI"),
                    value=kpi.get("valor", "—"),
                    help=f"Fórmula: {kpi.get('formula', '—')}\n\nContexto: {kpi.get('contexto', '—')}",
                )
        if st.button("Limpar KPIs", key="btn_limpar_kpis"):
            st.session_state.kpi_agent_kpis = []
            st.rerun()
        st.divider()

    # ── Perguntas rápidas ─────────────────────────────────────────────────────
    if not st.session_state.kpi_agent_messages:
        st.markdown('<div class="quick-label">Comece com uma dessas perguntas:</div>', unsafe_allow_html=True)
        perguntas = [
            "Adicione um KPI de atingimento médio por área",
            "Qual o desvio percentual total de SL01?",
            "Calcule a receita ajustada consolidada",
            "Sugira 3 KPIs para apresentar ao board",
            "O que é allowance neste contexto?",
        ]
        cols = st.columns(len(perguntas))
        for i, perg in enumerate(perguntas):
            with cols[i]:
                if st.button(perg, key=f"quick_{i}", use_container_width=True):
                    st.session_state["_kpi_quick"] = perg
                    st.rerun()
        st.markdown("---")

    # ── Processar pergunta rápida ─────────────────────────────────────────────
    if "_kpi_quick" in st.session_state:
        pergunta = st.session_state.pop("_kpi_quick")
        _processar_mensagem(pergunta, df)
        st.rerun()

    # ── Histórico de mensagens em bolhas ──────────────────────────────────────
    st.markdown('<div class="chat-wrapper">', unsafe_allow_html=True)
    for msg in st.session_state.kpi_agent_messages:
        _render_bubble(msg["role"], msg["content"])
    st.markdown('</div>', unsafe_allow_html=True)

    # ── Input ─────────────────────────────────────────────────────────────────
    prompt = st.chat_input(
        "Digite sua pergunta ou peça um KPI (ex: 'Adicione margem operacional por área')"
    )
    if prompt:
        _processar_mensagem(prompt, df)
        st.rerun()

    # ── Limpar conversa ───────────────────────────────────────────────────────
    if st.session_state.kpi_agent_messages:
        st.markdown("")
        if st.button("🗑 Limpar conversa", key="btn_limpar_chat"):
            st.session_state.kpi_agent_messages = []
            st.rerun()


def _processar_mensagem(prompt: str, df: pd.DataFrame | None):
    st.session_state.kpi_agent_messages.append({"role": "user", "content": prompt})
    contexto = _resumo_df(df)
    historico = list(st.session_state.kpi_agent_messages)

    with st.spinner("Analisando..."):
        resposta = _chamar_api(historico, contexto)

    kpi_json = _tentar_parse_kpi(resposta)
    if kpi_json:
        st.session_state.kpi_agent_kpis.append(kpi_json)

    st.session_state.kpi_agent_messages.append(
        {"role": "assistant", "content": resposta}
    )