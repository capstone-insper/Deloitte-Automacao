"""
kpi_agent.py
============
Agente de KPIs financeiros para o Dashboard Executivo — Deloitte.
Chame `render_kpi_agent(df_op)` dentro de uma aba do Streamlit.

Variável de ambiente obrigatória (defina no .env):
    ANTHROPIC_API_KEY=sk-ant-...
"""

import os
import json

import anthropic
import numpy as np
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# SYSTEM PROMPT — base de conhecimento financeiro + contexto do projeto
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
| ROIC               | NOPAT / Capital Investido × 100                                |
| CAC                | (Custo Marketing + Vendas) / Novos Clientes                    |
| LTV                | Ticket Médio × Frequência × Tempo de Retenção                  |
| Churn Rate         | Clientes Perdidos / Clientes no Início × 100                   |
| MRR                | Receita Mensal Recorrente                                      |
| ARR                | MRR × 12                                                       |
| NPS                | % Promotores − % Detratores                                    |
| Break-even         | Custos Fixos / Margem de Contribuição Unitária                 |

## CAMPOS DO PROJETO — BASE OPERACIONAL (data1_csv.txt)
- **Ajuste**: receita reconhecida fora de débito de horas (campo opcional)
- **Allowance**: lançamento contábil a ser estornado no mês seguinte (≥ 0)
- **Contingência**: reserva de receita a estornar (≥ 0)
- **Funcionário**: tipo de colaborador — STAFF ou EXECUTIVO
- **Centro de Custo**: CC1, CC2, CC3
- **Projeto**: P1 até P10
- **Área**: SL01, SL02
- **Sub Área**: CO (Consultoria), AI (Inteligência Artificial), En (Engenharia)
- **Mês/Ano**: período de competência mensal
- **ID Quinzena**: 1 = dias 1–15 / 2 = dias 16–fim do mês
- **Receita Prevista**: orçado / estimado para o período
- **Receita Líquida**: realizado após deduções e ajustes

## CAMPOS DO PROJETO — BASE ORÇAMENTÁRIA (BookService.txt)
- **Area**: SL01, SL02
- **Type**: Receita | Custo | Margin
- Colunas mensais de jun/25 a mai/26 com valores orçados

## REGRAS DE NEGÓCIO
- Receita Ajustada = Receita Líquida − Allowance − Contingência
- Custo Total = Allowance + Contingência
- Atingimento: meta de referência é 100%
- Desvio positivo (Líquida > Prevista) = favorável
- Quinzenas são a granularidade mínima; mês = soma das duas quinzenas

## INSTRUÇÕES DE COMPORTAMENTO
- Responda sempre em português brasileiro
- Use os dados reais do contexto quando fornecidos
- Mostre o cálculo passo a passo quando solicitado
- Quando o usuário pedir para "adicionar um KPI" ou "incluir uma métrica",
  retorne obrigatoriamente um bloco JSON no seguinte formato (antes ou depois
  da explicação textual):
  {"acao": "adicionar_kpi", "nome": "...", "formula": "...", "valor": "...", "contexto": "..."}
- Para perguntas conceituais, seja direto e didático, com exemplos numéricos
- Máximo 250 palavras por resposta, salvo quando o usuário pedir detalhes
"""

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS INTERNOS
# ─────────────────────────────────────────────────────────────────────────────

def _resumo_df(df: pd.DataFrame | None) -> str:
    """Gera um resumo compacto do DataFrame para injetar no prompt como contexto."""
    if df is None or df.empty:
        return "Nenhum dado disponível no momento."

    linhas = [f"Total de registros: {len(df)}"]

    num_cols = df.select_dtypes(include="number").columns.tolist()
    for col in num_cols[:8]:
        s = df[col].dropna()
        if len(s) == 0:
            continue
        linhas.append(
            f"{col}: soma={s.sum():,.0f} | média={s.mean():,.0f} | "
            f"min={s.min():,.0f} | max={s.max():,.0f}"
        )

    cat_cols = df.select_dtypes(include="object").columns.tolist()
    for col in cat_cols[:5]:
        vals = df[col].dropna().unique().tolist()[:10]
        linhas.append(f"{col}: {vals}")

    if "mes_ref" in df.columns:
        datas = df["mes_ref"].dropna().sort_values()
        if not datas.empty:
            linhas.append(
                f"Período: {datas.min().strftime('%b/%Y')} → {datas.max().strftime('%b/%Y')}"
            )

    return "\n".join(linhas)


def _chamar_api(messages: list[dict], contexto_df: str) -> str:
    """Chama a API da Anthropic e retorna o texto da resposta."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return (
            "⚠️ A variável **ANTHROPIC_API_KEY** não está configurada.\n\n"
            "Crie um arquivo `.env` na raiz do projeto com:\n"
            "```\nANTHROPIC_API_KEY=sk-ant-sua-chave-aqui\n```"
        )

    client = anthropic.Anthropic(api_key=api_key)

    system_com_contexto = (
        _SYSTEM_PROMPT
        + "\n\n## DADOS ATUAIS DO DASHBOARD\n"
        + contexto_df
    )

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1000,
        system=system_com_contexto,
        messages=messages,
    )
    return response.content[0].text


def _tentar_parse_kpi(texto: str) -> dict | None:
    """Tenta extrair um JSON de KPI da resposta do agente."""
    try:
        inicio = texto.index("{")
        fim = texto.rindex("}") + 1
        candidato = json.loads(texto[inicio:fim])
        if candidato.get("acao") == "adicionar_kpi":
            return candidato
    except (ValueError, json.JSONDecodeError):
        pass
    return None


# ─────────────────────────────────────────────────────────────────────────────
# COMPONENTE STREAMLIT PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def render_kpi_agent(df: pd.DataFrame | None = None):
    """
    Renderiza a aba do Assistente de KPIs dentro do Streamlit.

    Parâmetros
    ----------
    df : pd.DataFrame | None
        O DataFrame processado pelo pipeline ETL (df_op).
        Se None, o agente responde sem contexto de dados reais.
    """

    # ── Estado de sessão ──────────────────────────────────────────────────────
    if "kpi_agent_messages" not in st.session_state:
        st.session_state.kpi_agent_messages = []
    if "kpi_agent_kpis" not in st.session_state:
        st.session_state.kpi_agent_kpis = []

    # ── Cabeçalho ─────────────────────────────────────────────────────────────
    st.markdown(
        '<div class="sec-header">Assistente de KPIs — IA Financeira</div>',
        unsafe_allow_html=True,
    )
    st.caption(
        "Pergunte sobre qualquer KPI, solicite cálculos ou peça para adicionar "
        "novas métricas ao painel. O agente já conhece as fórmulas financeiras "
        "e os dados reais do dashboard."
    )

    # ── KPIs adicionados pelo agente ──────────────────────────────────────────
    if st.session_state.kpi_agent_kpis:
        st.markdown(
            '<div class="sec-header">KPIs adicionados pela IA</div>',
            unsafe_allow_html=True,
        )
        n = len(st.session_state.kpi_agent_kpis)
        cols = st.columns(min(n, 4))
        for i, kpi in enumerate(st.session_state.kpi_agent_kpis):
            with cols[i % 4]:
                st.metric(
                    label=kpi.get("nome", "KPI"),
                    value=kpi.get("valor", "—"),
                    help=(
                        f"**Fórmula:** {kpi.get('formula', '—')}\n\n"
                        f"**Contexto:** {kpi.get('contexto', '—')}"
                    ),
                )

        if st.button("Limpar KPIs", key="btn_limpar_kpis"):
            st.session_state.kpi_agent_kpis = []
            st.rerun()

        st.divider()

    # ── Perguntas rápidas (visíveis só antes do primeiro chat) ────────────────
    if not st.session_state.kpi_agent_messages:
        st.markdown("**Comece com uma dessas perguntas:**")
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

    # ── Processar pergunta rápida ──────────────────────────────────────────────
    if "_kpi_quick" in st.session_state:
        pergunta = st.session_state.pop("_kpi_quick")
        _processar_mensagem(pergunta, df)
        st.rerun()

    # ── Histórico de conversa ─────────────────────────────────────────────────
    for msg in st.session_state.kpi_agent_messages:
        role = "user" if msg["role"] == "user" else "assistant"
        with st.chat_message(role):
            st.markdown(msg["content"])

    # ── Input do usuário ──────────────────────────────────────────────────────
    prompt = st.chat_input(
        "Digite sua pergunta ou peça um KPI (ex: 'Adicione margem operacional por área')"
    )
    if prompt:
        _processar_mensagem(prompt, df)
        st.rerun()

    # ── Botão de limpar conversa ──────────────────────────────────────────────
    if st.session_state.kpi_agent_messages:
        st.markdown("")
        if st.button("🗑 Limpar conversa", key="btn_limpar_chat"):
            st.session_state.kpi_agent_messages = []
            st.rerun()


def _processar_mensagem(prompt: str, df: pd.DataFrame | None):
    """Adiciona a mensagem ao histórico, chama a API e salva a resposta."""
    st.session_state.kpi_agent_messages.append({"role": "user", "content": prompt})

    contexto = _resumo_df(df)
    historico_api = [
        {"role": m["role"], "content": m["content"]}
        for m in st.session_state.kpi_agent_messages
    ]

    with st.spinner("Analisando..."):
        resposta = _chamar_api(historico_api, contexto)

    kpi_json = _tentar_parse_kpi(resposta)
    if kpi_json:
        st.session_state.kpi_agent_kpis.append(kpi_json)

    st.session_state.kpi_agent_messages.append(
        {"role": "assistant", "content": resposta}
    )