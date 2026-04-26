"""
kpi_agent.py
============
Agente de KPIs financeiros para o Dashboard Executivo — Deloitte.
Usa Groq (gratuito) com o modelo llama-3.3-70b-versatile.

Variável de ambiente obrigatória (defina no .env):
    GROQ_API_KEY=gsk_sua-chave-aqui

Histórico salvo em: historico_chat.json (mesma pasta do app_v1.py)
"""

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
- Para perguntas conceituais, seja direto e didático, com exemplos numéricos
- Máximo 250 palavras por resposta, salvo quando o usuário pedir detalhes
"""

# ─────────────────────────────────────────────────────────────────────────────
# CSS
# ─────────────────────────────────────────────────────────────────────────────

_CHAT_CSS = """
<style>
[data-testid="stSidebar"] { background: #0d0d1a !important; border-right: 1px solid #1e1e35 !important; }

.sidebar-header { font-size: 11px; font-weight: 600; color: #86BC25; text-transform: uppercase;
    letter-spacing: 1px; padding: 4px 0 10px 0; border-bottom: 1px solid #1e1e35; margin-bottom: 12px; }

.history-section-label { font-size: 10px; color: #555; text-transform: uppercase;
    letter-spacing: 0.8px; margin: 14px 0 4px 0; }

.history-item { display: flex; align-items: center; gap: 8px; padding: 7px 10px;
    border-radius: 8px; margin-bottom: 2px; }
.history-item.active { background: #1a1a2e; border-left: 2px solid #86BC25; }

.history-title { font-size: 13px; color: #c0c0d0; white-space: nowrap;
    overflow: hidden; text-overflow: ellipsis; flex: 1; }
.history-time  { font-size: 10px; color: #555; flex-shrink: 0; }

.chat-wrapper { display: flex; flex-direction: column; gap: 20px; padding: 8px 0 32px 0; }

.chat-row { display: flex; align-items: flex-end; gap: 10px; }
.chat-row.user { flex-direction: row-reverse; }

.avatar { width: 30px; height: 30px; border-radius: 50%; display: flex; align-items: center;
    justify-content: center; font-size: 12px; font-weight: 700; flex-shrink: 0; }
.avatar.ai   { background: #86BC25; color: #fff; }
.avatar.user { background: #012169; color: #fff; }

.bubble { max-width: 70%; padding: 11px 15px; border-radius: 18px;
    font-size: 14px; line-height: 1.7; word-break: break-word; }
.bubble.ai   { background: #1a1a2e; color: #dde0ee; border-bottom-left-radius: 4px; }
.bubble.user { background: #012169; color: #fff; border-bottom-right-radius: 4px; }
.bubble ul { margin: 6px 0 6px 18px; padding: 0; }
.bubble li { margin-bottom: 2px; }

.kpi-section-title { font-size: 12px; color: #86BC25; font-weight: 600; text-transform: uppercase;
    letter-spacing: 0.5px; margin: 16px 0 8px 0; border-bottom: 1px solid #86BC2530; padding-bottom: 4px; }

.empty-state { text-align: center; padding: 48px 24px; color: #444; }
.empty-state .icon  { font-size: 36px; margin-bottom: 12px; }
.empty-state .title { font-size: 16px; color: #666; margin-bottom: 6px; }
.empty-state .sub   { font-size: 13px; }
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
    linhas = [f"Total de registros: {len(df)}"]
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


def _tentar_parse_kpi(texto: str) -> dict | None:
    try:
        candidato = json.loads(texto[texto.index("{"):texto.rindex("}")+1])
        return candidato if candidato.get("acao") == "adicionar_kpi" else None
    except (ValueError, json.JSONDecodeError):
        return None


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


def _render_bubble(role: str, content: str):
    is_user = role == "user"
    st.markdown(
        f"""<div class="chat-row {'user' if is_user else 'ai'}">
            <div class="avatar {'user' if is_user else 'ai'}">{'V' if is_user else 'IA'}</div>
            <div class="bubble {'user' if is_user else 'ai'}">{_md_to_html(content)}</div>
        </div>""",
        unsafe_allow_html=True,
    )


def _tempo_relativo(ts: datetime) -> str:
    diff = (datetime.now() - ts).total_seconds()
    if diff < 60:    return "agora"
    if diff < 3600:  return f"{int(diff//60)}min"
    if diff < 86400: return f"{int(diff//3600)}h"
    return ts.strftime("%d/%m")


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
    conv = {"id": conv_id, "title": "Nova conversa", "messages": [], "kpis": [], "created_at": datetime.now()}
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
        st.markdown('<div class="sidebar-header">✦ Assistente Delloite</div>', unsafe_allow_html=True)

        if st.button("＋  Nova conversa", key="btn_nova", use_container_width=True):
            _nova_conversa()
            st.session_state._jump_to_assistant = True
            st.rerun()

        if not st.session_state.conversations:
            st.markdown('<div style="font-size:12px;color:#444;margin-top:12px">Nenhuma conversa ainda.</div>', unsafe_allow_html=True)
            return

        hoje, semana, anteriores = [], [], []
        now = datetime.now()
        for c in st.session_state.conversations:
            diff = (now - c["created_at"]).days
            if diff == 0:    hoje.append(c)
            elif diff <= 7:  semana.append(c)
            else:            anteriores.append(c)

        def _render_group(label: str, group: list):
            if not group:
                return
            st.markdown(f'<div class="history-section-label">{label}</div>', unsafe_allow_html=True)
            for c in group:
                is_active = c["id"] == st.session_state.active_conv_id
                tempo = _tempo_relativo(c["created_at"])
                title = c["title"]

                # Linha: botão de seleção + botão de exclusão
                col_btn, col_del = st.columns([5, 1])
                with col_btn:
                    label_btn = f"{'▶ ' if is_active else ''}{title[:32]}"
                    if st.button(label_btn, key=f"sel_{c['id']}", use_container_width=True):
                        st.session_state.active_conv_id = c["id"]
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

        _render_group("Hoje", hoje)
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

    st.markdown('<div class="sec-header">Seu assistente Delloite</div>', unsafe_allow_html=True)
    st.caption("Pergunte sobre qualquer KPI, solicite cálculos ou peça para adicionar novas métricas.")

    # KPIs adicionados
    if conv["kpis"]:
        st.markdown('<div class="kpi-section-title">KPIs adicionados pela IA</div>', unsafe_allow_html=True)
        cols = st.columns(min(len(conv["kpis"]), 4))
        for i, kpi in enumerate(conv["kpis"]):
            with cols[i % 4]:
                st.metric(
                    label=kpi.get("nome", "KPI"),
                    value=kpi.get("valor", "—"),
                    help=f"Fórmula: {kpi.get('formula','—')}\n\nContexto: {kpi.get('contexto','—')}",
                )
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
        st.markdown("**Sugestões:**")
        perguntas = [
            "Adicione KPI de atingimento médio por área",
            "Qual o desvio percentual de SL01?",
            "Calcule a receita ajustada consolidada",
            "Sugira 3 KPIs para o board",
            "O que é allowance?",
        ]
        cols = st.columns(len(perguntas))
        for i, perg in enumerate(perguntas):
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
            _render_bubble(msg["role"], msg["content"])
        st.markdown('</div>', unsafe_allow_html=True)

    # Input
    prompt = st.chat_input("Mensagem para o Assistente de KPIs...")
    if prompt:
        _processar_mensagem(prompt, conv, df)
        st.rerun()


def _processar_mensagem(prompt: str, conv: dict, df: pd.DataFrame | None):
    conv["messages"].append({"role": "user", "content": prompt})
    if len(conv["messages"]) == 1:
        conv["title"] = prompt[:42] + ("…" if len(prompt) > 42 else "")

    with st.spinner("Analisando..."):
        resposta = _chamar_api(conv["messages"], _resumo_df(df))

    kpi_json = _tentar_parse_kpi(resposta)
    if kpi_json:
        conv["kpis"].append(kpi_json)

    conv["messages"].append({"role": "assistant", "content": resposta})
    _salvar_historico()