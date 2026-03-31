"""ETL standalone: lê dados brutos e gera arquivos CSV tratados."""

import re
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
PASTA_ENTRADA = ROOT / "entrada"
PASTA_SAIDA = ROOT / "output" / "vitoria"
PASTA_SAIDA.mkdir(parents=True, exist_ok=True)
ENCODINGS = ["utf-16", "utf-16-le", "utf-8-sig", "utf-8", "latin1"]
MESES_PT = {
    "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
    "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12,
}


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


def carregar_dados() -> tuple[pd.DataFrame, pd.DataFrame]:
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

    rl = df_op.get("receita_liquida", pd.Series(dtype=float))
    rp = df_op.get("receita_prevista", pd.Series(dtype=float))
    al = df_op.get("allowance", pd.Series(np.zeros(len(df_op)), index=df_op.index))
    co = df_op.get("contingencia", pd.Series(np.zeros(len(df_op)), index=df_op.index))

    df_op["custo_total"] = al + co
    df_op["desvio_abs"] = rl - rp
    df_op["desvio_pct"] = np.where(rp != 0, (rl - rp) / rp * 100, np.nan)
    df_op["atingimento_pct"] = np.where(rp != 0, rl / rp * 100, np.nan)
    df_op["receita_ajustada"] = rl - al - co

    df_orc_raw = _carregar_txt_raw(PASTA_ENTRADA / "BookService.txt")
    df_orc = pd.DataFrame()

    if not df_orc_raw.empty:
        df_orc_raw.columns = [normalizar_coluna(c) for c in df_orc_raw.columns]
        col_a = _col(df_orc_raw, "area")
        col_t = _col(df_orc_raw, "type", "tipo")

        if col_a and col_t:
            df_orc_raw = df_orc_raw.rename(columns={col_a: "area", col_t: "tipo"})
            id_vars = ["area", "tipo"]
            mes_cols = [c for c in df_orc_raw.columns
                        if c not in id_vars and re.match(r"[a-z]{3}\d{2}", c)]
            if not mes_cols:
                mes_cols = [c for c in df_orc_raw.columns if c not in id_vars]

            if mes_cols:
                df_orc = (
                    df_orc_raw[id_vars + mes_cols]
                    .melt(id_vars=id_vars, var_name="mes_col", value_name="valor")
                )
                df_orc["valor"] = df_orc["valor"].apply(limpar_valor_brl)
                df_orc = df_orc.dropna(subset=["valor"])
                df_orc["mes_ref"] = df_orc["mes_col"].apply(_parse_mes)
                df_orc = df_orc.dropna(subset=["mes_ref"])
                df_orc["area"] = df_orc["area"].astype(str).str.strip()
                df_orc["tipo"] = df_orc["tipo"].astype(str).str.strip()

    return df_op, df_orc


if __name__ == "__main__":
    df_op, df_orc = carregar_dados()
    if df_op.empty:
        print("ERRO: Não foi possível carregar dados operacionais. Verifique a pasta entrada.")
        raise SystemExit(1)

    df_op.to_csv(PASTA_SAIDA / "operacional_tratado.csv", index=False)
    df_orc.to_csv(PASTA_SAIDA / "orcamento_tratado.csv", index=False)
    print("ETL concluído com sucesso: output/vitoria/operacional_tratado.csv e orcamento_tratado.csv")
