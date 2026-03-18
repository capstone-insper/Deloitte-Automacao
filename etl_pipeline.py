import pandas as pd
import numpy as np

BASE_OPERACIONAL = "data/base_operacional.xlsx"
BASE_ORCAMENTO = "data/base_orcamento.xlsx"

OUTPUT_DATASET = "output/dataset_final.csv"

def extract_data():

    print("Extraindo dados...")

    df1 = pd.read_excel(BASE_OPERACIONAL)
    df2 = pd.read_excel(BASE_ORCAMENTO)

    print("Colunas arquivo 1:")
    print(df1.columns)

    print("Colunas arquivo 2:")
    print(df2.columns)

    # detectar base orçamento
    if "Type" in df1.columns or "Type" in df1.columns.str.lower():
        df_orcamento = df1
        df_operacional = df2
    else:
        df_orcamento = df2
        df_operacional = df1

    return df_operacional, df_orcamento

def clean_operational(df):

    print("Limpando base operacional...")

    # padronizar nomes das colunas
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace(" ", "_")
    df.columns = df.columns.str.replace("/", "_")
    df.columns = df.columns.str.lower()

    print("Colunas normalizadas:")
    print(df.columns)

    # converter coluna de data
    if "mês_ano" in df.columns:
        df["mês_ano"] = pd.to_datetime(df["mês_ano"], errors="coerce")

    # colunas que devem ser numéricas
    numeric_cols = [
        "receita_prevista",
        "receita_líquida",
        "allowance",
        "contingência"
    ]

    for col in numeric_cols:

        if col in df.columns:

            df[col] = (
                df[col]
                .astype(str)
                .str.replace(",", ".", regex=False)
                .str.replace(" ", "", regex=False)
            )

            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df

def clean_budget(df):

    print("Transformando base de orçamento...")

    df.columns = df.columns.str.lower()

    month_columns = df.columns[2:]

    df_long = df.melt(
        id_vars=["area", "type"],
        value_vars=month_columns,
        var_name="mes",
        value_name="valor"
    )

    return df_long

def validate_data(df):

    print("Validando dados...")

    # ID quinzena válido
    if "id_quinzena" in df.columns:
        df = df[df["id_quinzena"].isin([1, 2])]
    else:
        print("Aviso: coluna id_quinzena não encontrada")

    # checar valores negativos
    numeric_cols = [
        "receita_prevista",
        "receita_líquida",
        "allowance",
        "contingência"
    ]

    for col in numeric_cols:
        if col in df.columns:
            if (df[col] < 0).any():
                print(f"Aviso: valores negativos em {col}")

    return df


def create_metrics(df):

    print("Criando métricas...")

    df["desvio_receita"] = df["receita_líquida"] - df["receita_prevista"]

    df["receita_ajustada"] = (
        df["receita_líquida"]
        - df["allowance"]
        - df["contingência"]
    )

    df["atingimento"] = np.where(
        df["receita_prevista"] > 0,
        df["receita_líquida"] / df["receita_prevista"],
        np.nan
    )

    return df

def create_summary(df):

    print("Criando agregações analíticas...")

    receita_projeto = (
        df.groupby("projeto")
        .agg({
            "receita_líquida": "sum",
            "receita_prevista": "sum",
            "desvio_receita": "sum"
        })
        .reset_index()
    )

    receita_area = (
        df.groupby("area")
        .agg({
            "receita_líquida": "sum"
        })
        .reset_index()
    )

    return receita_projeto, receita_area


def load_data(df):

    print("Exportando dataset final...")

    df.to_csv(OUTPUT_DATASET, index=False)

    print("Dataset criado:", OUTPUT_DATASET)


def run_pipeline():

    df_operacional, df_orcamento = extract_data()

    df_operacional = clean_operational(df_operacional)

    df_orcamento = clean_budget(df_orcamento)

    df_operacional = validate_data(df_operacional)

    df_operacional = create_metrics(df_operacional)

    receita_projeto, receita_area = create_summary(df_operacional)

    load_data(df_operacional)

    print("\nETL concluído com sucesso.")


# =========================

if __name__ == "__main__":
    run_pipeline()