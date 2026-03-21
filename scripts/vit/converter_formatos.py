import pandas as pd
import os

INPUT_FOLDER = "entrada"
OUTPUT_FOLDER = "padronizado"

OUTPUT_FILES = [
    "base_operacional.xlsx",
    "base_orcamento.xlsx"
]

SUPPORTED_FORMATS = [
    ".csv",
    ".xls",
    ".xlsx",
    ".json",
    ".txt",
    ".parquet"
]

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def read_any_file(filepath):

    ext = os.path.splitext(filepath)[1].lower()

    if ext == ".csv":
        return pd.read_csv(filepath)

    elif ext in [".xls", ".xlsx"]:
        return pd.read_excel(filepath)

    elif ext == ".json":
        return pd.read_json(filepath)

    elif ext == ".txt":

        # tenta múltiplos encodings comuns
        encodings = ["utf-8", "utf-16", "latin1", "ISO-8859-1"]

        for enc in encodings:
            try:
                return pd.read_csv(filepath, sep=None, engine="python", encoding=enc)
            except:
                continue

        raise ValueError("Não foi possível identificar o encoding do arquivo TXT")

    elif ext == ".parquet":
        return pd.read_parquet(filepath)

    else:
        raise ValueError(f"Formato não suportado: {ext}")
    
    
def convert_files():

    print("Procurando arquivos de entrada...")

    files = [
        f for f in os.listdir(INPUT_FOLDER)
        if os.path.splitext(f)[1].lower() in SUPPORTED_FORMATS
    ]

    if len(files) == 0:
        print("Nenhum arquivo encontrado.")
        return

    for i, file in enumerate(files):

        input_path = os.path.join(INPUT_FOLDER, file)

        print(f"Lendo {file}")

        df = read_any_file(input_path)

        # nome de saída padronizado
        if i < len(OUTPUT_FILES):
            output_name = OUTPUT_FILES[i]
        else:
            output_name = f"dataset_{i}.xlsx"

        output_path = os.path.join(OUTPUT_FOLDER, output_name)

        df.to_excel(output_path, index=False)

        print(f"Arquivo convertido → {output_name}")


if __name__ == "__main__":

    convert_files()

    print("\nConversão concluída.")