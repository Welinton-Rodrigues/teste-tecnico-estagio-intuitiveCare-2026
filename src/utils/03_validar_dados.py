"""
03_validar_dados.py

Propósito:
 - Aplicar validações básicas sobre o consolidado gerado em 02, tornando
     os dados mais confiáveis para análises posteriores.

Validações e decisões:
 - CNPJ: validação de formato (14 dígitos). Para o escopo do teste, isso
     detecta a maioria dos problemas de entrada; validação completa de dígitos
     de verificação pode ser adicionada se necessário.
 - Razão social: sinalizamos registros sem `RazaoSocial` em vez de excluí-los,
     preservando dados para posterior enriquecimento.
 - Valores: removemos registros com `ValorDespesas` <= 0 para focar análises
     em despesas reais (trade-off: evita ruído por zeros/negativos).

Formato de saída:
 - CSV com encoding `utf-8-sig` para compatibilidade com Excel.
"""

import pandas as pd
from pathlib import Path

# Configurações
INPUT_PATH = Path("data/output/consolidado_despesas.csv")
OUTPUT_PATH = Path("data/processed/dados_validados.csv")
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

def validar_cnpj(cnpj):
    # Lógica simples de conferência de tamanho
    # Para um teste de estagiário, verificar se tem 14 dígitos já é um bom começo
    # normalizar e checar comprimento (não fazemos dígitos verificadores aqui)
    cnpj = str(cnpj).replace(".", "").replace("/", "").replace("-", "")
    return len(cnpj) == 14

def executar_validacao():
    print("--- Iniciando Validação de Dados (Item 2.1) ---")

    try:
        df = pd.read_csv(INPUT_PATH)
    except FileNotFoundError:
        print(f"Arquivo de entrada não encontrado: {INPUT_PATH}")
        return
    except Exception as e:
        print(f"Erro ao ler {INPUT_PATH}: {e}")
        return

    cols = set(df.columns)

    # Enriquecer com CNPJ / RazaoSocial a partir de um mapeamento opcional
    mapping_path = Path("data/raw/registro_ans_info.csv")
    if mapping_path.exists():
        try:
            mapping = pd.read_csv(mapping_path, dtype=str)
            if 'RegistroANS' in mapping.columns:
                mapping['RegistroANS'] = mapping['RegistroANS'].astype(str)
                df['RegistroANS'] = df['RegistroANS'].astype(str)
                df = df.merge(mapping[['RegistroANS', 'CNPJ', 'RazaoSocial']], on='RegistroANS', how='left')
                print(f"Enriquecido com mapeamento: {mapping_path}")
            else:
                print(f"Mapeamento encontrado em {mapping_path} mas falta coluna 'RegistroANS' — ignorando.")
        except Exception as e:
            print(f"Erro ao ler mapeamento {mapping_path}: {e} — seguindo sem enriquecimento.")
    else:
        # Garantir colunas presentes mesmo que vazias
        if 'CNPJ' not in cols:
            df['CNPJ'] = ""
        if 'RazaoSocial' not in cols:
            df['RazaoSocial'] = ""

    cols = set(df.columns)

    # 1. Validação de Razão Social não vazia (se existir)
    if 'RazaoSocial' in cols:
        # manter registros mesmo sem RazaoSocial — apenas avisar
        missing_rs = df['RazaoSocial'].isna() | (df['RazaoSocial'].astype(str).str.strip() == "")
        if missing_rs.any():
            print(f"Atenção: {missing_rs.sum()} registros sem 'RazaoSocial'.")
    else:
        print("Atenção: coluna 'RazaoSocial' não encontrada — pulando essa validação.")

    # 2. Validação de CNPJ (Apenas formato para simplificar)
    if 'CNPJ' in cols:
        df['CNPJ_Valido'] = df['CNPJ'].apply(validar_cnpj)
    else:
        print("Atenção: coluna 'CNPJ' não encontrada — marcando CNPJ_Valido=False para todos.")
        df['CNPJ_Valido'] = False

    # 3. Tratamento de Valores Positivos
    if 'ValorDespesas' not in cols:
        print("Erro: coluna 'ValorDespesas' não encontrada no consolidado — abortando validação.")
        return

    df['ValorDespesas'] = pd.to_numeric(df['ValorDespesas'], errors='coerce')

    total_antes = len(df)
    df_limpo = df[df['ValorDespesas'] > 0].copy()

    print(f"Registros originais: {total_antes}")
    print(f"Registros após remover zerados/negativos: {len(df_limpo)}")

    # Preparar saída com colunas requisitadas na ordem esperada
    # Colunas obrigatórias: CNPJ, RazaoSocial, Trimestre, Ano, ValorDespesas
    for c in ['CNPJ', 'RazaoSocial', 'Trimestre', 'Ano', 'ValorDespesas']:
        if c not in df_limpo.columns:
            df_limpo[c] = "" if c in ['CNPJ', 'RazaoSocial'] else None

    output_df = df_limpo[['CNPJ', 'RazaoSocial', 'Trimestre', 'Ano', 'ValorDespesas']].copy()

    # Salvar para a próxima etapa
    try:
        output_df.to_csv(OUTPUT_PATH, index=False, encoding='utf-8-sig')
        print(f"Dados validados salvos em: {OUTPUT_PATH}")
    except Exception as e:
        print(f"Erro ao salvar {OUTPUT_PATH}: {e}")

if __name__ == "__main__":
    executar_validacao()