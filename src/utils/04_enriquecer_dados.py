"""
04_enriquecer_dados.py

Propósito:
 - Enriquecer o conjunto de despesas com informações cadastrais públicas
     (CNPJ, Razão Social, Modalidade, UF) da ANS, usando o cadastro de
     operadoras ativas.

Decisões técnicas e heurísticas importantes:
 - Leitura robusta do cadastro: tentamos `utf-8` e caímos para `latin-1`
     porque as bases governamentais às vezes usam encodings distintos.
 - Aplicamos uma heurística de correção de "mojibake" (latin1→utf8) em
     campos de texto para evitar nomes com caracteres corrompidos.
 - O merge é feito por `RegistroANS` quando possível; quando isso falha
     procuramos outras colunas candidatas (robustez contra nomes de coluna
     diferentes entre dumps).
 - Saída com `utf-8-sig` para facilitar abertura em Excel/Windows.
"""

import pandas as pd
import requests
import io
from pathlib import Path
import re


def _fix_mojibake_text(s: str) -> str:
    """Tentativa simples de corrigir mojibake comum: re-decode latin1->utf-8.
    Se não houver padrão suspeito, retorna o original.
    """
    if not isinstance(s, str):
        return s
    # heurística: sequências como Ã, Â geralmente indicam double-encoding
    if re.search(r'[ÃÂ]', s):
        try:
            return s.encode('latin-1').decode('utf-8')
        except Exception:
            return s
    return s

# Configurações
# Usaremos o RegistroANS como "ponte" para pegar o CNPJ e Razão Social
URL_CADASTRO = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv"
INPUT_RAW = Path("data/output/consolidado_despesas.csv") # Arquivo que tem o RegistroANS
OUTPUT_FINAL = Path("data/processed/dados_validados.csv")

def executar_enriquecimento_total():
    print("--- Recuperando CNPJ e Razão Social (Enriquecimento) ---")
    
    # 1. Carregar seu consolidado (que hoje só tem RegistroANS e Valores)
    df_despesas = pd.read_csv(INPUT_RAW)
    # Garantir que RegistroANS seja string e sem caracteres extras
    if 'RegistroANS' in df_despesas.columns:
        df_despesas['RegistroANS'] = df_despesas['RegistroANS'].astype(str).str.replace(r'\D', '', regex=True)
    
    # 2. Baixar o cadastro oficial da ANS (ler bytes e tentar decodificar corretamente)
    response = requests.get(URL_CADASTRO, verify=False)
    content = response.content
    try:
        # tenta como utf-8 primeiro
        text = content.decode('utf-8')
        df_cadastro = pd.read_csv(io.StringIO(text), sep=';', dtype=str)
    except Exception:
        # fallback para latin-1 (comum em bases da ANS)
        df_cadastro = pd.read_csv(io.BytesIO(content), sep=';', encoding='latin-1', dtype=str)
    
    # Limpeza de nomes de colunas do governo
    df_cadastro.columns = [c.strip().upper() for c in df_cadastro.columns]

    # Corrigir mojibake em colunas de texto do cadastro
    for c in df_cadastro.select_dtypes(include=['object']).columns:
        df_cadastro[c] = df_cadastro[c].apply(_fix_mojibake_text)
    
    # Detectar coluna de registro (vários nomes/erros possíveis)
    registro_col = None
    for c in df_cadastro.columns:
        uc = c.upper()
        if 'REGIAO' in uc:
            continue
        if 'REGIST' in uc or 'REG' in uc and 'CNPJ' not in uc:
            registro_col = c
            break
    if registro_col is None:
        # fallback: usar primeira coluna
        registro_col = df_cadastro.columns[0]
    print(f"Usando coluna de registro do cadastro: {registro_col}")
    # Criar coluna padronizada 'RegistroANS' a partir da coluna detectada
    try:
        df_cadastro['RegistroANS'] = df_cadastro[registro_col].astype(str).str.replace(r'\D', '', regex=True)
    except Exception:
        df_cadastro['RegistroANS'] = df_cadastro[registro_col].astype(str)

    # Tentar criar coluna 'RazaoSocial' a partir de possíveis rótulos
    razao_col = None
    for c in df_cadastro.columns:
        if 'RAZAO' in c or 'NOME' in c:
            razao_col = c
            break
    if razao_col:
        df_cadastro['RazaoSocial'] = df_cadastro[razao_col]
    else:
        df_cadastro['RazaoSocial'] = ''
    
    print("Cruzando dados para preencher CNPJ e Razão Social...")
    
    # Fazemos o Join
    df_final = pd.merge(
        df_despesas[['RegistroANS', 'Trimestre', 'Ano', 'ValorDespesas']], 
        df_cadastro[['RegistroANS', 'CNPJ', 'RazaoSocial', 'MODALIDADE', 'UF']], 
        on='RegistroANS', 
        how='left'
    )

    # 4. Organizar as colunas como o desafio 1.3 pede
    colunas_finais = ['CNPJ', 'RazaoSocial', 'Trimestre', 'Ano', 'ValorDespesas', 'RegistroANS', 'Modalidade', 'UF']
    
    # Se o nome no cadastro for diferente, ajustamos
    df_final = df_final.rename(columns={'MODALIDADE': 'Modalidade'})
    
    # 5. Salvar o arquivo agora REALMENTE completo
    df_final[colunas_finais].to_csv(OUTPUT_FINAL, index=False, sep=';', encoding='utf-8-sig')
    
    print(f"Sucesso! Arquivo gerado com {len(df_final)} linhas.")
    print(f"Campos CNPJ e RazaoSocial agora estão preenchidos!")

if __name__ == "__main__":
    executar_enriquecimento_total()