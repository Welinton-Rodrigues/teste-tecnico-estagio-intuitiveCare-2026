import pandas as pd
import zipfile
import re
import tempfile
import os
import shutil
from pathlib import Path
from typing import Optional, Tuple


"""
02_processar_arquivos.py

Propósito:
 - Percorrer os ZIPs em `data/raw`, extrair arquivos e identificar tabelas
     que representam despesas/sinistros, normalizando e consolidando para um
     CSV único `data/output/consolidado_despesas.csv`.

Decisões técnicas e justificativa:
 - Processamento incremental (arquivo a arquivo) em vez de carregar tudo
     na memória: reduz uso de memória, permite retomada parcial e é mais
     seguro para coleções grandes (trade-off: foco em I/O e latência).
 - Tentamos múltiplos encodings e separadores para lidar com a heterogeneidade
     dos arquivos (CSV/TXT) encontrados nos ZIPs.
 - Heurísticas simples determinam se uma tabela é de despesas (nomes de
     colunas e padrões de valores). Isso é robusto o suficiente para o teste
     e pode ser parametrizado se necessário.
"""

# Configurações de diretórios
RAW_DIR = Path("data/raw")
OUTPUT_DIR = Path("data/output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Arquivo de saída consolidado (escrevemos incrementalmente)
CSV_FINAL = OUTPUT_DIR / "consolidado_despesas.csv"


def _extract_year_quarter_from_name(name: str) -> Optional[Tuple[int, int]]:
    # busca por padrões como '1T2025' ou '2025_1_trimestre' ou 'YYYYMM'
    m = re.search(r"([1-4])T(\d{4})", name, flags=re.IGNORECASE)
    if m:
        return int(m.group(2)), int(m.group(1))
    m = re.search(r"(\d{4})[^0-9]{0,3}([1-4])(?:[_\- ]|$)", name, flags=re.IGNORECASE)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = re.search(r"(\d{6})", name)
    if m:
        y = int(m.group(1)[:4]); mon = int(m.group(1)[4:6])
        if 1 <= mon <= 12:
            return y, (mon - 1) // 3 + 1
    return None


def _try_read_table(path: Path) -> Optional[pd.DataFrame]:
    """Tenta ler um arquivo CSV/TXT/XLSX retornando DataFrame ou None."""
    lower = path.suffix.lower()
    if lower in {".xls", ".xlsx"}:
        try:
            return pd.read_excel(path, dtype=str)
        except Exception:
            return None

    # para CSV/TXT tentamos múltiplos separadores e encodings
    # tentamos alguns encodings comuns no Brasil e em bases governamentais
    encodings = ["utf-8", "latin-1", "cp1252"]
    seps = [";", ",", "\t", "|"]
    for enc in encodings:
        for sep in seps:
            try:
                df = pd.read_csv(path, sep=sep, encoding=enc, dtype=str, engine="python")
                if df.shape[1] > 1:
                    return df
            except Exception:
                continue
    return None


def _is_expense_table(df: pd.DataFrame) -> bool:
    """Heurística para identificar tabelas com despesas/sinistros."""
    cols = {c.strip().upper() for c in df.columns}
    keywords_cols = {
        "CD_CONTA_CONTABIL", "VL_SALDO_FINAL", "VALOR", "REG_ANS", "DESPESA", "SINISTRO"
    }
    if cols & keywords_cols:
        return True

    # fallback: se existir alguma coluna com valores numéricos e col nomes parecidos
    for c in cols:
        if re.search(r"VAL|VLR|VL_", c):
            return True
    return False


def _normalize_and_extract(df: pd.DataFrame, ano: str, trimestre: str) -> Optional[pd.DataFrame]:
    # normaliza colunas para buscar RegistroANS e valor
    col_map = {c.strip().upper(): c for c in df.columns}
    upper_cols = {c.strip().upper(): c for c in df.columns}

    # possíveis nomes para registro e valor
    reg_candidates = ["REG_ANS", "REGANS", "REGISTROANS", "ANS"]
    val_candidates = ["VL_SALDO_FINAL", "VALOR", "VALOR_DESPESA", "VALOR_FINAL", "VLR"]

    reg_col = None
    val_col = None
    for rc in reg_candidates:
        if rc in upper_cols:
            reg_col = upper_cols[rc]
            break
    for vc in val_candidates:
        if vc in upper_cols:
            val_col = upper_cols[vc]
            break

    if reg_col is None or val_col is None:
        return None

    df_sub = df[[reg_col, val_col]].copy()
    df_sub.columns = ["RegistroANS", "ValorDespesas"]

    # limpar e converter ValorDespesas
    df_sub["ValorDespesas"] = (
        df_sub["ValorDespesas"].astype(str)
        .str.replace(r"[^0-9,\.\-]", "", regex=True)
        .str.replace(",", ".")
    )
    try:
        df_sub["ValorDespesas"] = pd.to_numeric(df_sub["ValorDespesas"], errors="coerce")
    except Exception:
        df_sub["ValorDespesas"] = pd.to_numeric(df_sub["ValorDespesas"].str.replace("." , "", regex=False), errors="coerce")

    df_sub["Ano"] = ano
    df_sub["Trimestre"] = trimestre

    # remover linhas sem valor
    df_sub = df_sub.dropna(subset=["ValorDespesas"]).reset_index(drop=True)
    if df_sub.empty:
        return None
    return df_sub[["RegistroANS", "Trimestre", "Ano", "ValorDespesas"]]


def processar_arquivos():
    """Processa incrementalmente os arquivos ZIP em `data/raw` e gera CSV consolidado.

    Strategy: iteramos cada ZIP, extraímos temporariamente, processamos cada arquivo
    (CSV/TXT/XLSX) um a um, identificamos tabelas de despesas, normalizamos e
    escrevemos incrementalmente em `CSV_FINAL`.
    """
    arquivos_zip = sorted(RAW_DIR.glob("*.zip"))
    if not arquivos_zip:
        print("Nenhum ZIP encontrado em data/raw")
        return

    # remover CSV_FINAL anterior para evitar duplicação
    if CSV_FINAL.exists():
        CSV_FINAL.unlink()

    primeira_escrita = True

    for caminho_zip in arquivos_zip:
        print(f"--- Processando ZIP: {caminho_zip.name} ---")
        yq = _extract_year_quarter_from_name(caminho_zip.name)
        ano = str(yq[0]) if yq else "N/A"
        trimestre = str(yq[1]) if yq else "N/A"

        with tempfile.TemporaryDirectory() as td:
            try:
                with zipfile.ZipFile(caminho_zip, 'r') as z:
                    z.extractall(td)
            except Exception as e:
                print(f"Erro ao extrair {caminho_zip.name}: {e}")
                continue

            # percorre arquivos extraídos
            for root, _, files in os.walk(td):
                for fname in files:
                    fpath = Path(root) / fname
                    # ignorar arquivos de sistema
                    if "__MACOSX" in str(fpath):
                        continue
                    print(f"  Lendo: {fname}")
                    df = _try_read_table(fpath)
                    if df is None:
                        print(f"    Não foi possível ler {fname}")
                        continue

                    if not _is_expense_table(df):
                        print(f"    Não é tabela de despesas (pular): {fname}")
                        continue

                    dfn = _normalize_and_extract(df, ano, trimestre)
                    if dfn is None:
                        print(f"    Não conseguiu extrair colunas necessárias em {fname}")
                        continue

                    # escrever incrementalmente
                    try:
                        dfn.to_csv(CSV_FINAL, mode='a', index=False, header=primeira_escrita, encoding='utf-8-sig')
                        primeira_escrita = False
                        print(f"    Gravado {len(dfn)} linhas de {fname}")
                    except Exception as e:
                        print(f"    Erro ao gravar dados de {fname}: {e}")

    if CSV_FINAL.exists():
        print(f"Consolidado gerado: {CSV_FINAL}")
    else:
        print("Nenhum dado de despesa consolidado gerado.")


if __name__ == "__main__":
    processar_arquivos()