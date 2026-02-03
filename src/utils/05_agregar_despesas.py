"""
05_agregar_despesas.py

Propósito:
 - Agrupar despesas por `RazaoSocial` e `UF`, calcular total, média por trimestre
   e desvio padrão por operadora/UF. Ordenar por total (decrescente) e salvar
   em `data/output/despesas_agregadas.csv`.

Estratégia:
 - Leitura em chunks para evitar uso excessivo de memória.
 - Primeiro agregamos por (RazaoSocial, UF, Ano, Trimestre) para obter soma
   por trimestre; depois agregamos por (RazaoSocial, UF) para calcular total,
   média trimestral e desvio padrão.

Saída:
 - CSV `data/output/despesas_agregadas.csv` com colunas:
   RazaoSocial, UF, TotalDespesas, MediaPorTrimestre, DesvioPadraoPorTrimestre, NumTrimestres

"""

import pandas as pd
from pathlib import Path

INPUT = Path("data/processed/dados_validados.csv")
OUTPUT = Path("data/output/despesas_agregadas.csv")
OUTPUT.parent.mkdir(parents=True, exist_ok=True)

def _read_with_sep(path):
    # tenta detectar separador comum (',' ou ';')
    try:
        return pd.read_csv(path, sep=',', low_memory=False)
    except Exception:
        return pd.read_csv(path, sep=';', low_memory=False)

def executar_agregacao(chunksize=200_000):
    print("--- Iniciando agregação de despesas (2.3) ---")
    if not INPUT.exists():
        print(f"Arquivo de entrada não encontrado: {INPUT}")
        return

    # Ler em chunks e agregar por (RazaoSocial, UF, Ano, Trimestre)
    quarter_parts = []
    try:
        # detectar separador pela amostra do arquivo
        sample = Path(INPUT).read_text(encoding='utf-8', errors='ignore')[:8192]
        sep = ';' if sample.count(';') > sample.count(',') else ','

        # leitura tolerante, pulando linhas malformadas
        reader = pd.read_csv(INPUT, sep=sep, chunksize=chunksize, low_memory=False, on_bad_lines='skip')

        for i, chunk in enumerate(reader, start=1):
            # garantir colunas necessárias
            for c in ['RazaoSocial', 'UF', 'Ano', 'Trimestre', 'ValorDespesas']:
                if c not in chunk.columns:
                    chunk[c] = None

            chunk['ValorDespesas'] = pd.to_numeric(chunk['ValorDespesas'], errors='coerce').fillna(0)

            grp = (
                chunk.groupby(['RazaoSocial', 'UF', 'Ano', 'Trimestre'], dropna=False)['ValorDespesas']
                .sum()
                .reset_index()
            )
            quarter_parts.append(grp)
            print(f"Processado chunk {i}, linhas lidas: {len(chunk)}")

        if not quarter_parts:
            print("Nenhum dado lido dos chunks.")
            return

        quarters = pd.concat(quarter_parts, ignore_index=True)
        # Somar novamente caso exista sobreposição entre chunks
        quarters = (
            quarters.groupby(['RazaoSocial', 'UF', 'Ano', 'Trimestre'], dropna=False)['ValorDespesas']
            .sum()
            .reset_index()
        )

        # Agora agregamos por RazaoSocial + UF: total, média por trimestre e desvio padrão
        stats = (
            quarters.groupby(['RazaoSocial', 'UF'], dropna=False)['ValorDespesas']
            .agg(TotalDespesas='sum', MediaPorTrimestre='mean', DesvioPadraoPorTrimestre='std', NumTrimestres='count')
            .reset_index()
        )

        # Ordenar por total decrescente
        stats = stats.sort_values('TotalDespesas', ascending=False)

        # Salvar CSV
        stats.to_csv(OUTPUT, index=False, encoding='utf-8-sig')
        print(f"Agregação concluída. Arquivo salvo: {OUTPUT} (linhas: {len(stats)})")

    except Exception as e:
        print(f"Erro durante agregação: {e}")

if __name__ == '__main__':
    executar_agregacao()
