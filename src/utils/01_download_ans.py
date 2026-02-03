import os
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from pathlib import Path

"""
01_download_ans.py

Propósito:
 - Navegar na seção pública de demonstrativos contábeis da ANS,
     detectar os trimestres mais recentes e baixar os arquivos ZIP.

Decisões técnicas (resumidas):
 - Uso de `requests` + `BeautifulSoup` para leitura do HTML porque
     o diretório é servido como página estática (sem API JSON estruturada).
 - Definimos um `User-Agent` e usamos `verify=False` para evitar
     falhas por certificados SSL mal configurados nos hosts governamentais;
     em produção preferir configurar CA corretamente.
 - Baixamos os 3 arquivos ZIP mais recentes (por ordem) como critério
     simples e reprodutível para o teste.

Observação sobre repetibilidade:
 - O script é idempotente: não baixa arquivos que já existam em `data/raw`.
"""

# Configurações
BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"
RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

def listar_links_v2(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache'
    }
    try:
        # verify=False ignora erros de certificado SSL comuns em sites do governo
        response = requests.get(url, timeout=20, headers=headers, verify=False)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        links = []
        for node in soup.find_all('a'):
            href = node.get('href')
            if href and not href.startswith('?'):
                links.append(urljoin(url, href))
        return links
    except Exception as e:
        print(f"Erro ao acessar {url}: {e}")
        return []

def main():
    print("--- Verificação Geral de Arquivos ---")
    
    # 1. Detectar o ano mais recente disponível sob BASE_URL
    print("Detectando anos disponíveis em:", BASE_URL)
    top_links = listar_links_v2(BASE_URL)
    years = []
    for l in top_links:
        seg = l.rstrip("/").split("/")[-1]
        if seg.isdigit() and len(seg) == 4:
            try:
                years.append(int(seg))
            except ValueError:
                pass

    if not years:
        print("Nenhum diretório de ano encontrado em BASE_URL. Abortando.")
        return

    max_year = max(years)
    url_year = urljoin(BASE_URL, f"{max_year}/")
    print(f"Lendo diretamente de: {url_year} (ano mais recente: {max_year})")

    links = listar_links_v2(url_year)
    
    encontrados = []
    print("\nLista completa de links encontrados no HTML:")
    for l in links:
        nome = l.split('/')[-1]
        print(f" > Link: {nome}") # Isso vai mostrar TUDO que o Python está lendo
        
        if ".zip" in nome.lower():
            encontrados.append(l)

    # 2. Filtragem e Download
    # Vamos pegar os 3 primeiros .zip que aparecerem
    arquivos_finais = sorted(encontrados, reverse=True)[:3]

    print(f"\n--- Selecionados para Download ({len(arquivos_finais)}) ---")
    for url in arquivos_finais:
        nome = url.split('/')[-1]
        print(f"Baixando: {nome}")
        destino = RAW_DIR / nome
        baixar_arquivo(url, destino)


def baixar_arquivo(url: str, destino: Path):
    """Baixa um arquivo .zip para `destino` se ele não existir."""
    if destino.exists():
        print(f"Já existe: {destino.name}")
        return

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    try:
        with requests.get(url, stream=True, timeout=60, headers=headers, verify=False) as r:
            r.raise_for_status()
            with open(destino, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        print(f"Salvo: {destino}")
    except Exception as e:
        print(f"Falha ao baixar {url}: {e}")


if __name__ == '__main__':
    main()