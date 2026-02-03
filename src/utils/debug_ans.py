import requests
from pathlib import Path

BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"

print('Requesting', BASE_URL)
r = requests.get(BASE_URL, timeout=15)
print('Status:', r.status_code)
print('Headers:')
for k,v in r.headers.items():
    print(' ', k, ':', v)
print('\n--- body start ---')
print(r.text[:1000])
print('--- body end ---')
