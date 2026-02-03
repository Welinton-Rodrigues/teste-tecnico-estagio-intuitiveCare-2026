# Teste Técnico – Intuitive Care

## Visão Geral

Este projeto foi desenvolvido como parte do **Teste de Entrada para Estagiários v2.0** da **Intuitive Care (Healthtech SaaS)**.

O objetivo é demonstrar capacidade de:

* Trabalhar com dados reais e inconsistentes
* Integrar com fontes públicas de dados (ANS)
* Tomar decisões técnicas fundamentadas (trade-offs)
* Organizar e documentar um projeto de forma profissional

O foco foi priorizar **clareza, robustez e justificativas técnicas**, em vez de apenas volume de código.

---

## Tecnologias Utilizadas

* **Python 3.12.3**
 # Teste Técnico – Intuitive Care

## Visão Geral

Este projeto foi desenvolvido como parte do **Teste de Entrada para Estagiários v2.0** da **Intuitive Care (Healthtech SaaS)**.

O objetivo é demonstrar capacidade de:

- Trabalhar com dados reais e inconsistentes
- Integrar com fontes públicas de dados (ANS)
- Tomar decisões técnicas fundamentadas (trade-offs)
- Organizar e documentar um projeto de forma profissional

O foco foi priorizar **clareza, robustez e justificativas técnicas**, em vez de apenas volume de código.

---

## Tecnologias Utilizadas

- **Python 3.12.3**
- Pandas
- Requests
- BeautifulSoup4
- OpenPyXL

> A versão do Python foi escolhida por ser estável, amplamente adotada em ambientes produtivos e compatível com todas as bibliotecas utilizadas.

---

## Estrutura do Projeto

```
Teste_Welinton_Rodrigues/
│
├── src/                  # Scripts principais do projeto
│   └── utils/            # Funções auxiliares reutilizáveis
│       ├── 01_download_ans.py
│       ├── 02_processar_arquivos.py
│       ├── 03_validar_dados.py
│       ├── 04_enriquecer_dados.py
│       └── 05_agregar_despesas.py
│
├── scripts/              # Helpers para executar o fluxo completo
│   └── run_all.py     # Executa 01→05 em sequência (ex.: CI)
│
├── data/                 # Dados organizados por estágio
│   ├── raw/              # Arquivos ZIP baixados da ANS
│   ├── extracted/        # Arquivos extraídos
│   ├── processed/        # CSVs intermediários
│   └── output/           # Resultados finais
│
├── README.md
├── requirements.txt
├── .gitignore
└── venv/
```


## Como Executar (Passo a passo para avaliadores)

Siga estes passos na ordem indicada para reproduzir o processamento completo e gerar os arquivos de saída.

Pré-requisitos:

- Git instalado
- Python 3.11+ (o projeto foi testado com Python 3.12)
- Conexão com a internet (para baixar os ZIPs da ANS)

Passo 1 — clonar o repositório

```bash
git clone https://github.com/Welinton-Rodrigues/teste-tecnico-estagio-intuitiveCare-2026.git
cd teste-tecnico-estagio-intuitiveCare-2026
```

Passo 2 — criar e ativar o ambiente virtual

Windows (PowerShell):

```powershell
python -m venv venv
.\venv\Scripts\Activate
```

Windows (cmd.exe):

```cmd
python -m venv venv
venv\Scripts\activate.bat
```

macOS / Linux:

```bash
python3 -m venv venv
source venv/bin/activate
```

Passo 3 — instalar dependências

```bash
python -m pip install -r requirements.txt
```

Passo 4 — executar o fluxo completo (recomendado)

Isso executa os scripts `01`→`05` na ordem correta.

```bash
python scripts/run_all.py
```

Passo 5 — executar scripts individualmente (opcional)

Se preferir rodar passo a passo ou debugar:

```bash
python -m src.utils.01_download_ans
python -m src.utils.02_processar_arquivos
python -m src.utils.03_validar_dados
python -m src.utils.04_enriquecer_dados
# (opcional) python -m src.utils.05_agregar_despesas
```

Passo 6 — gerar ZIP do consolidado (opcional)

```bash
python scripts/make_zip.py
```

Arquivos de saída principais:

- `data/output/consolidado_despesas.csv` — consolidados brutos extraídos
- `data/output/consolidado_despesas.zip` — zip do consolidad
- `data/processed/dados_validados.csv` — registros validados e enriquecidos

Dicas de troubleshooting rápidas:

- Erro de módulo ausente: execute `python -m pip install -r requirements.txt` dentro do `venv` ativado.
- Avisos SSL em downloads: são warnings do `urllib3`; o download costuma funcionar mesmo assim. Para ambiente fechado, é possível instalar certificados adequados.
- Problemas de encoding/nomes truncados: os scripts tentam várias codificações (UTF-8/latin-1) e aplicam heurísticas de correção de mojibake; se notar caracteres estranhos, tente abrir os CSVs com `encoding='utf-8-sig'` no Excel.

Se quiser, posso também commitar e subir este README agora (push para `origin/main`).
Resultado final:

```
data/output/despesas_agregadas.csv
```

---

## Considerações Finais

- O projeto foi desenvolvido pensando em **robustez e manutenibilidade**
- As decisões técnicas priorizaram clareza e aderência ao contexto de dados reais
- Limitações e trade-offs foram documentados de forma transparente

---

## 2.3 Agregação — Trade-off de ordenação

Para ordenar o resultado por `TotalDespesas` (maior → menor) existem duas estratégias principais:

- Ordenação em memória: mais simples e rápida quando o resultado agregado cabe na RAM (número de operadoras é tipicamente muito menor que o número de linhas originais). Para este projeto usamos esta abordagem porque após a agregação por trimestre os registros ficam compactos e facilmente manipuláveis em memória.
- Ordenação externa (em disco): necessária quando o número de grupos resultantes é muito grande e não cabe em memória; envolve técnicas como merge-sort em disco ou uso de ferramentas como `sqlite`/`duckdb` para ordenar/consultar sem carregar tudo na RAM.

Justificativa: dado o domínio (agrupamento por `RazaoSocial` + `UF`) o número de grupos é limitado pelo número de operadoras ativas — geralmente algumas dezenas ou poucas centenas — portanto optamos pela ordenação em memória por simplicidade e desempenho. Caso você rode o pipeline em uma máquina com memória muito limitada, recomendo executar a agregação em um banco leve (`duckdb`/`sqlite`) ou aumentar o `chunksize`/usar streaming para pré-ordenar em blocos.

## Autor

**Welinton Rodrigues**
