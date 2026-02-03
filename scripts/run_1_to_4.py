import runpy
import time
import traceback
from pathlib import Path

SCRIPTS = [
    'src/utils/01_download_ans.py',
    'src/utils/02_processar_arquivos.py',
    'src/utils/03_validar_dados.py',
    'src/utils/04_enriquecer_dados.py',
    'src/utils/05_agregar_despesas.py',
]

def main():
    base = Path('.').resolve()
    print('Running scripts 1 → 5 from', base)
    for s in SCRIPTS:
        print('\n=== Running', s, '===')
        t0 = time.time()
        try:
            runpy.run_path(s, run_name='__main__')
        except SystemExit as e:
            print(f"Script {s} exited with SystemExit: {e}")
            traceback.print_exc()
            raise
        except Exception:
            print(f"Error while running {s}:")
            traceback.print_exc()
            raise
        finally:
            print(f"=== Finished {s} ({time.time()-t0:.1f}s) ===")

    # after running scripts 1..5, create a zip with the aggregated output
    try:
        out = Path('data/output/despesas_agregadas.csv')
        zip_name = Path(f'Teste_Welinton-Rodrigues.zip')
        if out.exists():
            import zipfile
            with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as z:
                z.write(out, arcname=out.name)
            print(f"Created ZIP: {zip_name}")
        else:
            print(f"Aggregated file not found, skipping ZIP: {out}")
    except Exception as e:
        print(f"Error creating zip: {e}")

if __name__ == '__main__':
    main()
