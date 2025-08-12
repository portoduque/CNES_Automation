import subprocess
import sys
import os

def executar_script(script):
    try:
        print(f"Executando: {script}")
        resultado = subprocess.run([
            sys.executable, script  # ✅ Usar sys.executable
        ], check=True, capture_output=True, text=True, cwd=os.path.dirname(os.path.abspath(__file__)))
        
        if resultado.stdout:
            print(f"Saída: {resultado.stdout}")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar {script}: Código {e.returncode}")
        if e.stdout:
            print(f"Stdout: {e.stdout}")
        if e.stderr:
            print(f"Stderr: {e.stderr}")
        return False
    except Exception as e:
        print(f"Erro inesperado ao executar {script}: {e}")
        return False

if __name__ == "__main__":
    print("Iniciando automação CNES...")
    
    scripts = [
        ("EstabelecimentosCsvDownload.py", "Baixando arquivo CSV de estabelecimentos"),
        ("BuscarCnesApiOficial.py", "Buscando dados da API oficial do CNES"),
        ("GerarScriptSQLCnes.py", "Gerando script SQL para atualização do banco"),
        ("UptadeBancoDeDados.py", "Atualizando banco de dados")
    ]
    
    for script, descricao in scripts:
        print(f"\n{descricao}...")
        if executar_script(script):
            print(f"✅ {script} executado com sucesso!")
        else:
            print(f"❌ Falha ao executar {script}")
            print("Parando execução devido ao erro.")
            sys.exit(1)
    
    print("\n🎉 Processo de automação CNES finalizado com sucesso!")