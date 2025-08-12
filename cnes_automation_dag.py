from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import sys
import os

# Configurações padrão do DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1, 2, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

# Definição do DAG
dag = DAG(
    'automacao_cnes_quinzenal',
    default_args=default_args,
    description='Automação quinzenal para coleta e processamento de dados CNES - Rondônia',
    schedule_interval=timedelta(days=15),
    catchup=False,
    tags=['cnes', 'rondonia', 'saude', 'automacao'],
    max_active_runs=1,
)

# Diretório onde estão os scripts CNES
SCRIPTS_DIR = '/usr/local/airflow/dags/cnes_scripts'

def instalar_e_importar(nome_modulo, pacote_pip):
    """
    Instala um pacote e força a importação
    """
    try:
        # Tentar importar primeiro
        if nome_modulo == 'psycopg2':
            import psycopg2
            return True, psycopg2.__version__
        elif nome_modulo == 'selenium':
            import selenium  
            return True, selenium.__version__
        elif nome_modulo == 'requests':
            import requests
            return True, requests.__version__
        elif nome_modulo == 'pandas':
            import pandas
            return True, pandas.__version__
        elif nome_modulo == 'webdriver_manager':
            from webdriver_manager.chrome import ChromeDriverManager
            return True, "OK"
    except ImportError:
        pass
    
    # Se chegou aqui, precisa instalar
    print(f"📦 Instalando {nome_modulo}...")
    
    try:
        # Tentar instalar com --user primeiro
        result = subprocess.run([
            sys.executable, '-m', 'pip', 'install', '--user', '--quiet', '--force-reinstall', pacote_pip
        ], capture_output=True, text=True, timeout=300)
        
        if result.returncode != 0:
            # Se falhou com --user, tentar sem
            result = subprocess.run([
                sys.executable, '-m', 'pip', 'install', '--quiet', '--force-reinstall', pacote_pip
            ], capture_output=True, text=True, timeout=300)
            
        if result.returncode != 0:
            print(f"❌ Erro na instalação: {result.stderr}")
            return False, "Erro na instalação"
            
    except Exception as e:
        print(f"❌ Exceção durante instalação: {e}")
        return False, str(e)
    
    # Forçar atualização do sys.path
    try:
        import site
        import importlib
        
        # Recarregar site-packages
        importlib.reload(site)
        
        # Atualizar sys.path
        site.main()
        
    except Exception as e:
        print(f"⚠️ Aviso ao recarregar site: {e}")
    
    # Tentar importar após instalação
    try:
        if nome_modulo == 'selenium':
            # Usar exec para forçar importação "fresh"
            exec('import selenium')
            import selenium
            return True, selenium.__version__
        elif nome_modulo == 'requests':
            exec('import requests')
            import requests  
            return True, requests.__version__
        elif nome_modulo == 'pandas':
            exec('import pandas')
            import pandas
            return True, pandas.__version__
        elif nome_modulo == 'psycopg2':
            exec('import psycopg2')
            import psycopg2
            return True, psycopg2.__version__
        elif nome_modulo == 'webdriver_manager':
            exec('from webdriver_manager.chrome import ChromeDriverManager')
            from webdriver_manager.chrome import ChromeDriverManager
            return True, "OK"
            
    except ImportError as e:
        print(f"❌ Ainda não conseguiu importar {nome_modulo} após instalação: {e}")
        return False, f"Import failed: {e}"
    
    return False, "Método de importação não encontrado"

def verificar_ambiente(**context):
    """
    Verifica se o ambiente está preparado para execução
    """
    try:
        print("🔍 Verificando ambiente...")
        print(f"📁 Python executable: {sys.executable}")
        print(f"📁 Python version: {sys.version}")
        print(f"📁 Pasta de scripts CNES: {SCRIPTS_DIR}")
        
        # Verificar se o diretório de scripts existe
        if not os.path.exists(SCRIPTS_DIR):
            raise Exception(f"❌ Diretório de scripts CNES não encontrado: {SCRIPTS_DIR}")
        
        # Verificar scripts
        scripts_necessarios = [
            'main.py',
            'EstabelecimentosCsvDownload.py', 
            'BuscarCnesApiOficial.py',
            'GerarScriptSQLCnes.py',
            'UptadeBancoDeDados.py'
        ]
        
        scripts_encontrados = []
        for script in scripts_necessarios:
            script_path = os.path.join(SCRIPTS_DIR, script)
            if os.path.exists(script_path):
                scripts_encontrados.append(script)
        
        print(f"✅ Scripts encontrados: {scripts_encontrados}")
        if len(scripts_encontrados) != len(scripts_necessarios):
            faltando = set(scripts_necessarios) - set(scripts_encontrados)
            print(f"⚠️ Scripts faltando: {list(faltando)}")
        
        # Criar diretório downloads
        downloads_dir = os.path.join(SCRIPTS_DIR, 'downloads')
        os.makedirs(downloads_dir, exist_ok=True)
        print(f"📁 Diretório downloads preparado: {downloads_dir}")
        
        # Verificar/instalar dependências
        dependencias = {
            'selenium': 'selenium==4.15.2',
            'requests': 'requests==2.31.0',
            'pandas': 'pandas==2.1.3',
            'psycopg2': 'psycopg2-binary==2.9.9',
            'webdriver_manager': 'webdriver-manager==4.0.1'
        }
        
        for nome, pacote in dependencias.items():
            sucesso, versao = instalar_e_importar(nome, pacote)
            if sucesso:
                print(f"✅ {nome}: {versao}")
            else:
                print(f"❌ Falha com {nome}: {versao}")
                # Não parar aqui - tentar continuar
        
        # Teste final de importação
        try:
            import selenium, requests, pandas, psycopg2
            from webdriver_manager.chrome import ChromeDriverManager
            print("🎉 TESTE FINAL: Todas as dependências importadas com sucesso!")
        except ImportError as e:
            print(f"⚠️ TESTE FINAL: Algumas dependências ainda não disponíveis: {e}")
            print("💡 Continuando mesmo assim - dependências podem estar disponíveis na próxima tarefa")
        
        # Chrome (opcional)
        try:
            result = subprocess.run(['google-chrome', '--version'], 
                                 capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                print(f"✅ Chrome instalado: {result.stdout.strip()}")
            else:
                print("⚠️ Chrome não encontrado, mas webdriver-manager pode baixá-lo automaticamente")
        except Exception as e:
            print(f"⚠️ Chrome não disponível: {e}")
        
        print("✅ Verificação do ambiente concluída!")
        
    except Exception as e:
        print(f"❌ Erro na verificação do ambiente: {e}")
        import traceback
        traceback.print_exc()
        raise

def executar_automacao_cnes(**context):
    """
    Executa o script main.py que orquestra toda a automação
    """
    main_script = os.path.join(SCRIPTS_DIR, 'main.py')
    
    try:
        print("🚀 Iniciando automação CNES...")
        print(f"📁 Executando: {main_script}")
        print(f"📁 Diretório de trabalho: {SCRIPTS_DIR}")
        print("=" * 60)
        
        if not os.path.exists(main_script):
            raise FileNotFoundError(f"❌ Script main.py não encontrado: {main_script}")
        
        # Executar o main.py
        resultado = subprocess.run([
            sys.executable, main_script
        ], 
        capture_output=True, 
        text=True, 
        cwd=SCRIPTS_DIR,
        timeout=3600
        )
        
        # Logs
        if resultado.stdout:
            print("📋 SAÍDA DA AUTOMAÇÃO:")
            print("-" * 40)
            print(resultado.stdout)
            print("-" * 40)
            
        if resultado.stderr:
            print("⚠️ AVISOS/ERROS:")
            print("-" * 40)
            print(resultado.stderr)
            print("-" * 40)
        
        if resultado.returncode != 0:
            error_msg = f"❌ Automação falhou com código de retorno: {resultado.returncode}"
            print(error_msg)
            raise subprocess.CalledProcessError(resultado.returncode, 'main.py')
        
        print("🎉 AUTOMAÇÃO CNES EXECUTADA COM SUCESSO!")
        return {'status': 'success', 'return_code': resultado.returncode}
        
    except Exception as e:
        print(f"💥 ERRO na automação CNES: {str(e)}")
        raise

def notificar_conclusao(**context):
    """
    Notifica que a automação foi concluída
    """
    print("📧 Automação CNES quinzenal concluída!")
    print("📅 Próxima execução: dia 1 ou 15 do próximo mês")

# Definição das tarefas
tarefa_verificar_ambiente = PythonOperator(
    task_id='01_verificar_ambiente',
    python_callable=verificar_ambiente,
    dag=dag,
)

tarefa_executar_automacao = PythonOperator(
    task_id='02_executar_automacao_cnes',
    python_callable=executar_automacao_cnes,
    dag=dag,
)

tarefa_notificar = PythonOperator(
    task_id='03_notificar_conclusao',
    python_callable=notificar_conclusao,
    dag=dag,
)

# Ordem de execução
tarefa_verificar_ambiente >> tarefa_executar_automacao >> tarefa_notificar