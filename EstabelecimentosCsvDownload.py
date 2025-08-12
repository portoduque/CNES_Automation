from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import time
import zipfile
import os


# Caminho absoluto para a pasta 'download' dentro da pasta do script
download_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), "downloads")
os.makedirs(download_dir, exist_ok=True)  # Cria a pasta se não existir

# Entrar no site e baixar arquivo csv
def acessar_opendatasus():
    """
    Função básica para acessar o site do OpenDataSUS
    """
    
    # Configurar opções do Chrome
    chrome_options = Options()
    
    # CONFIGURAÇÕES PARA DOCKER/SERVIDOR (adicionar ANTES das outras opções):
    chrome_options.add_argument('--headless')  # Execução sem interface gráfica
    chrome_options.add_argument('--no-sandbox')  # Necessário para Docker
    chrome_options.add_argument('--disable-dev-shm-usage')  # Evita problemas de memória
    chrome_options.add_argument('--disable-gpu')  # Desabilita GPU
    chrome_options.add_argument('--window-size=1920,1080')  # Define tamanho da janela
    chrome_options.add_argument('--disable-extensions')  # Desabilita extensões
    chrome_options.add_argument('--disable-plugins')  # Desabilita plugins
    chrome_options.add_argument('--disable-images')  # Carrega páginas mais rápido
    
    # Definir o diretório de download 
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,  # Não perguntar onde salvar
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    try:
        # Usar WebDriver Manager para baixar ChromeDriver automaticamente
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
    except Exception as e:
        print(f"Erro ao inicializar ChromeDriver com WebDriver Manager: {e}")
        print("Tentando usar ChromeDriver do sistema...")
        # Fallback: tentar usar ChromeDriver do sistema
        driver = webdriver.Chrome(options=chrome_options)
    
    try:
        print("Abrindo navegador...")
        
        # URL do site
        url = "https://opendatasus.saude.gov.br/dataset/cnes-cadastro-nacional-de-estabelecimentos-de-saude"
        
        print(f"Acessando: {url}")
        
        # Navegar para o site
        driver.get(url)
        
        print("Site carregado com sucesso!")
        
        # Aguardar um pouco para ver a página (opcional)
        time.sleep(5)
        
        
        # Clicar botão "Explorar"
        explorar_btn = driver.find_element(By.XPATH, "/html/body/div[3]/div/div[3]/div/article/div/section[1]/ul/li[2]/div/a")
        explorar_btn.click()
        
        time.sleep(5)
        
        
        # Clicar botão "Ir para recurso"
        download_btn = driver.find_element(By.XPATH, "/html/body/div[3]/div/div[3]/div/article/div/section[1]/ul/li[2]/div/ul/li[2]/a")
        download_btn.click()
        time.sleep(10)
        
        
        # Lista todos os arquivos na pasta de download
        files = os.listdir(download_dir)
        zip_files = [f for f in files if f.endswith('.zip')]

        if zip_files:
            zip_file = os.path.join(download_dir, zip_files[0])  # Caminho completo do primeiro .zip
        else:
            print("Arquivo .zip não encontrado!")
            return
        
        # Extrair arquivo baixado
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(download_dir)
            print("Arquivo extraído com sucesso!")
        # Exclui o arquivo .zip após extração
        try:
            os.remove(zip_file)
            print(f"Arquivo .zip removido: {zip_file}")
        except Exception as e:
            print(f"Erro ao remover o arquivo .zip: {e}")
        
        
        
    except Exception as e:
        print(f"Erro ao acessar o site: {e}")
        
    finally:
        # Fechar o navegador
        print("Fechando navegador...")
        driver.quit()


# Filtrar arquivo csv baixado
def filtrar_estabelecimentos():
    import pandas as pd
    # Encontrar o arquivo CSV extraído
    files = os.listdir(download_dir)
    csv_files = [f for f in files if f.endswith('.csv')]
    if not csv_files:
        print('Arquivo CSV não encontrado no diretório de downloads!')
        return
    csv_path = os.path.join(download_dir, csv_files[0])

    # Ler o CSV
    df = pd.read_csv(csv_path, sep=';', encoding='latin1')
    
    # Filtrar apenas estabelecimentos de RO (CO_UF == 11)
    df_ro = df[df['CO_UF'] == 11]
    
    # Selecionar apenas a coluna CO_CNES
    cnes_list = df_ro['CO_CNES'].drop_duplicates()
    
    # Salvar em novo arquivo, apenas os códigos CNES separados por vírgula, sem cabeçalho
    output_path = os.path.join(download_dir, 'cnes_ro.csv')
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(','.join(str(cnes) for cnes in cnes_list))
    print(f'Arquivo filtrado salvo em: {output_path}')
    # Excluir o arquivo CSV original
    try:
        os.remove(csv_path)
        print(f"Arquivo CSV original removido: {csv_path}")
    except Exception as e:
        print(f"Erro ao remover o arquivo CSV original: {e}")
    


if __name__ == "__main__":

    acessar_opendatasus()

    filtrar_estabelecimentos()