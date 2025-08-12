import os
import requests
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

def ler_codigos_cnes(caminho_csv):
    """
    Lê os códigos CNES de um arquivo CSV onde os códigos estão separados por vírgula.
    Retorna uma lista de códigos como strings.
    """
    with open(caminho_csv, 'r', encoding='utf-8') as f:
        conteudo = f.read()
        codigos = conteudo.strip().split(',')
    return codigos

def requisicao_cnes(codigo_cnes, max_retries=3):
        url = f'https://apidadosabertos.saude.gov.br/cnes/estabelecimentos/{codigo_cnes}'
        headers = {'accept': 'application/json'}
        
        for tentativa in range(max_retries):
            try:
                response = requests.get(url, headers=headers, timeout=30)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 404:
                    print(f'CNES {codigo_cnes} não encontrado')
                    return None
                else:
                    print(f'Erro HTTP {response.status_code} para CNES {codigo_cnes}')
            except requests.exceptions.RequestException as e:
                print(f'Tentativa {tentativa + 1} falhou para CNES {codigo_cnes}: {e}')
                if tentativa < max_retries - 1:
                    time.sleep(2)  # Aguardar antes de tentar novamente
                
        return None

def consultar_lista_cnes_api(lista_codigos, pasta_downloads='downloads', max_workers=20, caminho_csv=None):
    """
    Recebe uma lista de códigos CNES, consulta a API pública para cada um deles em paralelo
    e salva todos os resultados em um único arquivo JSON na pasta downloads, como uma lista de objetos.
    Imprime no console o total de requisições, quantas deram certo e quantas deram erro.
    Se todas as requisições foram processadas (sucesso + erro == total), exclui o arquivo CSV inicial.
    Não retorna nada.
    """
    os.makedirs(pasta_downloads, exist_ok=True)
    resultados = []
    total = len(lista_codigos)
    sucesso = 0
    erro = 0
    max_retries = 3  # Número de tentativas para cada requisição
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_codigo = {executor.submit(requisicao_cnes, codigo, max_retries): codigo for codigo in lista_codigos}
        for future in as_completed(future_to_codigo):
            resultado = future.result()
            if resultado:
                resultados.append(resultado)
                sucesso += 1
            else:
                erro += 1
    arquivo_json = os.path.join(pasta_downloads, 'cnes_resultados.json')
    with open(arquivo_json, 'w', encoding='utf-8') as f:
        json.dump(resultados, f, ensure_ascii=False, indent=2)
    print(f'Resultados salvos em: {arquivo_json}')
    print(f'Total de requisições: {total}')
    print(f'Requisições bem-sucedidas: {sucesso}')
    print(f'Requisições com erro: {erro}')
    
    # Exclui o arquivo CSV inicial se todas as requisições foram processadas
    if caminho_csv and (sucesso + erro == total):
        try:
            os.remove(caminho_csv)
        except Exception as e:
            print(f'Erro ao remover o arquivo CSV: {e}')

def main():
    # Define o caminho do arquivo CSV
    caminho_csv = os.path.join('downloads', 'cnes_ro.csv')

    # Lê os códigos CNES
    cnes_codigos = ler_codigos_cnes(caminho_csv)

    # Consultar o CNES da lista e salvar todos os resultados em um único arquivo JSON
    consultar_lista_cnes_api(cnes_codigos, caminho_csv=caminho_csv)


if __name__ == "__main__":
    main()

