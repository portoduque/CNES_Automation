import subprocess

def executar_script(script):
	try:
		resultado = subprocess.run([
			'python', script
		], check=True)
		return resultado.returncode == 0
	except Exception as e:
		print(f"Erro ao executar {script}: {e}")
		return False

if __name__ == "__main__":
		print("Iniciando automação CNES...")
		print("1. Baixando o arquivo CSV de estabelecimentos (EstabelecimentosCsvDownload.py)...")
		if executar_script("EstabelecimentosCsvDownload.py"):
			print("Arquivo CSV baixado com sucesso!\n")
			print("2. Buscando dados da API oficial do CNES (BuscarCnesApiOficial.py)...")
			if executar_script("BuscarCnesApiOficial.py"):
				print("Dados da API oficial obtidos com sucesso!\n")
				print("3. Gerando script SQL para atualização do banco (GerarScriptSQLCnes.py)...")
				if executar_script("GerarScriptSQLCnes.py"):
					print("Script SQL gerado com sucesso!\n")
					print("4. Atualizando o banco de dados com os dados processados (UptadeBancoDeDados.py)...")
					if executar_script("UptadeBancoDeDados.py"):
						print("Banco de dados atualizado com sucesso!\n")
						print("Processo de automação CNES finalizado com sucesso!")
					else:
						print("[ERRO] Falha ao executar UptadeBancoDeDados.py. Verifique os logs para mais detalhes.")
				else:
					print("[ERRO] Falha ao executar GerarScriptSQLCnes.py. Verifique os logs para mais detalhes.")
			else:
				print("[ERRO] Falha ao executar BuscarCnesApiOficial.py. Verifique os logs para mais detalhes.")
		else:
			print("[ERRO] Falha ao executar EstabelecimentosCsvDownload.py. Verifique os logs para mais detalhes.")
