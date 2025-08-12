import json

def formatar_valor(valor):
    """Converte um valor Python para formato SQL"""
    if valor is None:
        return "NULL"
    elif isinstance(valor, str):
        # Escapa aspas simples duplicando elas
        valor_limpo = valor.replace("'", "''")
        return f"'{valor_limpo}'"
    else:
        return str(valor)

def gerar_upsert_cnes(dados_json):
    """Gera um único comando UPSERT otimizado para dados do CNES"""
    
    # Lista dos campos na ordem correta
    campos = [
        'codigo_cnes', 'numero_cnpj_entidade', 'nome_razao_social', 'nome_fantasia',
        'natureza_organizacao_entidade', 'tipo_gestao', 'descricao_nivel_hierarquia',
        'descricao_esfera_administrativa', 'codigo_tipo_unidade', 'codigo_cep_estabelecimento',
        'endereco_estabelecimento', 'numero_estabelecimento', 'bairro_estabelecimento',
        'numero_telefone_estabelecimento', 'latitude_estabelecimento_decimo_grau',
        'longitude_estabelecimento_decimo_grau', 'endereco_email_estabelecimento',
        'numero_cnpj', 'codigo_identificador_turno_atendimento', 'descricao_turno_atendimento',
        'estabelecimento_faz_atendimento_ambulatorial_sus', 'codigo_estabelecimento_saude',
        'codigo_uf', 'codigo_municipio', 'descricao_natureza_juridica_estabelecimento',
        'codigo_motivo_desabilitacao_estabelecimento', 'estabelecimento_possui_centro_cirurgico',
        'estabelecimento_possui_centro_obstetrico', 'estabelecimento_possui_centro_neonatal',
        'estabelecimento_possui_atendimento_hospitalar', 'estabelecimento_possui_servico_apoio',
        'estabelecimento_possui_atendimento_ambulatorial', 'codigo_atividade_ensino_unidade',
        'codigo_natureza_organizacao_unidade', 'codigo_nivel_hierarquia_unidade',
        'codigo_esfera_administrativa_unidade', 'data_atualizacao'
    ]
    
    # Campos para UPDATE (todos exceto a chave primária)
    campos_update = [campo for campo in campos if campo != 'codigo_cnes']
    
    # Coleta todos os VALUES
    lista_valores = []
    for registro in dados_json:
        valores = []
        for campo in campos:
            valor = registro.get(campo)
            valores.append(formatar_valor(valor))
        lista_valores.append(f"  ({', '.join(valores)})")
    
    # Monta o comando SQL único
    campos_str = ", ".join(campos)
    valores_str = ",\n".join(lista_valores)
    
    # Monta a parte do UPDATE
    updates = [f"  {campo} = EXCLUDED.{campo}" for campo in campos_update]
    updates_str = ",\n".join(updates)
    
    # Comando UPSERT único e compacto
    sql = f"""INSERT INTO unidade_saude ({campos_str})
VALUES
{valores_str}
ON CONFLICT (codigo_cnes) DO UPDATE SET
{updates_str};"""
    
    return sql

def main():
    # Configurações
    arquivo_json = "downloads/cnes_resultados.json"
    arquivo_sql = "downloads/cnes_upserts.sql"
    
    try:
        # Carrega o arquivo JSON
        print(f"Lendo arquivo: {arquivo_json}")
        with open(arquivo_json, 'r', encoding='utf-8') as f:
            dados = json.load(f)

        total = len(dados)
        print(f"Encontrados {total} registros")

        comando_sql = gerar_upsert_cnes(dados)
        with open(arquivo_sql, 'w', encoding='utf-8') as f:
            f.write("-- Comando UPSERT otimizado para tabela unidade_saude\n")
            f.write(f"-- Gerado automaticamente a partir dos dados do CNES (total de registros: {total})\n\n")
            f.write(comando_sql)
        print(f"Arquivo SQL criado: {arquivo_sql} ({total} registros)")
        # Exclui o arquivo JSON após gerar o SQL
        import os
        try:
            os.remove(arquivo_json)
            print(f"Arquivo {arquivo_json} excluído com sucesso.")
        except Exception as e:
            print(f"Não foi possível excluir {arquivo_json}: {e}")

    except FileNotFoundError:
        print(f"Erro: Arquivo {arquivo_json} não encontrado!")
    except json.JSONDecodeError:
        print("Erro: Arquivo JSON inválido!")
    except Exception as e:
        print(f"Erro inesperado: {e}")

if __name__ == "__main__":
    main()