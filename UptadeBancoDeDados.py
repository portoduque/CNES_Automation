# pip install psycopg2-binary

import psycopg2

# Configurações do banco (MODIFIQUE AQUI!)
host = '172.16.111.87'
database = 'cnes_ro_api_database'
user = 'servapp'
password = 'Dev4pp$auded1gital'
port = 5432

# Arquivo SQL para executar
arquivo_sql = 'downloads/cnes_upserts.sql'

try:
    # Conectar ao banco
    print("Conectando ao PostgreSQL...")
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port
    )
    cursor = conn.cursor()
    print("✅ Conectado!")
    
    # Ler e executar o arquivo SQL
    print(f"Executando {arquivo_sql}...")
    with open(arquivo_sql, 'r', encoding='utf-8') as f:
        sql_script = f.read()
    
    cursor.execute(sql_script)
    conn.commit()
    
    print(f"✅ Script executado com sucesso!")
    print(f"📊 Registros afetados: {cursor.rowcount}")
    # Exclui o arquivo SQL após execução bem-sucedida
    import os
    try:
        os.remove(arquivo_sql)
        print(f"🗑️ Arquivo '{arquivo_sql}' excluído com sucesso!")
    except Exception as e:
        print(f"⚠️ Não foi possível excluir '{arquivo_sql}': {e}")
    
except Exception as e:
    print(f"❌ Erro: {e}")
finally:
    # Fechar conexão
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
    print("🔌 Conexão fechada")