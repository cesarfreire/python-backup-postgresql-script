import os
import time
import psycopg2
import smtplib
import ssl

# parametros gerais
username = 'postgres'
password = str(123456)
defaultdb = 'postgres'
port = '5413'
date = time.strftime("%Y-%m-%d")
host = '127.0.0.1'


def conexao_banco():
    # conexao com o banco de dados
    try:
        conexao = psycopg2.connect(
            user=username,
            host=host,
            port=port,
            database=defaultdb,
        )
        # cria o cursor
        cursor = conexao.cursor()
        return cursor

    except (Exception, psycopg2.Error) as error:
        print("Erro ao retornar os dados do banco: ", error)
        conexao.close()
        cursor.close()


def listar_bases():
    # listar as bases do banco
    try:
        # cria o cursor
        cursor = conexao_banco()

        # query que busca todas as bases
        query_listar_bases = "select datname from pg_database where datname not in ('postgres','root') and datname not like 'template%' order by datname;"

        # executa a query
        cursor.execute(query_listar_bases)

        # guarda a lista na variavel
        lista_bases = cursor.fetchall()

        return lista_bases

    except (Exception, psycopg2.Error) as error:
        print("Erro: ", error)

    finally:
        cursor.close()


def executar_vacuum(tipo, base):
    try:
        comando_vacuum = 'psql -At -h ' + host + ' -p 5413 -U postgres -w -c "vacuum VERBOSE "' + tipo + ' ' + str(base)
        print(comando_vacuum)
        try:
            os.system(comando_vacuum)
        except (Exception, os.error) as error:
            print('Erro ao executar o comando: ' + error)
    except (Exception, os.error) as error:
        print('Erro ao executar o comando: ' + error)


def efetuar_backup(tipo):
    backupdir = 'backup/' + tipo + '/'
    lista_bases = listar_bases()
    corpo_email = 'Subject: Resumo backup\n\n'

    for base in lista_bases:
        try:
            comando_dump = 'pg_dump -h ' + host + ' -p ' + port + ' -U ' + username + ' -w -v -Fc --exclude-table-data=audit.logged_actions --exclude-schema=dne' \
                           ' -d ' + base[0] + ' -f ' + backupdir + base[0] + '-' + date + '.backup 2> ' + backupdir + base[0] + '-' + date + '.txt'
            print(comando_dump)
            try:
                os.system(comando_dump)
                arquivo_log = open(backupdir + base[0] + '-' + date + '.txt', 'r')

                for linha in arquivo_log:
                    corpo_email = corpo_email + linha

                enviar_email(corpo_email)

                arquivo_log.close()


            except (Exception, os.error) as error:
                print('Erro ao executar o comando: ' + error)

        except (Exception, psycopg2.Error) as error:
            print("Erro: ", error)


def manutencao():
    lista_bases = listar_bases()

    for base in lista_bases:
        executar_vacuum('', base[0])
        executar_vacuum('ANALYZE', base[0])


def enviar_email(mensagem):
    port = 465  # For SSL
    password = 'senha-email'

    # Create a secure SSL context
    context = ssl.create_default_context()

    with smtplib.SMTP_SSL("smtp.gmail.com", port, context=context) as server:
        server.login("seuemail@gmail.com", password)
        sender_email = "seuemail@gmail.com"
        receiver_email = "seuemail@gmail.com"

        server.sendmail(sender_email, receiver_email, mensagem)


def main():
    dia = str(time.strftime("%d"))
    if dia == '01':
        manutencao()
        efetuar_backup('monthly')
    elif dia == '0':
        efetuar_backup('weekly')
    else:
        efetuar_backup('daily')

"""
Funcao principal
"""
main()
