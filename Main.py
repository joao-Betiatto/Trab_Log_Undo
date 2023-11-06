import psycopg2

def execQuery(connect, sql):
    cur = connect.cursor()
    cur.execute(sql)
    connect.commit()

#Conecta no banco postgress, senha está censurada.
def conecta_bd():  # criando conexão com o banco
    connect = psycopg2.connect(

        host='localhost',
        database='trablog',
        user='postgres',
        password='***********')

    return connect


# teste abrindo arquivo
fileName = 'entrada.txt'
try:
    file = open(fileName, "r", encoding="utf-8")
except:
    print('Não foi possível abrir o arquivo') #Caso o arquivo seja inválido
    exit(0)

fileArray = file.read().splitlines()

log = []
bd_inicial = []

# lendo cada linha do arquivo se a abertura der certo

for i in fileArray:
    if (i.startswith("<")):
        log.append(i)
    else:
        bd_inicial.append(i)

numEspacos = 0
for j in bd_inicial:
    if (j == ''):
        numEspacos += 1
for i in range(0, numEspacos, 1):
    bd_inicial.remove('')

connect = conecta_bd()

bd_vetor = []
for line in bd_inicial:
    splitedLine = line.split('=')
    for i in range(0, len(splitedLine), 1):
        splitedLine[i] = splitedLine[i].strip()
        if ',' in splitedLine[i]:
            splitedLine[i] = splitedLine[i].split(',')
    splitedLine.append('Nao inserido')

    bd_vetor.append(splitedLine)
