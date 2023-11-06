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



sql = 'DROP TABLE IF EXISTS log_table'
execQuery(connect, sql)  # se a tabela já existir, será excluída.
cursor = connect.cursor()
column = []
sqlColumns = ''

for item in range(0, len(bd_vetor), 1):
    if bd_vetor[item][0][0] not in column:
        sqlColumns = sqlColumns+','+bd_vetor[item][0][0]+' INT'
        column.append(bd_vetor[item][0][0])

sql = 'CREATE TABLE log_table (id INT'+sqlColumns+')'
execQuery(connect, sql)  # Aqui foi criada a tabela

zerosNum = ''
for i in range(0, len(column), 1):
    zerosNum = zerosNum+',0'


for item in range(0, len(bd_vetor), 1):
    if bd_vetor[item][2] == 'Nao inserido':
        sql = 'INSERT INTO log_table VALUES (' + \
            bd_vetor[item][0][1]+zerosNum+')'
        execQuery(connect, sql)
        for itemTemp in range(0, len(bd_vetor), 1):
            if bd_vetor[itemTemp][0][1] == bd_vetor[item][0][1]:
                bd_vetor[itemTemp][2] = 'Inserido'

for item in range(0, len(bd_vetor), 1):
    sql = 'UPDATE log_table SET id = ' + \
        bd_vetor[item][0][1]+', '+bd_vetor[item][0][0]+' = ' + \
        bd_vetor[item][1]+' WHERE id ='+bd_vetor[item][0][1]
    execQuery(connect, sql)



# Começa checkpoints
VisitedCommitedTransactions = {} 
CKPT = False 
commitedTransaction = [] 
TStart = [] 
TCKPT = [] 
for line in range(len(log)-1, -1, -1): 
    
    if CKPT == True:
            if len(TCKPT) == 0: #Caso já tiver passado por todas as transações do checkpoint
                break;
            
    if 'START' in log[line] and 'CKPT' in log[line]:
        CKPT = True
        TCKPT.append(log[line].split('(')[1].replace(")>\n", "").replace(" ", ''))
        print("Transação do checkpoint", TCKPT)
        for lineEndCkpt in range(len(log)-1, -1, -1):
            if 'END' in log[lineEndCkpt] and lineEndCkpt > line:  
                for lineCkpt in range(line, len(log)-1, 1):
                    if 'commit' in log[lineCkpt]:
                        splitedCommit = log[lineCkpt].split(' ')
                        VisitedCommitedTransactions[splitedCommit[1][:-1]] = 'unvisited' #Marca como não visitada
                        print("Transação Commitada: ", VisitedCommitedTransactions)
