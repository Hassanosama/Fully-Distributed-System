import zmq
import time
import sys
import threading
import queue
import mysql.connector
from multiprocessing import Process

print('Establishing Master Tracker,Please Wait..')
print('--------------------------------------------------------------------')

#Initializing Variables.
#RecordsFile = open('Records.txt','r')   #Load records.
LookUpTable = {}
VideoNames = []
Instance = []
MinumumNumberOfCopies = 1
q = queue.Queue()
DataNodeAsSource = ['no','no','no']
State = ['offline', 'offline', 'offline']   #State of the Data Nodes, Intilally all Machines are offline.
LastTime = [0, 0, 0] #Last Time Machine sent an Alive message.
NumberOfThreads = 1
context = zmq.Context()
Threads = []
subscribers = []  #notice that number if subscriber ports = number of threads.
msg = []    #notice that number if messages = number of threads.
Ports = ['6000','7000','8000'] #Representitive Ports of the Data Nodes.
TransferPorts = {'6001':'free' , '6002':'free' , '6003':'free' , '7001':'free' , '7002':'free' , '7003':'free' , '8001':'free' , '8002':'free' , '8003':'free'}
DataNodePorts = [['6001','6002','6003'],['7001','7002','7003'],['8001','8002','8003']]
IP = ['localhost','localhost','localhost']  #IP's of the Data Nodes
MasterPort = '5555'     # Master Port is the port which the users connect with.
DataNodeAsServerPort = '1212'

Server = context.socket(zmq.REP)
Server.bind("tcp://*:%s" % MasterPort)

for i in range(0,NumberOfThreads):
    subscribers.append(context.socket(zmq.SUB))
    subscribers[i].connect('tcp://%s:%s' %(IP[i],Ports[i]) )
    subscribers[i].setsockopt(zmq.SUBSCRIBE, b'')
    msg.append('')

#Connecting to the database.
try:
    mydb = mysql.connector.connect(host="localhost",user="root",passwd="1234",database="os_project")
    mycursor = mydb.cursor()
except:
    print("Can't connect to database")

#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
def Connect(ID):
    msg[ID] = subscribers[ID].recv_string()
    if(msg[ID] == 'Alive'):
        print('Connected to Data Node number %s' %threading.currentThread().getName())
        LastTime[ID] = time.time()
        State[ID] = 'online'
    while True:
        msg[ID] = subscribers[ID].recv_string()
        if(msg[ID] == 'Alive'):
            LastTime[ID] = time.time()
        else:
            PortNumber = ''
            if 'Uploaded' in msg[ID]:
                FileName = subscribers[ID].recv_string()
                FilePath = subscribers[ID].recv_string()
                PortNumber = subscribers[ID].recv_string()
                FileSize = subscribers[ID].recv_string()
                dummy,user_id = msg[ID].split('|')
                #Insert that file record to the records table.
                mycursor.execute("INSERT INTO records (filename,datanode_id,filepath,filesize) VALUES(%s,%s,%s,%s);",(FileName,ID,FilePath,FileSize,))
                mydb.commit()

                #Update the lookup table.
                mycursor.execute("INSERT INTO look_up (user_id,filename,datanode_id,filepath,dataNode_state) VALUES(%s,%s,%s,%s,%s);",(user_id,FileName,ID,FilePath,State[ID],))            
                mydb.commit()

                VideoNames.append(FileName)
                if(CountCopies(FileName) < MinumumNumberOfCopies):
                    q.put(FileName)

            else:
                PortNumber = subscribers[ID].recv_string()

            TransferPorts[PortNumber] = 'free'
#----------------------------------------------------------------------------------------------

def GetFreePort(idx):
    if (State[idx] == 'offline'):
        return 'none','none'
    
    for p in DataNodePorts[idx]:
        if(TransferPorts[p] == 'free'):
            return p,IP[idx]
    return 'none','none'
#----------------------------------------------------------------------------------------------

def GetNodes(name):
    mycursor.execute("SELECT datanode_id FROM records WHERE filename = %s",(name,))
    nodes = mycursor.fetchall()
    ret = []
    for node in nodes:
        ret.append(int(node[0]))
    return ret

def SendList():
    mycursor.execute("SELECT filename,filesize FROM records")
    TMP = mycursor.fetchall()
    for row in TMP:
        name = str(row[0])
        size = str(row[1])
        Server.send_string(name + '|' + size)
        Server.recv_string()
    Server.send_string('done')



def ClientsHandler():
    while True:
        request = Server.recv_string() #Waiting for request from any client.
        if(request == 'check'):
            Server.send_string('')
            continue
        
        print('Received %s request from a user..' %request) # There is a client sent a request.
        print('Finding Free Port..')
        FreePort = 'none'
        PortIP = 'none'
        if(request == 'Upload'):
            idx = 0
            while(FreePort == 'none'):
                FreePort,PortIP = GetFreePort(idx)
                idx+=1
                idx%=len(Ports)
            print('Port Found')
            Server.send_string(PortIP+' '+FreePort)
            TransferPorts[FreePort] = 'busy'
        else:           #Download.
            SendList()
            FileName = Server.recv_string()
            List = GetNodes(FileName)       #Get The nodes indecies which containing that file.
            Info = ''
            while len(Info) == 0:
                for i in range(0,NumberOfThreads):
                    FreePort = 'none'
                    PortIP = 'none'
                    if(List.__contains__(i)):
                        FreePort,PortIP = GetFreePort(i)
                    if(FreePort != 'none'):
                        Info += ' '
                        Info += PortIP
                        Info += ' '
                        Info += FreePort
                        TransferPorts[FreePort] = 'busy'
            print('Ports Found')
            Server.send_string(Info)
#----------------------------------------------------------------------------------------------
def GetSourceNode(name):
    mycursor.execute("SELECT datanode_id , filepath FROM records WHERE filename = %s",(name,))
    sources = mycursor.fetchall()
    #We can get more than one source, so we should iterate to find an available one.
    while True:
        for src in sources:
            DataNodeIdx = src[0]
            if State[DataNodeIdx] == 'online' and DataNodeAsSource[DataNodeIdx] == 'no': #The machine should be online and not replicating for now.
                return src  #return the index of the datanode and the path of the file on that datanode

def GetChosenNodes(name):
    Info = ''
    mycursor.execute("SELECT datanode_id FROM records WHERE filename = %s",(name,))
    sources = mycursor.fetchall()
    counter = len(sources)  #Number of copies till now.

    #Iterating to find the data nodes which are NOT in the sources array.
    for DataNodeIdx in range (0,NumberOfThreads):

        if(counter == MinumumNumberOfCopies):   #Check if the number of copies of the wanted fill reached the minimum wanted number or not.
            break
        if State[DataNodeIdx] == 'online' and not sources.__contains__((DataNodeIdx,)):
            FreePort = 'none'
            while FreePort == 'none':   #Stalling till get a free port from that data node.
                FreePort,PortIP = GetFreePort(DataNodeIdx)
            TransferPorts[FreePort] = 'busy'
            counter+=1     #Increase the number of copies.
            Info += ' '
            Info += PortIP
            Info += ' '
            Info += FreePort

    return Info

def StartCopying(SourceNode,ChosenNodes,FilePath):

    ToDataNode = context.socket(zmq.REQ)
    ToDataNode.connect ("tcp://%s:%s" % (IP[SourceNode],DataNodeAsServerPort))

    ToDataNode.send_string(ChosenNodes)
    ToDataNode.recv_string()
    print('FilePath:')
    print(FilePath)
    ToDataNode.send_string(FilePath)        #Sending the file path to the data node to be able access it and send it to the other chosen data nodes.
    ToDataNode.recv_string()        #Note: the source node will not send a respond till it finished the copying porcess.

def CountCopies(name):
    mycursor.execute("SELECT COUNT(*) FROM records WHERE filename = %s",(name,))
    counter = mycursor.fetchone()
    return counter[0]       #Remeber, it comes from the database, so it's an array of arrays.


def ManageRuplicating():
    
    print('Searching for files that should be replicated..')
    for name in VideoNames:     # VideoNames is an array of arrays, beacuse it's a result of retrieve data from database.
        if(CountCopies(name[0]) < MinumumNumberOfCopies):
            q.put(name[0])
    if(q.empty()):
        print('There is no files need to be replicated in the system for now.')
    while True:
        while q.qsize() > 0:
            VideoName = q.get()
            print('Replicating status: A file Found: ' + VideoName)
            SourceNode = GetSourceNode(VideoName)       #SourceNode contains = [datanode_id , file path]
            ChosenNodes = GetChosenNodes(VideoName)
            print('Chosen nodes data:')
            print(ChosenNodes)
            StartCopying(SourceNode[0],ChosenNodes,SourceNode[1])
            DataNodeAsSource[SourceNode[0]] = 'no'

#------------------------------Main------------------------------#
if __name__ == '__main__':

    #Get the records from the database to iterates on the video's name.
    mycursor.execute("SELECT DISTINCT filename FROM records;")
    VideoNames = mycursor.fetchall()


    for i in range(0,NumberOfThreads):  #Running a thread for every connected data node.
        Threads.append(threading.Thread(name = str(i),target=Connect, args=(i,) ) )
        Threads[i].start()
        time.sleep(0.5)

    ClientThread = threading.Thread(name = 'Client Thread',target=ClientsHandler)  #Creating a thread to deal with the incoming clients.
    ClientThread.start()

    print('Master Tracker Stablished successfully.')

    time.sleep(1)
    for i in range(0,NumberOfThreads):
        if(State[i] == 'offline'):
            print('Data Node number %d is offline' %i)

    ReplicateThread = threading.Thread(name = 'Replication Thread',target=ManageRuplicating)
    ReplicateThread.start()

    while True:
        for i in range(0,NumberOfThreads):
            Current_Time = time.time()
            if(Current_Time - LastTime[i] > 1.5 and State[i] == 'online'):
                State[i] = 'offline'
                print('Warning !! Data Node number %d is offline' %i)
            if(Current_Time - LastTime[i] < 1.5):
                if(State[i] == 'offline'):
                    print('Machine number ' + str(i) + ' is reconnected')
                State[i] = 'online'
