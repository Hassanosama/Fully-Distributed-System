import zmq
import time
import sys
import threading
import queue

print('Establishing Master Tracker,Please Wait..')

#Initializing Variables.
RecordsFile = open('Records.txt','r')   #Load records.
LookUpTable = {}
VideoNames = {""}
Instance = []
MinumumNumberOfCopies = 3
q = queue.Queue()
DataNodeAsSource = ['no','no','no']
State = ['offline', 'offline', 'offline']   #State of the Data Nodes, Intilally all Machines are offline.
LastTime = [0, 0, 0] #Last Time Machine sent an Alive message.
NumberOfThreads = 2
context = zmq.Context()
Threads = []
subscribers = []  #notice that number if subscriber ports = number of threads.
msg = []    #notice that number if messages = number of threads.
Ports = ['6000','7000'] #Representitive Ports of the Data Nodes.
TransferPorts = {'6001':'free' , '6002':'free' , '6003':'free' , '7001':'free' , '7002':'free' , '7003':'free'}
DataNodePorts = [['6001','6002','6003'],['7001','7002','7003']]
IP = ['localhost','localhost']  #IP's of the Data Nodes
MasterPort = '5555'     # Master Port is the port which the users connect with.
DataNodeAsServerPort = '1212'

Server = context.socket(zmq.REP)
Server.bind("tcp://*:%s" % MasterPort)

for i in range(0,NumberOfThreads):
    subscribers.append(context.socket(zmq.SUB))
    subscribers[i].connect('tcp://%s:%s' %(IP[i],Ports[i]) )
    subscribers[i].setsockopt(zmq.SUBSCRIBE, b'')
    msg.append('')
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
            if msg[ID] == 'Uploaded':
                FileName = subscribers[ID].recv_string()
                FilePath = subscribers[ID].recv_string()
                PortNumber = subscribers[ID].recv_string()
                LookUpTable.update({FileName :[ID, FilePath,State[ID]]})
                RecordsFile.write(FileName+'|'+str(ID))
                Instance.append(FileName+'|'+str(ID))
                VideoNames.add(FileName)
                q.put(FileName)

            else:
                PortNumber = subscribers[ID].recv_string()

            TransferPorts[PortNumber] = 'free'
#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>


#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
def GetFreePort(idx):
    if (State[idx] == 'offline'):
        return 'none','none'
    
    for p in DataNodePorts[idx]:
        if(TransferPorts[p] == 'free'):
            return p,IP[idx]
    return 'none','none'
#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
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
        else:
            Info = ''
            while len(Info) == 0:
                for i in range(0,len(Ports)):
                    FreePort,PortIP = GetFreePort(i)
                    if(FreePort != 'none'):
                        Info += ' '
                        Info += PortIP
                        Info += ' '
                        Info += FreePort
                        TransferPorts[FreePort] = 'busy'
            print('Ports Found')
            Server.send_string(Info)
#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
def GetSourceNode(VideoName):
    for i in range(0,NumberOfThreads):
        if State[i] == 'online' and DataNodeAsSource[i] == 'no' and Instance.__contains__(VideoName +'|'+ str(i)):
            DataNodeAsSource[i] == 'yes'
            return i
def GetChosenNodes(VideoName):
    Info = ''
    counter = 1
    for i in range(0,NumberOfThreads):

        if (counter == MinumumNumberOfCopies):
            break

        if State[i] == 'online' and (not Instance.__contains__(VideoName +'|'+ str(i))):
            FreePort = 'none'
            while FreePort == 'none':
                FreePort,PortIP = GetFreePort(i)
            TransferPorts[FreePort] = 'busy'
            cnounter+=1
            Info += ' '
            Info += PortIP
            Info += ' '
            Info += FreePort
            
    return Info

def StartCopying(SourceNode,ChosenNodes,VideoName):

    ToDataNode = context.socket(zmq.REQ)
    ToDataNode.connect ("tcp://%s:%s" % (IP[SourceNode],DataNodeAsServerPort))

    ToDataNode.send_string(ChosenNodes)
    ToDataNode.recv_string()

    ToDataNode.send_string(VideoName)
    ToDataNode.recv_string()        #Note: the source node will not send a respond till it finished the copying porcess.

def CountCopies(name):
    counter = 0
    for i in range(0,NumberOfThreads):
        if(Instance.__contains__(name+'|'+str(i))):
            counter+=1
    return counter


def ManageRuplicating():
    
    for x in VideoNames:
        if(CountCopies(x) < MinumumNumberOfCopies):
            q.put(x)

    while True:
        while q.qsize() > 0:
            VideoName = q.get()
            SourceNode = GetSourceNode(VideoName)
            ChosenNodes = GetChosenNodes(VideoName)
            StartCopying(SourceNode,ChosenNodes,VideoName)
            DataNodeAsSource[SourceNode] = 'no'

#..........................................................................Main..........................................................................
RecordsFile = open('Records.txt','a')   #Load records.
for record in RecordsFile:
    record = str(record)
    record = record.replace('\n','')
    if(len(record) > 0):
        Instance.append(record)
        name,dummy = record.split('|')
        VideoNames.add(name)
RecordsFile.close()


for i in range(0,NumberOfThreads):  #Running a thread for every connected data node.
    Threads.append( threading.Thread(name = str(i),target=Connect, args=(i,) ) )
    Threads[i].start()
    time.sleep(0.5)

ClientThread = threading.Thread(target=ClientsHandler)  #Creating a thread to deal with the incoming clients.
ClientThread.start()

print('Master Tracker Stablished successfully.')

time.sleep(1)
for i in range(0,3):
    if(State[i] == 'offline'):
        print('Data Node number %d is offline' %i)

ReplicateThread = threading.Thread(target=ManageRuplicating)
ReplicateThread.start()

while True:
    for i in range(0,3):
        Current_Time = time.time()
        if(Current_Time - LastTime[i] > 1.5 and State[i] == 'online'):
            State[i] = 'offline'
            print('Warning !! Data Node number %d is offline' %i)
