import zmq
import time
import sys
import threading
import queue 

print('Establishing Master Tracker,Please Wait..')

#Initializing Variables.
LookUpTable = {}
q = queue.Queue()

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
Server = context.socket(zmq.REP)
Server.bind("tcp://*:%s" % MasterPort)

for i in range(0,NumberOfThreads):
    subscribers.append(context.socket(zmq.SUB))
    subscribers[i].connect('tcp://%s:%s' %(IP[i],Ports[i]) )
    subscribers[i].setsockopt(zmq.SUBSCRIBE, b'')
    msg.append('')

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
                q.put(FileName)

            else:
                PortNumber = subscribers[ID].recv_string()

            TransferPorts[PortNumber] = 'free'



def GetFreePort(idx):
    if (State[idx] == 'offline'):
        return 'none','none'
    
    for p in DataNodePorts[idx]:
        if(TransferPorts[p] == 'free'):
            return p,IP[idx]
    return 'none','none'
    
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

#Main..........................................................................
for i in range(0,NumberOfThreads):
    Threads.append( threading.Thread(name = str(i),target=Connect, args=(i,) ) )
    Threads[i].start()
    time.sleep(0.5)

ClientThread = threading.Thread(target=ClientsHandler)
ClientThread.start()

print('Master Tracker Stablished successfully.')

time.sleep(1)
for i in range(0,3):
    if(State[i] == 'offline'):
        print('Data Node number %d is offline' %i)

while True:
    for i in range(0,3):
        Current_Time = time.time()
        if(Current_Time - LastTime[i] > 1.5 and State[i] == 'online'):
            State[i] = 'offline'
            print('Warning !! Data Node number %d is offline' %i)
        #if(!q.empty()):
            
            
























        

