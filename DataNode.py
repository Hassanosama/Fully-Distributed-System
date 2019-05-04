import zmq
import sys
import random
import time
import threading
import os
import math

RepPort = "6000" 
Ports = ['6001','6002','6003'] 
Sockets = []
Threads = []
MaxNumberBytes = 10000
MaxBytesCopying = 50000
ServerForMasterPort = '1212'
CopyingPort = '1111'


context = zmq.Context() 
publisher = context.socket(zmq.PUB)
publisher.sndhwm = 1100000
publisher.bind('tcp://*:%s' %RepPort)

for i in range(0,3):
        Sockets.append(context.socket(zmq.REP))
        Sockets[i].bind ("tcp://*:%s" % (Ports[i]))

def Transfer(ID):
        while True:
                msg = Sockets[ID].recv()
                Sockets[ID].send(b'')
                if(msg == b'upload'):
                        FileName = Sockets[ID].recv_string()
                        file = open(FileName,'wb')
                        Sockets[ID].send(b'')
                        while True:
                                msg = Sockets[ID].recv()
                                Sockets[ID].send(b'ok')
                                if(msg != b'done'):
                                        file.write(msg)
                                else:
                                        file.close()
                                        break
                        publisher.send_string('Uploaded')
                        FilePath = os.path.dirname(os.path.realpath(FileName))
                        publisher.send_string(FileName)
                        publisher.send_string(FilePath)
                        publisher.send_string(Ports[ID])
                else:   #Download
                        
                        FileName = Sockets[ID].recv_string()
                        FileSize = os.stat(FileName).st_size
                        Sockets[ID].send_string(str(FileSize))
                        
                        Servers = int(Sockets[ID].recv_string())        #Number Of servers
                        Sockets[ID].send(b'')
                        
                        #Now every server will transfer ceil(FileSize / servers)
                        size = math.ceil(int(FileSize)/Servers)
                        file = open(FileName,'rb')
                        
                        msg = Sockets[ID].recv()         #Receving My Role in sending.
                        dummy , part = str(msg.decode()).split()
                        part = int(part)
                        file.read(part*size)
                        data = b''
                        counter = 0
                        while (size > 0):
                                size-=1
                                byte = file.read(1)
                                data+=byte
                                counter+=1
                                if(counter == MaxNumberBytes):
                                        counter = 0
                                        Sockets[ID].send(data)
                                        data = b''
                                        Sockets[ID].recv()
                        if(counter > 0):
                                counter = 0
                                Sockets[ID].send(data)
                                data = b''
                                respond = Sockets[ID].recv()
                        file.close()
                        Sockets[ID].send(b'done')
                        publisher.send_string('Downloaded')
                        publisher.send_string(Ports[ID])

def StartCopying(Info,FileName):
        CopyingSocket  = context.socket(zmq.REQ)

        for i in range(0,len(Info),2):          #NOTE: User will connect to more than one Data Node in case of download
                CopyingSocket.connect ("tcp://%s:%s" %(Info[i],Info[i+1]))
        
        NumberOfMachines = len(Info) / 2
        for i in range(0,NumberOfMachines):
                CopyingSocket.send(b'upload')
                CopyingSocket.recv()
        for i in range(0,NumberOfMachines):
                CopyingSocket.send_string(FileName)
                CopyingSocket.recv()

        data = b''
        n = 0
        with open(FileName, "rb") as f:
                byte = f.read(1)
                n+=1
                data+=byte
                while byte != b"":
                        byte = f.read(1)
                        data+=byte
                        n+=1
                        if(n == MaxBytesCopying):
                                n = 0
                                for i in range(0,NumberOfMachines):
                                        CopyingSocket.send(data)
                                        respond = CopyingSocket.recv()
                                        if(respond != b'ok'):
                                                print('Error occured, Transfer Failed.')
                                                break
                                data = b''
                                
        if(n > 0):
                n = 0
                for i in range(0,NumberOfMachines):
                        CopyingSocket.send(data)
                        respond = CopyingSocket.recv()
                        if(respond != b'ok'):
                                print('Error occured, Transfer Failed.')
                data = b''

        CopyingSocket.send(b'done')
        CopyingSocket.recv()

        print('Done Copying')
        

def Replicating():
        Server = context.socket(zmq.REP)
        Server.bind("tcp://*:%s" % ServerForMasterPort)
        while True:
                Info = Server.recv_string()
                Server.send_string('')

                StartCopying(Info,Server.recv_string())
                Server.send_string('done')
        

#---------------------------------------Main---------------------------------------
for i in range(0, 3):
	Threads.append(threading.Thread(name = str(i),target=Transfer, args=(i,) ))
	Threads[i].start()
print('Server started')
ReplicatingThread = threading.Thread(target = Replicating)
ReplicatingThread.start()
#Main Thread.
while True:
	publisher.send_string('Alive')
	time.sleep(1)                   