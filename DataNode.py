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
MaxNumberBytes = 1000000
MaxBytesCopying = 1000000
BytesPerIteration = 200
ServerForMasterPort = '1212'
CopyingPort = '1111'
Lock = threading.Lock()

context = zmq.Context() 
publisher = context.socket(zmq.PUB)
publisher.sndhwm = 1100000
publisher.bind('tcp://*:%s' %RepPort)

for i in range(0,3):
        Sockets.append(context.socket(zmq.REP))
        Sockets[i].bind ("tcp://*:%s" % (Ports[i]))


def Publish(msgs):
        #Critical Section.
        Lock.acquire()
        for msg in msgs:
                publisher.send_string(msg)
        Lock.release()


def Transfer(ID):
        while True:
                msg = Sockets[ID].recv()
                Sockets[ID].send(b'')
                if(msg == b'upload'):
                        print('A file will be uploaded to me..')
                        FileName = Sockets[ID].recv_string()
                        print('the file name is:')
                        print(FileName)
                        print('--------')
                        file = open(FileName,'wb')
                        Sockets[ID].send(b'')
                        user_id = 0
                        while True:
                                msg = Sockets[ID].recv()
                                Sockets[ID].send(b'ok')
                                if(not (b'done' in msg)):
                                        file.write(msg)
                                else:
                                        file.close()
                                        msg = msg.decode()
                                        msg = msg.replace('done','')
                                        user_id = msg
                                        break
                        Publish(['Uploaded' + '|' +msg])
                        print('Done..')
                        FilePath = os.path.dirname(os.path.realpath(FileName))
                        FilePath = FilePath + chr(92) + FileName                # 92 is the ASCII code of '\' symbol.
                        FileSize = os.stat(FileName).st_size
                        msgs = []
                        msgs.append(str(FileName))
                        msgs.append(str(FilePath))
                        msgs.append(str(Ports[ID]))
                        msgs.append(str(FileSize))
                        Publish(msgs)
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
                                am = min(BytesPerIteration,size)
                                size-=am
                                byte = file.read(am)
                                data+=byte
                                counter+=am
                                if(counter >= MaxNumberBytes):
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
                        msgs = []
                        msgs.append('Downloaded')
                        msgs.append(str(Ports[ID]))
                        Publish(msgs)

def GetFileNameFromFilePath(FilePath):
        tmp = FilePath.split(chr(92))
        return tmp[len(tmp) - 1]

def StartCopying(Info,FilePath):

        CopyingSocket  = context.socket(zmq.REQ)
        print('The File Path is' + FilePath)
        FileName = GetFileNameFromFilePath(FilePath)
        print('The File Name is' + FileName)
        for i in range(0,len(Info),2):          #NOTE: User will connect to more than one Data Node in case of download
                CopyingSocket.connect ("tcp://%s:%s" %(Info[i],Info[i+1]))
        
        NumberOfMachines = len(Info) // 2
        for i in range(0,NumberOfMachines):
                CopyingSocket.send(b'upload')
                CopyingSocket.recv()
        for i in range(0,NumberOfMachines):
                CopyingSocket.send_string(FileName)
                CopyingSocket.recv()

        data = b''
        n = 0
        completed = 0
        FileSize = os.stat(FileName).st_size
        print(FileName)
        print(FileSize)
        with open(FileName, "rb") as f:
                byte = f.read(BytesPerIteration)
                n+=BytesPerIteration
                completed+=BytesPerIteration
                data+=byte
                while byte != b"":
                        byte = f.read(BytesPerIteration)
                        data+=byte
                        n+=BytesPerIteration
                        completed+=BytesPerIteration
                        per = int(completed/int(FileSize)*100)
                        print('Sending.. [%d%%]\r'%per, end="")
                        if(n >= MaxBytesCopying):
                                print('In the If condition')
                                n = 0
                                for i in range(0,NumberOfMachines):
                                        CopyingSocket.send(data)
                                        print('I Sent the data')
                                        respond = CopyingSocket.recv()
                                        if(respond != b'ok'):
                                                print('Error occured, Transfer Failed.')
                                                break
                                data = b''
                print('out of the file.')
                                
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
                Info = Server.recv_string().split()
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
	Publish(['Alive'])
	time.sleep(1)                   
