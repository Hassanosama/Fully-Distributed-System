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
                        


for i in range(0, 3):
	Threads.append(threading.Thread(name = str(i),target=Transfer, args=(i,) ))
	Threads[i].start()
print('Server started')
#Main Thread.
while True:
	publisher.send_string('Alive')
	time.sleep(1)                   
