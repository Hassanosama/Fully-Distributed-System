import zmq
import sys
import random
import time
import os

#Initializing Variables.
MasterPort = '5555'
MasterIP = 'localhost'
FileName = ''
context = zmq.Context()
DataNodeSocket = context.socket(zmq.REQ)
NumberOfConnectedServers = 0
MaxNumberBytes = 10000
MasterSocket = context.socket(zmq.REQ)
MasterSocket.connect ("tcp://%s:%s" % (MasterIP,MasterPort) )
#----------------------------

#------------------------------------------------------------------
def connectMaster(info):

    port = "5556"
    #print ("Connecting to server...\n")
    socket = context.socket(zmq.REQ)
    socket.connect ("tcp://%s:%s" % (MasterIP, port) )
    socket.linger = 0

    socket.send_string(info)
    time.sleep(0.3)
    master_Response = "Server Down, Try again later"
    try:
        master_Response = socket.recv_string(flags=zmq.NOBLOCK)
        return master_Response
    except:
        pass
    return master_Response

#------------------------------------------------------------------
def connectSlave(num, info):
    
    slave = "192.168.137.146"
    #if(num == 2):
    #    slave = ip2
    
    port = "5556"

    #print ("Connecting to server...\n")
    socket = context.socket(zmq.REQ)
    socket.connect ("tcp://%s:%s" % (slave, port) )
    socket.linger = 0

    socket.send_string(info)
    time.sleep(0.3)
    slave_Response = "Server Down, Try again later"
    try:
        slave_Response = socket.recv_string(flags=zmq.NOBLOCK)
        return slave_Response
    except:
        pass
    return slave_Response

#------------------------------------------------------------------
def client():
    print("(1) Sign up\n(2) login")
    choice = 0
    while(choice != 1 and choice != 2):
        choice = int(input())


    f = 0
    #Sign Up
    if(choice == 1):
        passwd = ""
        while True:
            print("- Enter your username: ")
            name = input()
            print("- Enter your email: ")
            email = input()
            if f == 0:
                print("- Enter your password: ")
                passwd = input()

            if(len(name) == 0 or len(email) == 0 or len(passwd) == 0):
                print("Invalid Input")
                continue    
            info = name + " " + email + " " + passwd
            print("Loading: Please wait a while\n")

            master_Response = connectMaster(info)
            print(master_Response)

            if master_Response == "Success: your account is ready":
                break
            elif master_Response == "Failed: either username or email is used before":
                f = 1       
            elif master_Response == "Server Down, Try again later":
                return
            
    #Login
    else:
        
        while True:
            print("- Enter your email: ")
            email = input()
            print("- Enter your password: ")
            passwd = input()
            
            if(len(email) == 0 or len(passwd) == 0):
                print("Invalid Input")
                continue
            info = email + " " + passwd
            print("Loading: Please wait a while\n")

            cnt = 0
            slave_Response = "Servers are down, Try again later"
            while(slave_Response == "Servers are down, Try again later" and cnt < 1000):
                num = random.randint(1,1)
                slave_Response = connectSlave(num, info)
                cnt += 1

            print(slave_Response)        
            if slave_Response == "Login Successfully, You can procced":
                break
            elif slave_Response == "Login Failed, password or email is invalid, Enter them again":
                pass
            else:
                return
    LoggedIn()            

#------------------------------------------------------------------
def MakeConnectionWithDataNodes():
    Info = MasterSocket.recv_string().split()
                    #NOTE: even indices of Info are IP's and odd are ports. ex: Info[0] = IP,Info[1] = Port,Info[2] = IP and so on.
    for i in range(0,len(Info),2):          #NOTE: User will connect to more than one Data Node in case of download
        DataNodeSocket.connect ("tcp://%s:%s" %(Info[i],Info[i+1]))
        #print(Info[i] + ' ' + Info[i+1])
        
    x = int(len(Info)/2)
    return x

#------------------------------------------------------------------
def Download(NumberOfConnectedServers):
    AllData = []
    Done = []
    for i in range(0,NumberOfConnectedServers):
        AllData.append([])
        Done.append(0)
    FileSize = ''
    for i in range(0,NumberOfConnectedServers):
        DataNodeSocket.send(b'download')
        DataNodeSocket.recv()
    for i in range(0,NumberOfConnectedServers):
        DataNodeSocket.send_string(FileName)
        FileSize = DataNodeSocket.recv_string()
    for i in range(0,NumberOfConnectedServers):
        DataNodeSocket.send_string(str(NumberOfConnectedServers))
        DataNodeSocket.recv()
    print('connected.')

    completed = 0 # number of servers which completed transfereing.
    delivered = 0 # number of bytes which transfered.
    while True:
        
        for i in range(0,NumberOfConnectedServers):
            if(Done[i] == 1):
                continue
            
            DataNodeSocket.send(b'ready ' + str.encode(str(i)))
            data = DataNodeSocket.recv()
            if(data != b'done'):
                delivered+=len(data)
                per = int(delivered/int(FileSize)*100)
                print('Downloading.. [%d%%]\r'%per, end="")
                AllData[i].append(data)
                
            else:
                Done[i] = 1
                MasterSocket.send_string('check')
                MasterSocket.recv_string()
                completed+=1
                
                
        if(NumberOfConnectedServers == completed):
            completed = 0  # Now completed variable will represent the size of the constructed file.
            print('File Downloaded successfully from the servers.')
            print('Please wait while constructing the file..')
            file = open(FileName,'wb')
            for i in range(0,NumberOfConnectedServers):
                for b in AllData[i]:
                    file.write(b)
                    completed+=1
                    per = int(completed//int(FileSize)*100)
                    print('Constructing.. [%d%%]\r'%per, end="")

            file.close()
            break
    print('Your File is ready now.')

#------------------------------------------------------------------
def Upload():
    DataNodeSocket.send(b'upload')
    DataNodeSocket.recv()
    DataNodeSocket.send_string(FileName)
    DataNodeSocket.recv()
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
            if(n == MaxNumberBytes):
                n = 0
                DataNodeSocket.send(data)
                data = b''
                respond = DataNodeSocket.recv()
                if(respond != b'ok'):
                    print('Error occured, Transfer Failed.')
                    break
    if(n > 0):
        n = 0
        DataNodeSocket.send(data)
        data = b''
        respond = DataNodeSocket.recv()
        if(respond != b'ok'):
            print('Error occured, Transfer Failed.')
    DataNodeSocket.send(b'done')
    DataNodeSocket.recv()
    MasterSocket.send_string('check')
    MasterSocket.recv_string()
    print('The File is successfully uploaded.')

#------------------------------------------------------------------
def LoggedIn():
    print('Welcome..')
    print('Choose type of operation:\n(1)Upload.\n(2)Download.')
    Operation = input()
    print('Now Please enter the file name')
    FileName = input()
    print('Establishing connection, Please wait..')
    #------------------------------------------------
    if Operation == '1':
        MasterSocket.send_string('Upload')          #Make an Upload request.
        NumberOfConnectedServers = MakeConnectionWithDataNodes()
        Upload()
    else:
        MasterSocket.send_string('Download')          #Make a Download request.
        NumberOfConnectedServers = MakeConnectionWithDataNodes()
        Download(NumberOfConnectedServers)

    os.system('pause')
    


if __name__ == "__main__":
	client()
