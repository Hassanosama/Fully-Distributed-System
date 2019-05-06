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
MaxNumberBytes = 1000000
BytesPerIteration = 200
MasterSocket = context.socket(zmq.REQ)
MasterSocket.connect ("tcp://%s:%s" % (MasterIP,MasterPort) )
#----------------------------
client_id = 0
#------------------------------------------------------------------
def connectMaster(info):

    port = "5556"
    #print ("Connecting to server...\n")
    socket = context.socket(zmq.REQ)
    socket.connect ("tcp://%s:%s" % (MasterIP, port) )
    socket.linger = 0

    socket.send_string(info)
    time.sleep(0.6)
    master_Response = "Server Down, Try again later"
    try:
        master_Response = socket.recv_string(flags=zmq.NOBLOCK)
        return master_Response
    except:
        pass
    return master_Response

#------------------------------------------------------------------
def connectSlave(num, info):
    
    slavelst = [None] * 2
    slavelst[0] = "localhost"
    slavelst[1] = "localhost"
    
    slave = slavelst[num]
    
    port = "5556"

    #print ("Connecting to server...\n")
    socket = context.socket(zmq.REQ)
    socket.connect ("tcp://%s:%s" % (slave, port) )
    socket.linger = 0

    socket.send_string(info)
    time.sleep(0.6)
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
            client_id = int(master_Response[-1])
            
            if "Success" in master_Response:
                print("Successful sign up")        
                break
            elif "Failed" in master_Response:
                f = 1
                print(master_Response)       
            elif "Server" in master_Response:
                print(master_Response)
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
            slave_Response = "Server Down, Try again later"
            while(slave_Response == "Server Down, Try again later" and cnt < 10):
                print("Trying")
                num = random.randint(0,1)
                slave_Response = connectSlave(num, info)
                client_id = int(slave_Response[-1])
                cnt += 1

            if "Success" in slave_Response:
                print("Successful login")
                break
            elif slave_Response == "Login Failed, password or email is invalid, Enter them again":
                print(slave_Response) 
                pass
            else:
                print(slave_Response) 
                return
    print(client_id)
    LoggedIn()            

#------------------------------------------------------------------
def MakeConnectionWithDataNodes():
    Info = MasterSocket.recv_string().split()
                    #NOTE: even indices of Info are IP's and odd are ports. ex: Info[0] = IP,Info[1] = Port,Info[2] = IP and so on.
    for i in range(0,len(Info),2):          #NOTE: User will connect to more than one Data Node in case of download
        DataNodeSocket.connect ("tcp://%s:%s" %(Info[i],Info[i+1]))
        #print(Info[i] + ' ' + Info[i+1])
        
    x = int(len(Info)/2)
    
    print('number of servers: '+ str(x))
    print(Info)
    return x

#------------------------------------------------------------------
def Download(FileName, NumberOfConnectedServers):
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
            print(FileName)
            for i in range(0,NumberOfConnectedServers):
                for b in AllData[i]:
                    file.write(b)
                    completed+=1
                    per = int(completed/int(FileSize)*100)
                    print('Constructing.. [%d%%]\r'%per, end="")

            file.close()
            break
    print('Your File is ready now.')

#------------------------------------------------------------------
def Upload(FileName):
    DataNodeSocket.send(b'upload')
    DataNodeSocket.recv()
    DataNodeSocket.send_string(FileName)
    DataNodeSocket.recv()
    data = b''
    n = 0
    delivered = 0
    FileSize = os.stat(FileName).st_size
    with open(FileName, "rb") as f:
        byte = f.read(BytesPerIteration)
        n+=BytesPerIteration
        data+=byte
        while byte != b"":
            byte = f.read(BytesPerIteration)
            data+=byte
            n+=BytesPerIteration
            delivered+=BytesPerIteration
            per = int(delivered/int(FileSize)*100)
            print('Uploading.. [%d%%]\r'%per, end="")
            if(n >= MaxNumberBytes):
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
    DataNodeSocket.send(b'done' + str.encode(str(client_id)))
    DataNodeSocket.recv()
    MasterSocket.send_string('check')
    MasterSocket.recv_string()
    print('The File is successfully uploaded.')

#------------------------------------------------------------------
def ViewList(List):
    label = 1
    for video in List:
        print('(%d) %s [%s] baytes' %(label,video[0],video[1]))
        label+=1
def GetList():
    row = MasterSocket.recv_string()
    List = []
    while row != 'done':
        MasterSocket.send_string('')
        List.append(row.split('|'))
        row = MasterSocket.recv_string()
    return  List
    


def LoggedIn():
    print('Welcome..')
    print('Choose type of operation:\n(1)Upload.\n(2)Download.')
    Operation = input()
    #------------------------------------------------
    while True:
        if Operation == '1':
            print('Now Please enter the file name')
            FileName = input()
            print('Establishing connection, Please wait..')
            MasterSocket.send_string('Upload')          #Make an Upload request.
            NumberOfConnectedServers = MakeConnectionWithDataNodes()
            Upload(FileName)
            break
        elif Operation == '2':
            MasterSocket.send_string('Download')          #Make a Download request.
            List = GetList()
            ViewList(List)
            print('Now Please enter the number of the video.')

            while True:
                try:
                    idx = int(input())          #Index will be 1-based.
                    idx-=1      # converting it to 0-based
                    if(idx < len(List) and idx >= 0):
                        break
                    else:
                        print('Please enter a number in range :)')
                except:
                    print('Please enter a valid input :)')

            print('Establishing connection, Please wait..')
            FileName = List[idx][0]
            MasterSocket.send_string(FileName)
            NumberOfConnectedServers = MakeConnectionWithDataNodes()
            Download(FileName,NumberOfConnectedServers)
            break
        else:
            print('Please Enter a vaild operation :)')


    os.system('pause')
    


if __name__ == "__main__":
	client()
