import zmq
import sys
import random
import time


def connectMaster(info):

    masterip = "localhost"
    port = "5556"
    context = zmq.Context()
    #print ("Connecting to server...\n")
    socket = context.socket(zmq.REQ)
    socket.connect ("tcp://%s:%s" % (masterip, port) )
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


def connectSlave(num, info):
    
    slave = "192.168.137.146"
    #if(num == 2):
    #    slave = ip2
    
    port = "5556"

    context = zmq.Context()
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
            


if __name__ == "__main__":
	client()