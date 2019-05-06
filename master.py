from multiprocessing import Process, Queue
import mysql.connector
import zmq
import sys
import random
import time

def client_signup(q1, q2, q3):
    #port between client and master server
    client_port = "5556"

    context = zmq.Context()
    #socket between client and master
    socket_client = context.socket(zmq.REP)
    socket_client.bind("tcp://*:%s" % client_port)

    try:
        mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="123456789",
        database="os_project"
        )

        slaveip1 = "192.168.43.37"
        slaveip2 = "192.168.43.172"
        slaveip3 = ""
        slave1_process = Process(target=inform_slave, args=(slaveip1, 1, q1,))
        slave2_process = Process(target=inform_slave, args=(slaveip2, 2, q2,))
        slave1_process.start()
        slave2_process.start()

        mycursor = mydb.cursor()
        sql = "INSERT INTO users(username, passwd, email) VALUES(%s, %s, %s)"
                
        while True:
            clientInfo = socket_client.recv_string()

            #print("receive " + clientInfo)
            clientData = clientInfo.split()
            name = clientData[0]
            email = clientData[1]
            passwd = clientData[2]
            val = (name, passwd, email)
                
            try:
                mycursor.execute(sql, val)
                mycursor.execute("select user_id from users where email = %s", (email,))
                myresults = mycursor.fetchall()
                
                socket_client.send_string("Success: your account is ready"+str(myresults[0][0]))
               
                mydb.commit()

                q1.put(clientInfo)
                q2.put(clientInfo)
                q3.put(clientInfo)
                
            except:
                socket_client.send_string("Failed: either username or email is used before")
    except:
        print("Can't connect to database")


def inform_slave(slaveip, num, q):
    #port between client and master server
    slave_port = "5558"
    #socket between master and slave
    context = zmq.Context()
    socket_slave = context.socket(zmq.REQ)
    socket_slave.connect ("tcp://%s:%s" % (slaveip, slave_port) )
    socket_slave.linger = 0

    while True:
        info = q.get()
        socket_slave.send_string(info)

        print("Sending to slave " + str(num))            
        slave_Response = socket_slave.recv_string()
        print(slave_Response + " In Slave " + str(num))
                

if __name__ == '__main__':
    print('Master Started')
    q1 = Queue()
    q2 = Queue()
    q3 = Queue()
    client_Process = Process(target=client_signup, args=(q1, q2, q3,))
    client_Process.start()
  