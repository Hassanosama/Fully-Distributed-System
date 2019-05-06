from multiprocessing import Process, Queue
import mysql.connector
import zmq
import sys
import random


def client_login():
    
    # Client port
    port = "5556"
    context = zmq.Context()
    socket_client = context.socket(zmq.REP)
    socket_client.bind("tcp://*:%s" % port)

    try:
        mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="1234",
        database="os_project"
        )
        mycursor = mydb.cursor()
        sql = "SELECT * FROM users WHERE email = %s AND passwd = %s"
           
        while True:
            
            client_message = socket_client.recv_string()
            print("Recieved CLient request")
            info = client_message.split()
            email = info[0]
            pw = info[1]
        
            val = (email, pw)

            mycursor.execute(sql, val)
            mycursor.fetchall()

            if mycursor.rowcount == 1:
                mycursor.execute("select user_id from users where email = %s", (email,))
                myresults = mycursor.fetchall()
               
                socket_client.send_string("Login Successfully, You can procced"+str(myresults[0][0]))
            else:
                socket_client.send_string("Login Failed, password or email is invalid, Enter them again")
    except:
        print("Can't connect to database")    


def master_recieve():

    port = "5558"
    context = zmq.Context()
    socket_master = context.socket(zmq.REP)
    socket_master.bind("tcp://*:%s" % port)
    
    try:
        mydb = mysql.connector.connect(
                host="localhost",
                user="root",
                passwd="1234",
                database="os_project"
        )
        mycursor = mydb.cursor()
        sql = "INSERT INTO users(username, passwd, email) VALUES(%s, %s, %s)"
          
        while True:

            master_message = socket_master.recv_string()
            print("Recieved Master request")
            info = master_message.split()
            name = info[0]
            email = info[1]
            passwd = info[2]
            
            val = (name, passwd, email)
            mycursor.execute(sql, val)
            socket_master.send_string("Insertion Done")

            mydb.commit()
    except:
        print("Can't connect to database")


if __name__ == "__main__":
    print("Slave Started")
    client_Process = Process(target=client_login, args=())
    master_Process = Process(target=master_recieve, args=())
    client_Process.start()
    master_Process.start()
