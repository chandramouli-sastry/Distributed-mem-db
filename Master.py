from socket import *
from threading import *
class Master:
    def __init__(self, zookeeper, ip, slaveSocket):
        self.zookeeper = zookeeper
        self.ip = ip
        self.config = {0:[1,5],1:[6,10],2:[11,15],3:[16,20]}
        print("I am the master")
        self.slaveSocket = slaveSocket
        self.clientSocket = socket(AF_INET, SOCK_STREAM)
        self.clientSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        #todo change
        self.clientSocket.bind(("", 12345))
        self.safeMode = True
        self.slave_ip = {}#key: slave_index, value:ip,znode,sock
        self.kr_ip = {} #this dictionary should be returned to client
        self.timer = False
        self.slaves = []
        Thread(target=self.listenSlaves).start()


    def sendCommands(self):
        l=[{'operation':'requestSafeBootStart'},{'operation':'requestSafeBootStart'},{'operation':'setKeysResponsible','data':[0,1]},{'operation':'setKeysResponsible','data':[2,3]},{'operation':'setBuddy' , 'data':'192.168.0.57'},{'operation':'setBuddy' , 'data':'192.168.0.55'},{'operation':'safeBootStop'} ,{'operation':'safeBootStop'} ]
        ind = 0
        for i in l:
            socks[ind].send(str(i).encode())
            print('received', socks[ind].recv(2048).decode())
            ind = ind ^ 1
        while True:
            try:
                inp=int(input('Enter index'))
                com=input('Enter command')
                socks[inp].send(com.encode())
                print('received', socks[inp].recv(2048).decode())
            except Exception:
                print("wrong inp")


    def listenSlaves(self):
        global socks
        count = 0
        socks = []
        while True:
            count +=1
            self.slaveSocket.listen(5)
            sock , _ = self.slaveSocket.accept()
            #when the control is in this statement, it means that a slave just came up
            if not(self.timer):
                Timer(20.0,self.safeBoot)
                self.timer = True
            #Thread(target = self.onslaveup, args=(sock,)).start()
            sock.send(str(self.config).encode())
            self.onslaveup(sock)
            socks.append(sock)
            if count ==2:
                self.sendCommands()

    def onslaveup(self, sock):
        data = sock.recv(2048).decode()
        ip,_ = self.zookeeper.get(data,watch=self.onslavedown)
        self.slaves.append((ip,data,sock))

        pass

    def onslavedown(self,event):
        print("Slave went down",event)

    def safeBoot(self):
        #Request safe boot start to all the slaves-using the sock which we already store
        #now, clear the dictionary and key ranges
        #now, start assigning keyranges and buddy servers
        pass
    def listenClients(self):
        pass

    def handleClient(self):
        pass

