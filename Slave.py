from socket import *
from threading import Thread


class Slave:
    def __init__(self, zookeeper, master, ip):
        self.zookeeper = zookeeper
        self.ip = ip
        self.name = self.zookeeper.create("/slave", ephemeral=True, sequence=True, value=ip)
        self.master = master
        self.masterSocket = socket(AF_INET, SOCK_STREAM)
        self.masterSocket.connect((master[0], int(master[1])))
        self.safeMode = True
        Thread(target=self.reportToMaster).start()

    def reportToMaster(self):
        self.masterSocket.send(self.name)
        while True:
            # Listens to commands by master
            response = self.masterSocket.recv(2048)
        pass

    def modifyKeysResponsible(self):
        pass

    def setBuddy(self):
        # Will set buddy and duplicate the keys responsible
        pass

    def stopSafeBoot(self):
        pass

    def listenClients(self):
        # Starts listening to clients when master instructs
        self.clientSocket = socket(AF_INET, SOCK_STREAM)
        self.clientSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.clientSocket.bind(("", 1234))
        while True:
            conn_sock, addr = self.clientSocket.accept()
            Thread(target=self.serveClient, args=(conn_sock,))
        pass

    def startSafeBoot(self):
        # sets safe boot variable to true; waits for semaphore to be 0
        # then informs the master saying it is ready for safe boot

        pass

    def get(self, key, conn_soc):
        pass

    def put(self, key, value, conn_soc):
        # check safeboot variable before serving;
        # it'll increment a semaphore counter on entering and decrement after doing the change
        pass

    def sendDupl(self, key, value):
        pass

    def getDupl1(self, key, value, conn_soc):
        pass

    def getDuplS(self, keyRanges, dicts, conn_soc):
        pass

    def serveClient(self, conn_soc):
        # waits for clients to give get and
        data = conn_soc.recv(2048)
        method, args = data.split(":")
        if method == "GET":
            self.get(args, conn_soc)
        elif method == "PUT":
            self.put(args[0], args[1], conn_soc)
        elif method == "DUPL1":
            self.getDupl1(args[0], args[1], conn_soc)
        elif method == "DUPL*":
            self.getDuplS(args[0], args[1], conn_soc)
