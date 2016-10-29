from socket import *
from threading import Thread, Lock, Semaphore, Timer
import json
import time



class Slave:
    def __init__(self, zookeeper, master, ip):
        self.zookeeper = zookeeper
        self.ip = ip
        print("I am the slave")
        self.name = self.zookeeper.create("/slave", ephemeral=True, sequence=True, value=ip.encode())
        self.master = master
        print(master)
        self.respIndices = []  # list of indices
        self.mainDict = {}  # 0:{}
        self.backupDict = {}  # same for backup..
        self.masterSocket = socket(AF_INET, SOCK_STREAM)
        self.masterSocket.connect((master[0], int(master[1])))
        self.safeMode = True
        self.clientCount = 0
        self.lock = Lock()
        self.lock2 = Lock()
        self.time_updated=time.clock()
        self.sem = Semaphore(0)
        self.buddy = "0.0.0.0"
        Thread(target=self.reportToMaster).start()
        Thread(target=self.listenClients).start()
        self.num_updates = 0
        self.unsync = {}#dict is like mainDict
        self.interpreter()

    def interpreter(self):
        while True:
            cmd=input(">>> ")
            try:
                print(exec(cmd))
                print(eval(cmd))
            except Exception as e:
                print(e)

    def process(self, args):
        if args['operation'] == 'requestSafeBootStart':
            self.startSafeBoot()
        elif args['operation'] == 'transfer':
            self.transfer(args)
        elif args['operation'] == 'setKeysResponsible':
            self.setKeysResponsible(args)
        elif args['operation'] == 'setBuddy':
            self.setBuddy(args)
        elif args['operation'] == 'safeBootStop':
            self.stopSafeBoot()
        self.masterSocket.send("OK".encode())

    def transfer(self, args):
        '''
            {'data':{'target_ip':value, 'key_ranges':list_of_indices}}
        '''
        target = args['data']['target_ip']
        conn_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn_sock.connect((target, 1234))
        key_ranges = args['data']['key_ranges']
        data = {i: self.mainDict[i] for i in key_ranges}
        message = {'method': 'GET*', 'args': [data]}
        conn_sock.send(str(message).encode())
        print('transfer', conn_sock.recv(2048))

    def setKeysResponsible(self, args):
        self.respIndices = args['data']  # data-list of indexes
        notResp = set(self.mainDict.keys()) - set(self.respIndices)  #
        for key in notResp:
            del self.mainDict[key]
        newResp = set(self.respIndices) - set(self.mainDict.keys())  #
        for key in newResp:
            #Todo if this key is in backupdict, copy from backup else {}
            self.mainDict[key] = {}

    def reportToMaster(self):
        '''
            format : {'data':value, 'operation':'setBuddy'}
        '''
        self.masterSocket.send(self.name.encode())
        self.config = eval(self.masterSocket.recv(2048).decode())  # config = {0:(0,5),1:(5,10)...}
        while True:
            # Listens to commands by master
            response = eval(self.masterSocket.recv(2048).decode())
            # debug.
            print('received', response)
            self.process(response)
            # commands: requestSafeBootStart, transfer(kr,ip), setKeysResponsible(kr), setBuddy(ip), safeBootStop
        pass

    def setBuddy(self, args):
        '''
            format : {'data':value}
        '''
        self.buddy=args['data']
        #self.sync(force=True,full=True)
        pass

    def sync(self,update=(None,None,None),force=False,full=False):
        '''{}
        :param update: krIndex,key,value
        :param force:
        :param full:
        '''

        if force:
            #if full:send mainDict
            #else if not safeMode and  (current_time - time_updated >= 9 and num_updates>=1), sync and reset
            #else ignore
            if(full):
                self.sendToBuddy(self.mainDict,purge=True)
            elif not self.safeMode and\
                (time.clock()-self.time_updated >=9 and self.num_updates>=1):
                self.sendToBuddy(self.unsync)
                with self.lock2:
                    self.unsync={}      #wait.
                    self.num_updates=0
        elif self.safeMode == False:
            #increment num_updates
            with self.lock2:
                self.num_updates+=1
                print(update)
                if(not(update[0] in self.unsync)):
                    self.unsync[update[0]] = {}
                self.unsync[update[0]][update[1]]=update[2]
            if self.num_updates==1:
                self.time_updated=time.clock()
                Timer(10.0,self.sync,[(),True,False]).start()
            elif self.num_updates==5:
                self.sendToBuddy(self.unsync)
                with self.lock2:
                    self.unsync={}
                    self.num_updates=0
            #if num_updates is 1, set the time_updated to current time and set timer for 10 sec
            #else if it is 5, send it off and reset num_updates and unsync

    def sendToBuddy(self,value,purge=False):
        conn_sock = socket(AF_INET, SOCK_STREAM)
        conn_sock.connect((self.buddy, 1234))
        toSend={"method":"DUPL","args":[value,purge]}
        conn_sock.send(str(toSend).encode())
        assert conn_sock.recv(2048).decode() == "OK"
        conn_sock.close()

    def stopSafeBoot(self):
        self.sync(force=True,full=True)
        self.safeMode = False
        self.masterSocket.send('OK'.encode())

    def listenClients(self):
        # Starts listening to clients when master instructs
        self.clientSocket = socket(AF_INET, SOCK_STREAM)
        self.clientSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.clientSocket.bind(("", 1234))
        while True:
            self.clientSocket.listen(5)
            conn_sock, addr = self.clientSocket.accept()
            Thread(target=self.serveClient, args=(conn_sock,)).start()
        pass

    def startSafeBoot(self):
        # sets safe mode variable to true; waits for semaphore to be 0
        # then informs the master saying it is ready for safe boot
        # now, it listens for commands by master and executes them
        self.safeMode = True
        print("Going to Safeboot")
        if self.clientCount != 0:
            self.sem.acquire()
        print("Sending okay")

    def get(self, key, conn_soc):
        """
        {'status': 0 1 2 3,"message":"", data:""}
        status codes : 0 - success, 1 - safeboot error, 2 - key not found, 3 - not responsible for key
    """
        if self.safeMode:
            # throw error
            conn_soc.send(json.dumps({'status': 1, 'message': 'safeboot error', 'data': None}).encode())
            return
        # increment clientcount
        with self.lock:
            self.clientCount += 1

        # Check if responsible for key (bool,index)
        responsible = self.responsible(key)
        if (responsible[0]):
            # If the key exists, then serve it
            if (key in self.mainDict[responsible[1]]):
                value = self.mainDict[responsible[1]][key]
                conn_soc.send(json.dumps({'status': 0, 'message': 'success', 'data': value}).encode())
            # Give key not found
            else:
                conn_soc.send(json.dumps({'status': 2, 'message': 'key not found', 'data': None}).encode())
        # not responsible for key
        else:
            conn_soc.send(json.dumps({'status': 3, 'message': 'not responsible', 'data': None}).encode())

        with self.lock:
            self.clientCount -= 1
            if self.safeMode and self.clientCount == 0:
                self.sem.release()

    def put(self, key, value, conn_soc):
        if self.safeMode:
            # throw error
            conn_soc.send(json.dumps({'status': 1, 'message': 'safeboot error', 'data': None}).encode())
            return
        # increment clientcount
        with self.lock:
            self.clientCount += 1
        # Check if responsible for key (bool,index)
        responsible = self.responsible(key)
        if (responsible[0]):
            # If the key exists, then return existing value?
            oldValue = self.mainDict[responsible[1]].get(key, None)
            self.mainDict[responsible[1]][key] = value
            self.sync(update=(responsible[1],key,value))
            conn_soc.send(json.dumps({'status': 0, 'message': 'success', 'data': oldValue}).encode())
        # not responsible for key
        else:
            conn_soc.send(json.dumps({'status': 3, 'message': 'not responsible', 'data': None}).encode())

        with self.lock:
            self.clientCount -= 1
            if self.safeMode and self.clientCount == 0:
                self.sem.release()

    def getDupl(self, update,purge, conn_soc):
        if(purge):
            self.backupDict=update
        else:
            self.backupDict.update(update)
        conn_soc.send('OK'.encode())

    def getS(self, update, conn_soc):  # update:{0:{},5:{},7:{}}
        if self.safeMode:
            self.mainDict.update(update)
            conn_soc.send('OK'.encode())

    def serveClient(self, conn_soc):
        """
        {'method':'GET','args':}
        """
        # waits for clients to give get and
        print("Recv client")
        data = conn_soc.recv(2048).decode()
        data = eval(data)
        method = data["method"]
        args = data["args"]
        # method, args = data.split(":")
        if method == "GET":
            self.get(args[0], conn_soc)
        elif method == "PUT":
            self.put(args[0], args[1], conn_soc)
        elif method == "DUPL":
            self.getDupl(args[0],args[1], conn_soc)
        elif method == "GET*":
            self.getS(args[0], conn_soc)
        conn_soc.close()

    def responsible(self, key):
        for index in (self.respIndices):
            range = self.config[index]
            if (range[0] <= key <= range[1]):
                return (True, index)
        else:
            return (False, None)
