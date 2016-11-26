import json
from socket import *
from threading import Thread, Lock, Timer

INTERVAL = 10.0

SOCKET = 1

IP = 0

KEY_RANGE = 2


class Master:
    def __init__(self, zookeeper, ip, slaveSocket):
        """
        Constructor to initialize the master. Starts required threads after
        initialization
        :param zookeeper: Zookeeper client
        :param ip: IP of self
        :param slaveSocket: Socket on which it needs to listen to slaves
        :return:
        """
        self.zookeeper = zookeeper
        self.ip = ip
        #Keyranges and ID assigned to each. Can dynamically increase or decrease
        #Will distribute to slaves accordingly
        self.config = {0: [1, 5], 1: [6, 10], 2: [11, 15], 3: [16, 20]}
        #Reverse dict of the config
        self.rev_config = {tuple(v): k for k, v in self.config.items()}
        print("I am the master")

        self.slaveSocket = slaveSocket
        #Always listen to client on port 1234
        self.clientSocket = socket(AF_INET, SOCK_STREAM)
        self.clientSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.clientSocket.bind(("", 12345))
        #Set safemode as init is not complete yet
        self.safeMode = True
        self.znode_data = {}  # key: slave_znode, value:ip,sock,{kr..}
        self.kr_ip = {}  # this dictionary should be returned to client. Maps key
                         # range to IP
        self.buddies = None #Array of buddy details
        self.timer = False
        self.waitingArea = []   #Slaves which have contacted master but not active
        self.slaves = []        #All slaves
        self.safeLock = Lock()
        #Start listening
        Thread(target=self.listenSlaves).start()
        Thread(target=self.listenClients).start()
        #Opens up a mimic of a python console so we can try commands and see
        #the current state of the master's variables.
        self.interpreter()

    def interpreter(self):
        """
        Give commands to evaluate current state.
        Ex: self.buddies => To print out what the current buddy server
        :return:
        """
        while True:
            cmd = input(">>> ")
            try:
                print(exec(cmd))
                print(eval(cmd))
            except Exception as e:
                print(e)


    def listenSlaves(self):
        """
        Listen to slaves and set timer (if not already set) to add the slave
        :return:
        """
        global socks
        count = 0
        socks = []
        while True:
            self.slaveSocket.listen(5)
            sock, _ = self.slaveSocket.accept()
            self.safeLock.acquire()
            # when the control is in this statement,
            # it means that a slave just came up. Send the config details
            sock.send(str(self.config).encode())
            self.onslaveup(count, sock)
            count += 1
            socks.append(sock)
            self.safeLock.release()

    def onslaveup(self, count, sock):
        """
        Sets up slave and puts watch on the znode
        :param count:
        :param sock: Socket of the slave
        :return:
        """
        data = sock.recv(2048).decode()
        ip, _ = self.zookeeper.get(data, watch=self.onslavedown)
        self.znode_data[data] = [ip, sock, set()]
        self.waitingArea.append(data)
        self.slaves.append((ip, data, sock))
        if not (self.timer):
            Timer(INTERVAL, self.safeBoot).start()
            self.timer = True

    def onslavedown(self, event):
        """
        Slave went down. Now, call for safeboot and redistribute keys
        :param event:
        :return:
        """
        print("Slave went down", event)
        z_down_node = event.path
        self.safeMode = True
        #All the alive slaves. Call safeboot on them
        alive = list(filter(lambda x: x != z_down_node, self.buddies))
        self.requestSafeBootStart(alive)
        #Get index of the z_down_node (Node that went down)
        index = self.buddies.index(z_down_node)
        length = len(self.buddies)
        #Find the buddy of the server that went down
        buddy_of_down = self.buddies[(index + 1) % length]
        #Find the server that trusted the down server to store backup
        #And hence is disappointed :)
        disappointed = self.buddies[(index - 1) % length]
        #Get the key ranges stored to redist
        own_keys = self.znode_data[buddy_of_down][KEY_RANGE]
        backup_keys = self.znode_data[z_down_node][KEY_RANGE]
        #Set the buddy's keys responsible as union of both key ranges
        total_keys = list(own_keys | backup_keys)
        message = {"operation": "setKeysResponsible",
                   "data": total_keys
                   }
        self.znode_data[buddy_of_down][KEY_RANGE] = set(total_keys)
        #Send the message to the buddy
        self.sendMessage(self.znode_data[buddy_of_down][SOCKET], str(message))
        #Update the key range details which the client requests
        for kr in self.kr_ip:
            if (self.kr_ip[kr] == self.znode_data[z_down_node][IP]):
                self.kr_ip[kr] = self.znode_data[buddy_of_down][IP]
        #Set disappointed's buddy as the buddy of the down node
        message = {'operation': 'setBuddy', 'data': self.znode_data[buddy_of_down][IP]}
        self.sendMessage(self.znode_data[disappointed][SOCKET], str(message))
        #Delete the entry
        del self.znode_data[z_down_node]
        self.buddies.pop(index)
        self.safeBootStop(sync=True)

    def sendMessage(self, sock, message):
        sock.send(message.encode())
        decode = sock.recv(2048).decode()

    def safeBoot(self):
        # Request safe boot start to all the slaves-using the sock which we already store
        # now, clear the dictionary and key ranges
        # now, start assigning keyranges and buddy servers
        self.safeLock.acquire()
        self.safeMode = True
        if (self.buddies is None or len(self.buddies) == 0):
            # For the first time
            znodes = sorted(self.waitingArea)
            l = len(znodes)
            for curr, kr_index in enumerate(self.config):
                z = znodes[curr % l]
                self.znode_data[z][KEY_RANGE].add(kr_index)
                self.kr_ip[tuple(self.config[kr_index])] = self.znode_data[z][IP]
            self.buddies = znodes
            self.requestSafeBootStart(znodes)
            self.setKeysResponsible(znodes)
            self.setAllBuddies(l, znodes)
            self.safeBootStop()
        else:
            for slave in self.waitingArea:
                self.buddies = self.addSlave(self.buddies, slave)
            #now, the key ranges are logically assigned
            for slave in self.waitingArea:
                key_ranges = self.znode_data[slave][KEY_RANGE]
                for key_range in key_ranges:
                    current_ip = self.kr_ip[tuple(self.config[key_range])]
                    current_sock = self.getSocketByIP(current_ip)
                    dest_ip = self.znode_data[slave][IP]
                    command = {'operation': 'transfer', 'data':
                        {'target_ip': dest_ip, 'key_ranges': [key_range]}}
                    self.sendMessage(current_sock,str(command))
                    self.kr_ip[key_range] = dest_ip
            #now, the data is distributed according to the logical assignment; kr_ip updated
            self.setKeysResponsible(self.znode_data.keys())
            self.setAllBuddies(len(self.buddies),self.buddies)
            self.safeBootStop(sync=True)
        self.waitingArea = []
        self.timer = False
        self.safeMode = False
        self.safeLock.release()

    def getSocketByIP(self, ip):
        """
        Utility function
        :param ip:
        :return:
        """
        return self.znode_data[list(filter(lambda x: self.znode_data[x][IP] == ip, self.znode_data))[0]][SOCKET]

    def addSlave(self, existing, new):
        """
        If a slave comes up later on. Add the slave to the system
        :param existing: Buddies array
        :param new: Znode value of the new slave that came up
        :return: the new Buddies array
        """
        most_overloaded = max(existing, key=lambda x: len(self.znode_data[x][KEY_RANGE]))
        idx = existing.index(most_overloaded)
        # Distribute keys from the most overloaded slave with the new slave.
        keys = list(self.znode_data[most_overloaded][KEY_RANGE])
        half = len(keys) // 2
        self.znode_data[new][KEY_RANGE] = set(keys[:half])
        self.znode_data[most_overloaded][KEY_RANGE] = set(keys[half:])
        existing = existing[:idx] + [new] + existing[idx:]
        return existing

    def safeBootStop(self, sync=False):
        threads = []
        for ind, i in enumerate(self.buddies):
            t = Thread(target=self.sendMessage, args=(self.znode_data[i][SOCKET],
                                                      str({
                                                          "operation": "safeBootStop",
                                                          "sync": sync
                                                      })))
            threads.append(t)
            t.start()
        for thread in threads:
            thread.join()

    def setAllBuddies(self, l, znodes):
        threads = []
        for ind, i in enumerate(self.buddies):
            t = Thread(target=self.sendMessage, args=(self.znode_data[i][SOCKET],
                                                      str({
                                                          "operation": "setBuddy",
                                                          "data": self.znode_data[znodes[(ind + 1) % l]][IP]
                                                      })))
            threads.append(t)
            t.start()
        for thread in threads:
            thread.join()

    def setKeysResponsible(self, znodes):
        # setKeysResponsible
        threads = []
        for i in znodes:
            t = Thread(target=self.sendMessage, args=(self.znode_data[i][SOCKET],
                                                      str({
                                                          "operation": "setKeysResponsible",
                                                          "data": list(self.znode_data[i][KEY_RANGE])
                                                      })))
            threads.append(t)
            t.start()
        for thread in threads:
            thread.join()

    def requestSafeBootStart(self, znodes):
        # requestSafeBootStart
        threads = []
        for i in znodes:
            t = Thread(target=self.sendMessage,
                       args=(self.znode_data[i][SOCKET], "{'operation':'requestSafeBootStart'}"))
            threads.append(t)
            t.start()
        for thread in threads:
            thread.join()
        print("Safeboot started!!")

    def listenClients(self):
        while True:
            self.clientSocket.listen(5)
            conn_sock, addr = self.clientSocket.accept()
            Thread(target=self.serveClient, args=(conn_sock,)).start()
        pass

    def serveClient(self, conn_soc):
        if self.safeMode:
            # throw error
            conn_soc.send(json.dumps({'status': 1, 'message': 'safeboot error', 'data': None}).encode())
            return
        conn_soc.send(str(self.kr_ip).encode())
        return
