from socket import *

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError
from Master import Master
from Slave import Slave

ZK_PORT = "2181"

ZK_ADDRESS = "192.168.43.169"


class ServerFactory:
    def __init__(self, zk_address, zk_port):
        """
        Initializes the server
        :param zk_address:  Address of the zookeeper server
        :param zk_port:     Port of the zookeeper server
        :return:
        """
        connection_socket = socket(AF_INET, SOCK_STREAM)
        connection_socket.bind(('', 0))
        self.socket = connection_socket
        self.ip = self.get_own_ip(zk_address, zk_port)
        self.port = connection_socket.getsockname()[1]
        self.zookeeper = KazooClient(hosts=zk_address + ":" + zk_port, read_only=True)
        self.zookeeper.start()
        self.master = self.get_master()


    def get_own_ip(self, zk_address, zk_port):
        s = socket(AF_INET, SOCK_STREAM)
        s.connect((zk_address, int(zk_port)))
        ip = s.getsockname()[0]
        s.close()
        return ip

    def get_server(self):
        if (self.master[0] == self.ip and self.master[1]==self.port):
            return Master(self.zookeeper, self.ip, self.socket)
        else:
            self.socket.close()
            return Slave(self.zookeeper, self.master, self.ip)

    def get_master(self):
        """
        :return: [master's ip , master's port]
        """

        try:
            #Try to become master. If you cannot, then become slave
            val=self.ip + ":" + str(self.port)
            self.zookeeper.create("/master", value=val.encode(),ephemeral=True)
            return [self.ip, self.port]
        except NodeExistsError:
            data, stat = self.zookeeper.get("/master")
            return data.decode().split(":")



server = ServerFactory(ZK_ADDRESS, ZK_PORT).get_server()
