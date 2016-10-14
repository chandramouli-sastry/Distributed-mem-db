from socket import *

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError

"""
    Class
"""


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
        # create internal and external?
        self.ip = self.get_own_ip(zk_address, zk_port)
        self.port = connection_socket.getsockname()[1]
        self.zookeeper = KazooClient(hosts=zk_address + ":" + zk_port, read_only=True)
        self.zookeeper.start()
        self.connection_socket = connection_socket
        self.master = self.get_master()

    def get_own_ip(self, zk_address, zk_port):
        s = socket(AF_INET, SOCK_STREAM)
        s.connect((zk_address, int(zk_port)))
        ip = s.getsockname()[0]
        s.close()
        return ip

    def get_server(self):
        if (self.master[0] == self.ip):
            return Master(self.zookeeper, self.ip)
        else:
            return Slave(self.zookeeper, self.master, self.ip)

    def get_master(self):
        """
        :return: [master's ip , master's port]
        """
        try:
            self.zookeeper.create("/master", value=self.ip + ":" + self.port)
            return [self.ip, self.port]
        except NodeExistsError:
            data, stat = self.zookeeper.get("/master")
            return data.decode("utf-8").split(":")

    def perform_master_job(self):
        pass

    def perform_slave_job(self):
        pass


server = ServerFactory("127.0.0.1", "2181").get_server()
server.start()
