import socket
import threading

ip = '192.168.199.1'
port = 6969
conn_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
conn_sock.connect((ip, port))


def recvr(conn_sock):
    while (True):
        data = conn_sock.recv(2048).decode("utf-8")
        print(data)


threading.Thread(target=recvr, args=(conn_sock,)).start()
while (True):
    s = input()
    conn_sock.send(s.encode("utf-8"))
