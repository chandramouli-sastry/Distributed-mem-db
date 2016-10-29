import socket
import threading

ips = ['192.168.0.55','192.168.0.57']
port = 1234


#threading.Thread(target=recvr, args=(conn_sock,)).start()
while (True):
    try:
        ip = ips[int(input("Enter ind"))]
        s = input("Enter cmd")
        conn_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn_sock.connect((ip, port))
        conn_sock.send(s.encode())
        data = conn_sock.recv(2048).decode()
        print(data)
    except Exception as e:
        print(e)