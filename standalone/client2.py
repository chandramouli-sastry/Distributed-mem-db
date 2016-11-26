import socket
import threading

master_ip = '192.168.43.169'
master_port = 12345
slave_port = 1234
conn_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
conn_sock.connect((master_ip, master_port))
decode = conn_sock.recv(2048).decode()
print(decode)
kr_ips = eval(decode)
conn_sock.close()
#threading.Thread(target=recvr, args=(conn_sock,)).start()
def get_ip(key):
    for kr in kr_ips:
        if kr[0]<=key<=kr[1]:
            return kr_ips[kr]
    return None

while (True):
    try:
        key = input("Enter key")
        slave_ip = get_ip(int(key))
        s = (input("Enter get/put") == "get") or input("Enter value")
        conn_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn_sock.connect((slave_ip, slave_port))
        if s== True:
            m = {"method":"GET", "args":[int(key)]}
            conn_sock.send(str(m).encode())
        else:
            m = {"method":"PUT", "args":[int(key),int(s)]}
            conn_sock.send(str(m).encode())
        data = conn_sock.recv(2048).decode()
        print(data)
    except Exception as e:
        print(e)