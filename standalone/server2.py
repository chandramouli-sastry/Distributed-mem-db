import socket
import threading

server_port = 6969
server_sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server_sock.bind(('', server_port))
people = []
d = {}


def handle_person(person):
    while (True):
        global d
        message = person[0].recv(2048).decode("utf-8")
        parts = message.split(":")
        if (parts[0] == "GET"):
            val = d.get(parts[1], "KeyError : Invalid Key")
            person[0].send(str(val).encode("utf-8"))
        else:
            key1 = parts[1]
            val = parts[2]
            d[key1] = val
            person[0].send(str("SUCCESS").encode("utf-8"))


i = 0

while (True):
    server_sock.listen(5)
    conn_sock, addr = server_sock.accept()
    person = (conn_sock, addr, i, socket.gethostbyaddr(addr[0]))
    people.append(person)
    i += 1
    t = threading.Thread(target=handle_person, args=(person,))
    t.start()
