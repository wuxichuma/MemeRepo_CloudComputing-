#-*- coding: UTF-8 -*-
import socket
try:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # An den Port binden; IP nicht notwending
    s.bind( ("localhost", 9999) )
    pictures=["base64xxxx1","base64xxxx2","base64xxxx3","base64xxxx4","base64xxxx5"]
    s.listen(1)
    for i in range(5):
        conn, addr = s.accept()
        conn.send(bytes(pictures[i],encoding = "utf8"))
        conn.close()
finally:
    #conn.close()
    s.close()
~             