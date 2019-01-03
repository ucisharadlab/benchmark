from socket import socket

ip = '127.0.0.1'
port1 = 19010
filePath = '../adm/observation.adm'

sock1 = socket()
sock1.connect((ip, port1))

with open(filePath) as inputData:
    for line in inputData:
        sock1.sendall(line)
    sock1.close()