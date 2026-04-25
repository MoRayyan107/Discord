import socket

c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
c.connect(("10.12.92.18", 8888))

print("connected to Java Server.....")

while True:
    name = input('Enter your name: ')
    if name.lower() == 'exit':
        c.send(bytes(name +'\n', 'utf-8'))
        break

    c.send(bytes(name + '\n', 'utf-8'))

    resp = c.recv(1024).decode()
    print("Server:", resp.strip())

c.close()
