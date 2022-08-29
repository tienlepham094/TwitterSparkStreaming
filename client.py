import socket
# Create a client socket

clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
clientSocket.connect(("127.0.0.1", 5553));
dataFromServer = clientSocket.recv(1024);
print(dataFromServer.decode('utf-8'));