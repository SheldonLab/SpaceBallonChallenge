import socket
import multiprocessing
from httpServer import httpServer


ip = socket.gethostbyname(socket.getfqdn())
thr = multiprocessing.Process(target=httpServer, args=(ip, 8001))
thr.start()