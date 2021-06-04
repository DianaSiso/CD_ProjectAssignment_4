# coding: utf-8

import socket
import selectors
import signal
import logging
import argparse
import time
from statistics import mean

# configure logger output format
logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('Load Balancer')


# used to stop the infinity loop
done = False

sel = selectors.DefaultSelector()

policy = None
mapper = None

# implements a graceful shutdown
def graceful_shutdown(signalNumber, frame):  
    logger.debug('Graceful Shutdown...')
    global done
    done = True


# n to 1 policy
class N2One:
    def __init__(self, servers):
        self.servers = servers  

    def select_server(self):
        return self.servers[0]

    def update(self, *arg):
        pass


# round robin policy
class RoundRobin:
    def __init__(self, servers):
        self.servers = servers
        self.idx=-1
    def select_server(self):
        self.idx=self.idx+1
        if self.idx>=len(self.servers):
            self.idx=0
        return self.servers[self.idx]
    
    def update(self, *arg):
        pass


class LeastConnections:
    def __init__(self, servers):
        self.servers = servers
        self.connections=[]
        for i in range(0,len(self.servers),1):
            self.connections.append(0)
    def select_server(self):
        min=1000
        idx=0
        for i in range(0,len(self.servers),1):
            if(self.connections[i]<min):
                min=self.connections[i]
                idx=i
        
        self.connections[idx]=self.connections[idx]+1
        print(self.connections)
        print("indice a por:" + str(idx))
        return self.servers[idx]
        #array num conexoes
        #ver quem tem menos, e atruibuis esse
        #{2 , 1 , 2}
        pass

    def update(self, *arg):
        idx=self.servers.index(arg[0])
        print("indice a tirar:" + str(idx))
        if(self.connections[idx]!=0):
            self.connections[idx]=self.connections[idx]-1
        print(self.connections)
        #recevemos o server que vai perder uma conexao
        #o 1º servidor deixou de ter conn
        #{1,1,2}
        pass



# least response time
class LeastResponseTime:
    def __init__(self, servers):
        self.servers = servers
        self.ultimotempo={}
        self.tempo={}
        self.media={}
        for i in range(0,len(self.servers),1):
            self.media[self.servers[i]]=0
        
    def select_server(self):
        temp=min(self.media, key=self.media.get)
        idx=self.servers.index(temp)
        start=time.time()
        self.ultimotempo[self.servers[idx]]=start
        return self.servers[idx]
        pass

    def update(self, *arg):
        end=time.time()
        temp=end - self.ultimotempo[arg[0]]
        if(arg[0] in self.tempo):
            self.tempo[arg[0]].append(temp)
        else:
            self.tempo[arg[0]]=[temp]
        self.media[arg[0]]=mean(self.tempo[arg[0]])
        pass


POLICIES = {
    "N2One": N2One,
    "RoundRobin": RoundRobin,
    "LeastConnections": LeastConnections,
    "LeastResponseTime": LeastResponseTime
}

class SocketMapper:
    def __init__(self, policy):
        self.policy = policy
        self.map = {}
        self.servers = {}

    def add(self, client_sock, upstream_server):
        client_sock.setblocking(False)
        self.userv=upstream_server
        sel.register(client_sock, selectors.EVENT_READ, read)
        upstream_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        upstream_sock.connect(upstream_server)
        upstream_sock.setblocking(False)
        sel.register(upstream_sock, selectors.EVENT_READ, read)
        logger.debug("Proxying to %s %s", *upstream_server)
        self.map[client_sock] =  upstream_sock
        self.servers[client_sock]=upstream_server

    def delete(self, sock):
        sel.unregister(sock)
        if sock in self.servers:
            policy.update(self.servers[sock])
        sock.close()
        if sock in self.map:
            self.map.pop(sock)

    def get_sock(self, sock):
        for client, upstream in self.map.items():
            if upstream == sock:
                return client
            if client == sock:
                return upstream
        return None
    
    def get_upstream_sock(self, sock):
        return self.map.get(sock)

    def get_all_socks(self):
        """ Flatten all sockets into a list"""
        return list(sum(self.map.items(), ())) 

def accept(sock, mask):
    client, addr = sock.accept()
    logger.debug("Accepted connection %s %s", *addr)
    #print("add"+str(sock))
    mapper.add(client, policy.select_server())

def read(conn,mask):
    data = conn.recv(4096)
    if len(data) == 0: # No messages in socket, we can close down the socket
        #print("remove"+str(conn))
        mapper.delete(conn)
        
    else:
        mapper.get_sock(conn).send(data)


def main(addr, servers, policy_class):
    global policy
    global mapper

    # register handler for interruption 
    # it stops the infinite loop gracefully
    signal.signal(signal.SIGINT, graceful_shutdown)

    policy = policy_class(servers)
    mapper = SocketMapper(policy)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(addr)
    sock.listen()
    sock.setblocking(False)

    sel.register(sock, selectors.EVENT_READ, accept)

    try:
        logger.debug("Listening on %s %s", *addr)
        while not done:
            events = sel.select(timeout=1)
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)
                
    except Exception as err:
        logger.error(err)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Pi HTTP server')
    parser.add_argument('-a', dest='policy', choices=POLICIES)
    parser.add_argument('-p', dest='port', type=int, help='load balancer port', default=8080)
    parser.add_argument('-s', dest='servers', nargs='+', type=int, help='list of servers ports')
    args = parser.parse_args()
    
    servers = [('localhost', p) for p in args.servers]
    
    main(('127.0.0.1', args.port), servers, POLICIES[args.policy])
