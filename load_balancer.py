# coding: utf-8

import socket
import selectors
import select
import signal
import logging
import argparse

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

    def select_server(self):
        pass
    
    def update(self, *arg):
        pass


# least connections policy
class LeastConnections:
    def __init__(self, servers):
        self.servers = servers

    def select_server(self):
        pass

    def update(self, *arg):
        pass


# least response time
class LeastResponseTime:
    def __init__(self, servers):
        self.servers = servers

    def select_server(self):
        pass

    def update(self, *arg):
        pass


class SocketMapper:
    def __init__(self, policy):
        self.policy = policy
        self.map = {}

    def add(self, client_sock, upstream_server):
        client_sock.setblocking(False)
        sel.register(client_sock, selectors.EVENT_READ, read)
        upstream_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        upstream_sock.connect(upstream_server)
        upstream_sock.setblocking(False)
        logger.debug("Proxying to %s %s", *upstream_server)
        self.map[client_sock] =  upstream_sock

    def delete(self, sock):
        try:
            self.map.pop(sock)
            sel.unregister(sock)
            sock.close() 
        except KeyError:
            pass

    def get_sock(self, sock):
        for c, u in self.map.items():
            if u == sock:
                return c
            if c == sock:
                return u
        return None
    
    def get_upstream_sock(self, sock):
        for c, u in self.map.items():
            if c == sock:
                return u
        return None

    def get_all_socks(self):
        """ Flatten all sockets into a list"""
        return list(sum(self.map.items(), ())) 

def accept(sock, mask):
    client, addr = sock.accept()
    logger.debug("Accepted connection %s %s", *addr)
    mapper.add(client, policy.select_server())

def read(conn,mask):
    data = conn.recv(4096)
    if len(data) == 0: # No messages in socket, we can close down the socket
        mapper.delete(conn)
    else:
        mapper.get_sock(conn).send(data)


def main(addr, servers):
    global policy
    global mapper


def main(addr, servers):
    # register handler for interruption 
    # it stops the infinite loop gracefully
    signal.signal(signal.SIGINT, graceful_shutdown)

    policy = N2One(servers)
    mapper = SocketMapper(policy)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        sock.setblocking(False)
        sock.bind(addr)
        sock.listen()
        logger.debug("Listening on %s %s", *addr)
        while not done:
            readable, writable, exceptional = select.select([sock]+mapper.get_all_socks(), [], [], 1)
            if readable is not None:
                for s in readable:
                    if s == sock:
                        client, addr = sock.accept()
                        logger.debug("Accepted connection %s %s", *addr)
                        client.setblocking(False)
                        mapper.add(client, policy.select_server())
                    if mapper.get_sock(s):
                        data = s.recv(4096)
                        if len(data) == 0: # No messages in socket, we can close down the socket
                            mapper.delete(s)
                        else:
                            mapper.get_sock(s).send(data)
    except Exception as err:
        logger.error(err)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Pi HTTP server')
    parser.add_argument('-p', dest='port', type=int, help='load balancer port', default=8080)
    parser.add_argument('-s', dest='servers', nargs='+', type=int, help='list of servers ports')
    args = parser.parse_args()
    
    servers = []
    for p in args.servers:
        servers.append(('localhost', p))
    
    main(('127.0.0.1', args.port), servers)
