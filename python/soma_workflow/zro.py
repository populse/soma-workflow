'''
@author: Manuel Boissenin, Yann Cointepas, Denis Riviere

@organization: NAO, UNATI, Neurospin, Gif-sur-Yvette, France

'''
from __future__ import print_function

try:
    import cPickle as pickle
except:
    import pickle
import traceback
import zmq
import logging

#For some reason the zmq bind_to_random_port did not work with
#one of the version of zmq that we are using. Therfore we have
#to use the followin function:

import socket
from contextlib import closing

DEBUG=True #we print in file /tmp/zro

def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        return s.getsockname()[1]

class ObjectServer:
    '''
    Usage:
    -create an ObjectServer providing a port.
    -register the object you want to access from another program that might be on
    a distant object.
    -lauch the server loop.
    '''
    def __init__(self, port=None):
        self.objects = {}
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        if not port:
            port = find_free_port()
            # try:
            #     #Here there is a bug probably linked with the zmq version
            #     port = self.socket.bind_to_random_port("tcp://*:",
            #                                            min_port=1025,
            #                                            max_port=65536,
            #                                            max_tries=1200)
            # except Exception as e:
            #     logging.debug("Maximum number of attempt to find a port reached?: " + str(e))
        #else:
        self.socket.bind("tcp://*:" + str(port))
        self.port = port

    def register(self, object):
        """The full socket adress should be provided
        what if we have multiple object of one given class
        (the identifier of the object could be used and the uri should be changed)
        """
        if object.__class__.__name__ not in self.objects:
            self.objects[object.__class__.__name__] = {}
        self.objects[object.__class__.__name__][str(id(object))] = object

        return str(object.__class__.__name__) + ":" + str(id(object)) + ":" + str(self.port)

    def serve_forever(self):
        while True:
            #  Wait for next request from client
            #print("Waiting for incoming data", file=open('/tmp/WTF','a'))
            message = self.socket.recv()
            try:
                classname, object_id, method, args, kwargs = pickle.loads(message)
                #TODO
                #print(classname, object_id, method, args, file=open('/tmp/WTF','a'))
                try:
                    if self.objects[classname][object_id]:
                        result = getattr(self.objects[classname][object_id], method)(*args, **kwargs)
                    else:
                        pass #TODO
                        #logging.debug("object not in the list of objects")
                except Exception as e:
                    result = e
                self.socket.send(pickle.dumps(result))
            except:
                print("An exception ocurred in the server of the remote object")
                traceback.print_last()

class Proxy(object):
    """
    The Proxy object is created with the uri of the object
    afterwards you can call any method you want on it,
    to access variable attributes you will have to create properties (accessors)
    """
    def __init__(self, uri):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        #Deux cas: ou uri est un bytes object ou il est du type str
        if type(uri) == type(b'bytes type'):
            (classname, object_id, self._port) = uri.split(b':')
            # caveat: note that connect will succeed even if there is
            # no port waiting for a connection, as such you have to
            # check yourself that the connection has succeeded
            # (cf how the database server engine is handled
            self.classname = classname.decode('utf-8')
            self.object_id = object_id.decode('utf-8')
            self.socket.connect("tcp://localhost:" + self._port.decode('utf-8'))
        elif type(uri) == type("str type"):
            (self.classname, self.object_id, self._port) = uri.split(':')
            # caveat: note that connect will succeed even if there is
            # no port waiting for a connection, as such you have to
            # check yourself that the connection has succeeded
            # (cf how the database server engine is handled
            self.socket.connect("tcp://localhost:" + self._port)
        elif type(uri) == type(u"unicode type"):
            (self.classname, self.object_id, self._port) = uri.split(':')
            # caveat: note that connect will succeed even if there is
            # no port waiting for a connection, as such you have to
            # check yourself that the connection has succeeded
            # (cf how the database server engine is handled
            self.socket.connect("tcp://localhost:" + self._port)
        else:
            print("Issue in zro: the uri is not taken into account, "
                  "this is probably due to its type")
        if DEBUG:
            print("Proxy: ", str(self.classname), str(self.object_id), str(self._port), file=open('/tmp/zro','a'))
        # TODO
        # logging.debug(self.classname, self.object_id, self._port)

    def __getattr__(self, method_name):
        if DEBUG:
            print("On class: ", self.classname, file=open('/tmp/zro','a'))
            print("method called: ", method_name, file=open('/tmp/zro','a'))
        return ProxyMethod(self, method_name)

class ProxyMethod(object):
    def __init__(self, proxy, method):
        self.proxy = proxy
        self.method = method

    def __call__(self, *args, **kwargs):
        try:
            self.proxy.socket.send(pickle.dumps([self.proxy.classname, self.proxy.object_id, self.method, args, kwargs]))
        except Exception as e:
            print(e)
        result = pickle.loads(self.proxy.socket.recv())
        if DEBUG:
            print(result, file=open('/tmp/'+ 'zro','a'))
        if isinstance(result, Exception):
            raise result
        return result