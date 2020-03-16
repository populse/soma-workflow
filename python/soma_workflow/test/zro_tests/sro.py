# -*- coding: utf-8 -*-
'''
@authors: Manuel Boissenin, Yann Cointepas, Denis Riviere

@organization: NAO, UNATI, Neurospin, Gif-sur-Yvette, France

'''

from __future__ import print_function

from __future__ import absolute_import
try:
    import six.moves.cPickle as pickle
except ImportError:
    import pickle
import traceback
# import zmq
# import re
import logging
import threading

# For some reason the zmq bind_to_random_port did not work with
# one of the version of zmq that we are using. Therfore we have
# to use the followin function:

import socket
from contextlib import closing


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        return s.getsockname()[1]


class Respond(threading.Thread):

    def __init__(self, a_socket, obj_server):
        threading.Thread.__init__(self)  # TODO TBC sanity check
        self.client = a_socket
        self.obj_server = obj_server

    def run(self):
        while True:
            try:
                message = self.client.recv(12000)
            except Exception:
                print("Exception while waiting to receive a message")
            # print("msg received")
            # message = self.socket.recv()
            if message:
                try:
                    classname, object_id, method, args, kwargs = pickle.loads(
                        message)
                    # TODO
                    # logging.debug(classname, object_id, method, args)
                    try:
                        if self.obj_server.objects[classname][object_id]:
                            # print("accessing the object")
                            result = getattr(self.obj_server.objects[
                                             classname][object_id], method)(*args, **kwargs)
                        else:
                            # print("object not found")
                            pass  # TODO
                            # logging.debug("object not in the list of
                            # objects")
                    except Exception as e:
                        # print("an exception occurred")
                        result = e
                    # print("sending result")
                    self.client.send(pickle.dumps(result))
                except Exception as e:
                    print(
                        "An exception ocurred in the server of the remote object")
                    print(e)
                    traceback.print_last()  # TODO what is this?


class ObjectServer(object):

    '''
    Usage:
    -create an ObjectServer providing a port.
    -register the object you want to access from another program that might be on
    a distant object.
    -lauch the server loop.
    '''

    def __init__(self, port=None):
        self.objects = {}
        # self.context = zmq.Context()
        # self.socket = self.context.socket(zmq.REP)

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        if not port:
            port = find_free_port()
            # try:
            # Here there is a bug probably linked with the zmq version
            #     port = self.socket.bind_to_random_port("tcp://*:",
            #                                            min_port=1025,
            #                                            max_port=65536,
            #                                            max_tries=1200)
            # except Exception as e:
            #     logging.debug("Maximum number of attempt to find a port reached?: " + str(e))
        # else:
        # self.socket.bind("tcp://*:" + str(port))
        self.socket.bind(('', port))
        self.socket.listen(12)
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
            client, address = self.socket.accept()  # multiple client possible
            # print("Accepting a new connection")
            receiving_thread = Respond(client, self)
            receiving_thread.start()

            #  Wait for next request from client


class Proxy(object):

    """
    The Proxy object is created with the uri of the object
    afterwards you can call any method you want on it,
    to access variable attributes you will have to create properties (accessors)
    """

    def __init__(self, uri):
        # self.context = zmq.Context()
        # self.socket = self.context.socket(zmq.REQ)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        (self.classname, self.object_id, self._port) = uri.split(":")
        # self.socket.connect("tcp://localhost:" + self._port)
        self.socket.connect(("localhost", int(self._port)))
        # TODO
        # logging.debug(self.classname, self.object_id, self._port)

    def __getattr__(self, method_name):
        return ProxyMethod(self, method_name)


class ProxyMethod(object):

    def __init__(self, proxy, method):
        self.proxy = proxy
        self.method = method

    def __call__(self, *args, **kwargs):
        self.proxy.socket.send(
            pickle.dumps([self.proxy.classname, self.proxy.object_id, self.method, args, kwargs]))
        result = pickle.loads(self.proxy.socket.recv(12000))
        if isinstance(result, Exception):
            raise result
        return result
