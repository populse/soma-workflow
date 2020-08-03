# -*- coding: utf-8 -*-
'''
@author: Manuel Boissenin, Yann Cointepas, Denis Riviere

@organization: NAO, UNATI, Neurospin, Gif-sur-Yvette, France

'''
from __future__ import print_function
from __future__ import absolute_import

try:
    import six.moves.cPickle as pickle
except ImportError:
    import pickle
import traceback
import zmq
import logging
import threading
import sys
import weakref
import time

# For some reason the zmq bind_to_random_port did not work with
# one of the version of zmq that we are using. Therfore we have
# to use the followin function:

import socket
from contextlib import closing


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        return s.getsockname()[1]


class ReturnException(object):

    ''' Fake exception, wraps an exception occuring during server-side
        execution.

        ReturnException cannot be an Exception subclass because it causes
        problems in pickling (in unpickling, actually). In the same idea,
        the exception traceback cannot be a "real" traceback because a
        traceback cannot be pickled. We use a string representation of it
        instead (using traceback.format_exc()).
    '''

    def __init__(self, e, exc_info):
        self.exc = e
        self.exc_info = exc_info  # a tuple (exc_type, exc_value, traceback)


class WorkerThread(threading.Thread):
    """
    Thread which pops method calls requests from ObjectServer, and executes
    them in the correct thread. Each client request is done from an arbitrary
    thread, and the request is marked with that thread ID. Here on server side
    we duplicate client threads and execute calls in the duplicated threads, in
    order to reproduce client threads behavior, and avoid deadlocks.
    """

    def __init__(self, worker_id):
        super(WorkerThread, self).__init__()
        self.worker_id = worker_id
        self.lock = threading.RLock()
        self.todo = []
        self.replies = []
        self.stop_request = False
        self._hold_running = True  # keep alive until we submit a job
        self.event = threading.Event()

    def running(self):
        with self.lock:
            return not self.stop_request or self._hold_running

    def hold_running(self):
        with self.lock:
            if self.stop_request:
                return False
            self._hold_running = True
            return True

    def add_job(self, job):
        with self.lock:
            self.todo.append(job)
            self._hold_running = False
            # awaken the thread
            self.event.set()

    def run(self):
        logger = logging.getLogger('zro.ObjectServer')
        logger.debug('worker thread starting: ' + self.worker_id + ' : '
                     + str(self))
        iddle_t0 = None
        iddle_t = None
        iddle_timeout = 30.
        while self.running():
            job = None
            with self.lock:
                if len(self.todo) != 0:
                    job = self.todo.pop(0)
                    logger.debug('pop job: ' + repr(job))
            if not job:
                if iddle_t0 is None:
                    iddle_t0 = time.time()
                iddle_t = time.time()
                if (iddle_t - iddle_t0 >= iddle_timeout) \
                        and not self._hold_running:
                    # no more jobs for a "long time": stop the thread
                    logger.debug('stopping iddle worker: ' + self.worker_id)
                    self.stop_request = True
                    break
                self.event.wait(0.05)
            else:
                self.event.clear()
                iddle_t0 = None
                socket, client, instance, method, args, kwargs, call_time \
                    = job
                pop_time = time.time()
                try:
                    method_code = getattr(instance, method)
                    logger.debug('calling: ' + repr(method_code) + 'after %f s' % (pop_time - call_time))
                    result = method_code(*args, **kwargs)
                except Exception as e:
                    logger.exception(e)
                    etype, evalue, etb = sys.exc_info()
                    if hasattr(e, 'server_traceback'):
                        logger.error('server-side traceback:\n'
                                     + e.server_traceback)
                        evalue.server_traceback = traceback.format_exc() \
                            + '\nremote server traceback:\n' \
                            + e.server_traceback
                    else:
                        evalue.server_traceback = traceback.format_exc()
                    result = ReturnException(e, (etype, evalue,
                                                 traceback.format_exc()))

                res_time = time.time()
                logger.debug('result (%f s): ' % (res_time - pop_time) + repr(result))
                with self.lock:
                    self.replies.append((socket, client, result,
                                         res_time - call_time))
                    logger.debug('replies: ' + str(len(self.replies)) + ', todo: ' + str(len(self.todo)))

        logger.debug('stopping worker: ' + self.worker_id)


class ObjectServer(object):

    '''
    Usage:
    -create an ObjectServer providing a port.
    -register the object you want to access from another
     program that might be on a distant object.
    -lauch the server loop.

    The loop expects events from client(s). When a message is received
    '''

    def __init__(self, port=None):
        self.objects = {}
        self.context = zmq.Context()
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
        self.port = port
        logger = logging.getLogger('zro.ObjectServer')
        logger.debug("Initialising object server on port: " + repr(self.port))
        self.workers = {}
        self.lock = threading.RLock()
        self.stop_request = False

    def must_stop(self):
        with self.lock:
            return self.stop_request

    def register(self, object):
        """The full socket adress should be provided
        what if we have multiple object of one given class
        (the identifier of the object could be used and the uri should be changed)
        """
        if object.__class__.__name__ not in self.objects:
            self.objects[object.__class__.__name__] = {}
        self.objects[object.__class__.__name__][str(id(object))] = object

        logger = logging.getLogger('zro.ObjectServer')
        logger.debug("The oject server is registering a "
                     + repr(object.__class__.__name__)
                     + " object, on " + repr(self.port))

        return str(object.__class__.__name__) + ":" + str(id(object)) + ":" + str(self.port)

    def serve_forever(self):
        logger = logging.getLogger('zro.ObjectServer')
        interval = 100
        frontend = self.context.socket(zmq.ROUTER)
        frontend.bind("tcp://*:" + str(self.port))
        poller = zmq.Poller()
        poller.register(frontend, zmq.POLLIN)
        logger.debug("ObS0:" + str(self.port)[-3:]
                     + ":Waiting for incoming data")
        init_time = time.time()
        nreq = 0
        # logger.setLevel(logging.DEBUG)
        while not self.must_stop():
            try:
                #  Wait for next request from client
                #logger.debug("ObS0:" + str(self.port)[-3:]
                            #+ ":Waiting for incoming data")
                socks = dict(poller.poll(interval))
                event = None
                for zsocket in socks:
                    event = True
                    message = zsocket.recv_multipart()
                    client = message[0]
                    classname, object_id, method, thread_id, args, kwargs \
                        = pickle.loads(message[2])
                    instance = self.objects.get(classname, {}).get(object_id)
                    if instance:
                        logger.debug(
                            "ObS1:" + str(self.port)[-3:] + ":calling "
                            + classname + " " + object_id + ", thread: " + thread_id + ", client: " + repr(client) + ": " + method
                            + " " + repr(args))
                        worker = self.get_worker(thread_id)
                        with worker.lock:
                            worker.add_job(
                                (zsocket, client,
                                self.objects[classname][object_id],
                                method, args, kwargs, time.time()))
                    else:
                        logger.info(
                            "object not in the list of objects: %s / %s"
                            % (classname, object_id))
                        #pass  # TODO

                # poll answers
                ended_workers = {}
                for worker_id, worker in self.workers.items():
                    replies = []
                    with worker.lock:
                        if not worker.is_alive() or not worker.running():
                            ended_workers[worker_id] = worker
                        if worker.replies:
                            replies = list(worker.replies)
                            worker.replies = []  # vacuum replies list
                    for reply in replies:
                        zsocket, client, result, exc_time = reply
                        t = time.time()
                        nreq += 1
                        logger.debug("ObS2:" + str(self.port)[-3:]
                                     + " (%f s - %d req in %f: %f req/s): %s result is: "
                                     % (exc_time, nreq, t - init_time,
                                        nreq / (t - init_time), repr(client))
                                + repr(result))
                        zsocket.send_multipart((client, '',
                                               pickle.dumps(result)))

                for worker_id, worker in ended_workers.items():
                    worker.join()
                    del self.workers[worker_id]

            except Exception as e:
                logger.exception(e)
                # print("An exception occurred in the server of the remote object")
                # traceback.print_exc()

    def get_worker(self, worker_id):
        worker = self.workers.get(worker_id)
        if worker:
            with worker.lock:
                if worker.running():
                    worker.hold_running()
                    return worker
            del self.workers[worker_id]
        worker = WorkerThread(worker_id)
        self.workers[worker_id] = worker
        worker.start()
        return worker

    def stop(self):
        with self.lock:
            self.stop_request = True
        for cls, obj in self.objects.items():
            for instance in obj.values():
                if hasattr(instance, 'interrupt_after'):
                    # object is itself a proxy to another server (database)
                    instance.interrupt_after(0)
        for worker in self.workers.values():
            if worker.running():
                with worker.lock:
                    worker.stop_request = True
                worker.join()
        self.workers = {}


class Proxy(object):

    """
    The Proxy object is created with the uri of the object
    afterwards you can call any method you want on it,
    to access variable attributes you will have to create properties (accessors)
    """

    def __init__(self, uri):
        self.context = zmq.Context()
        # To avoid multiple threads using the proxy variables at the same time
        self.lock = threading.RLock()  # FIXME remove this
        # Deux cas: ou uri est un bytes object ou il est du type str
        if type(uri) == type(b'bytes type'):
            uri = uri.decode('utf-8')

        (self.classname, self.object_id, self._port) = uri.split(':')
        # caveat: note that connect will succeed even if there is
        # no port waiting for a connection, as such you have to
        # check yourself that the connection has succeeded
        # (cf how the database server engine is handled
        #self.socket.connect("tcp://localhost:" + self._port)
        logger = logging.getLogger('zro.ObjectServer')
        logger.debug("Proxy: " + str(self.classname) + str(self.object_id)
                     + str(self._port))
        # TODO
        # logging.debug(self.classname, self.object_id, self._port)
        self.timeout = -1
        self.running_methods = set()

    def __del__(self):
        # kill all running methods because the proxy / connection is destroyed.
        print('del Proxy', self.classname, self)
        def get_running(self):
            with self.lock:
                if self.running_methods:
                    return next(iter(self.running_methods))
                return None
        while True:
            method = get_running(self)
            if method is None:
                break
            print('    interrupt method:', method.method)
            method.interrupt()

    def __getattr__(self, method_name):
        if method_name in self.__dict__:
            return self.__dict__[method_name]
        logger = logging.getLogger('zro.ObjectServer')
        logger.debug("On class:               " + self.classname)
        logger.debug("method called:          " + method_name)
        with self.lock:
            method = ProxyMethod(self, method_name)
            self.running_methods.add(method)
        return method

    def interrupt_after(self, timeout):
        with self.lock:
            self.timeout = timeout


class ProxyMethod(object):

    def __init__(self, proxy, method):
        self.proxy = weakref.proxy(proxy)
        self.method = method
        self.stop_request = False

    def new_socket(self):
        socket = self.proxy.context.socket(zmq.REQ)
        # caveat: note that connect will succeed even if there is
        # no port waiting for a connection, as such you have to
        # check yourself that the connection has succeeded
        # (cf how the database server engine is handled
        socket.connect("tcp://localhost:" + self.proxy._port)
        return socket

    def __call__(self, *args, **kwargs):
        try:
            timeout = 2000  # ms
            logger = logging.getLogger('zro.ObjectServer')
            logger.debug('Execute method: %s.%s(*%s, **%s)'
                         % (self.proxy.classname, self.method, repr(args),
                            repr(kwargs)))
            socket = self.new_socket()
            logger.debug('socket: %s, thread: %s' % (repr(socket), threading.current_thread().name))
            try:
                if socket.closed:
                    logger.info('### SOCKET CLOSED. ###')
                    done = True
                    raise RuntimeError(
                        'Disconnected in ProxyMethod.__call__ for: %s.%s(*%s, **%s)'
                        % (self.proxy.classname, self.method, repr(args),
                          repr(kwargs)))
                thread_id = threading.current_thread().name
                logger.debug('send: %s' % repr([self.proxy.classname, self.proxy.object_id, self.method, thread_id, args, kwargs]))
                socket.send(
                    pickle.dumps([self.proxy.classname, self.proxy.object_id, self.method, thread_id, args, kwargs]))
                logger.debug('sent.')
            except Exception as e:
                logger.exception(e)
                print("Exception occurred while calling a remote object: %s.%s(*%s, **%s)"
                      % (self.proxy.classname, self.method, repr(args),
                      repr(kwargs)))
                print(e)
            done = False
            t0 = time.time()
            while not done:
                if socket.closed:
                    print('### SOCKET CLOSED. ###', file=sys.stderr)
                    logger.info('### SOCKET CLOSED. ###')
                    done = True
                    raise RuntimeError(
                        'Disconnected in ProxyMethod.__call__ for: %s.%s(*%s, **%s)'
                        % (self.proxy.classname, self.method, repr(args),
                          repr(kwargs)))
                logger.debug(self.proxy.classname + ' ' + self.method
                             + ' polling... timeout: % f' % self.proxy.timeout)
                t1 = time.time()
                poll_res = socket.poll(timeout, zmq.POLLIN)
                if poll_res:
                    done = True
                    logger.debug('receiving result... %f' % (time.time() - t1))
                    msg = socket.recv(zmq.NOBLOCK)
                    logger.debug('received: %s' % repr(msg))
                    break
                if self.stop_request or (self.proxy.timeout >= 0 \
                        and time.time() - t0 > self.proxy.timeout):
                    done = True
                    raise RuntimeError(
                        'Connection timeout in ProxyMethod.__call__ for: %s.%s(*%s, **%s)'
                        % (self.proxy.classname, self.method, repr(args),
                          repr(kwargs)))
            result = pickle.loads(msg)
            logger.debug("remote call result:     " + str(result))

            if isinstance(result, ReturnException):
                logger.error('ZRO proxy returned an exception: '
                            + str(result.exc_info[1]))
                logger.error(''.join(traceback.format_stack()))
                if hasattr(result.exc_info[1], 'server_traceback'):
                    logger.error('exception remote traceback:'
                                + result.exc_info[1].server_traceback)
                else:
                    logger.error('exception traceback: '
                                + str(result.exc_info[2]))
                raise result.exc_info[1]

            return result

        finally:
            with self.proxy.lock:
                self.proxy.running_methods.remove(self)
                if self.stop_request:
                    self.stop_request = False

    def interrupt(self):
        with self.proxy.lock:
            self.stop_request = True
        done = False
        while not done:
            with self.proxy.lock:
                if not self.stop_request:
                    done = True
