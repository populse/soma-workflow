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
import six
import uuid

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

    def __init__(self, worker_id, context, poller):
        super(WorkerThread, self).__init__()
        self.worker_id = worker_id
        self.lock = threading.RLock()
        self.todo = []
        self.replies = []
        self.stop_request = False
        self._hold_running = True  # keep alive until we submit a job
        self.event = threading.Event()
        self.context = context
        # we use the ObjectServer poller to register a socket pair. This socket
        # will send signals when worker job results are available in order to
        # send results immediately to the clients. The socket here is
        # thread-specific: zmq doesn't send messages on the same receiver
        # from multiple threads, so each threads must register its own.
        self.poller = poller
        self.uuid = uuid.uuid4()
        self.serv_sock = context.socket(zmq.PAIR)
        self.serv_sock.bind('inproc://%s' % self.uuid)
        self.poller.register(self.serv_sock, zmq.POLLIN)

    def __del__(self):
        # the socket will be deleted and must not be monitored any longer.
        self.poller.unregister(self.serv_sock)

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

        # connect a socket to the ObjectServer poller to notify answers
        context = self.context
        reply_sock = context.socket(zmq.PAIR)
        reply_sock.connect("inproc://%s" % self.uuid)

        while self.running():
            job = None
            with self.lock:
                if len(self.todo) != 0:
                    job = self.todo.pop(0)
                    #logger.debug('pop job: ' + repr(job))
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
                socket, client, instance, method, args, kwargs = job
                #call_time, rec_time, rec1_time = timing
                #pop_time = time.time()
                try:
                    method_code = getattr(instance, method)
                    logger.debug('calling: ' + repr(method_code))
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

                #res_time = time.time()
                #logger.debug('result (%f s): ' % (res_time - pop_time) + repr(result))
                logger.debug('%s result : %s'
                             % (repr(method_code), repr(result)))
                with self.lock:
                    self.replies.append((socket, client, result))  #,
                                         #(res_time - call_time, rec_time, rec1_time, call_time, pop_time, res_time)))
                    # wake the communication thread
                    reply_sock.send(b'')
                    #logger.debug('replies: ' + str(len(self.replies)) + ', todo: ' + str(len(self.todo)))

        logger.debug('stopping worker: ' + self.worker_id)


class ObjectServer(object):

    '''
    ObjectServer runs on server side. It listens to client requests (method
    calls on a registered object), executes them, and sends back the results.

    Each request may be performed from any thread on client side, and we
    replicate client threads on server side: each client thread gets a worker
    thread on server side to process its requests. This way we do not get into
    deadlocks or out-of-order answers.

    Communications are based on zmq sockets (REQ on client side, ROUTER on
    server side).

    Usage:
    -create an ObjectServer providing a port.
    -register the object you want to access from another
     program that might be on a distant object.
    -lauch the server loop.

    The loop expects events from client(s). When a message is received
    '''

    def __init__(self, port=None):
        self.objects = {}
        self.context = zmq.Context.instance()
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
        #self.poller = zmq.Poller()

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
                     + " object, on " + repr(self.port)
                     + ', id: %s' % str(id(object)))

        return str(object.__class__.__name__) + ":" + str(id(object)) + ":" + str(self.port)

    def serve_forever(self):
        logger = logging.getLogger('zro.ObjectServer')
        interval = 100
        frontend = self.context.socket(zmq.ROUTER)
        frontend.bind("tcp://*:" + str(self.port))
        poller = zmq.Poller()
        self.poller = poller
        poller.register(frontend, zmq.POLLIN)

        logger.debug("ObS0: " + str(self.port) + ":Waiting for incoming data")
        #init_time = time.time()
        #nreq = 0
        #ovh_total = 0.
        #exc_total = 0.
        #rcv_total = 0.
        #send_total = 0.
        #wait_total = 0.
        #other_total = 0.
        #getwk_total = 0.
        #getres_total = 0.
        # logger.setLevel(logging.DEBUG)

        while not self.must_stop():
            try:
                #  Wait for next request from client
                #logger.debug("ObS0:" + str(self.port)[-3:]
                            #+ ":Waiting for incoming data")
                socks = dict(poller.poll(interval))
                event = None
                #logger.info('socks: %s' % repr(socks))
                for zsocket in socks:
                    if zsocket is frontend:
                        event = True
                        #t0 = time.time()
                        message = frontend.recv_multipart()
                        client = message[0]
                        classname, object_id, method, thread_id, args, kwargs \
                            = pickle.loads(message[2])
                        instance = self.objects.get(
                            classname, {}).get(object_id)
                        #t01 = time.time()
                        if instance:
                            logger.debug(
                                "ObS1:" + str(self.port) + ":calling "
                                + classname + " " + object_id + ", thread: " + thread_id + ", client: " + repr(client) + ": " + method
                                + " " + repr(args))
                            worker = self.get_worker(thread_id)
                            with worker.lock:
                                worker.add_job(
                                    (frontend, client,
                                    self.objects[classname][object_id],
                                    method, args, kwargs))  #, (time.time(), t0, t01)))
                        else:
                            logger.info(
                                "object not in the list of objects: %s:%s, "
                                "port: %s"
                                % (classname, object_id, self.port))

                    else:
                        # an internal message is received on an inproc socket,
                        # meaning that an job answer is ready to be sent to
                        # the client.
                        #logger.debug('answer ready')
                        dummy_msg = zsocket.recv()
                        #logger.info('message: %s' % repr(dummy_msg))
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
                                #frontend, client, result, timimg = reply
                                frontend, client, result = reply
                                #exc_time, rec_time, rec1_time, call_time, pop_time, res_time = timimg
                                #t = time.time()
                                #nreq += 1
                                #logger.debug("ObS2:" + str(self.port)[-3:]
                                            #+ " (%f s - %d req in %f: %f req/s): %s result is: "
                                            #% (exc_time, nreq, t - init_time,
                                                #nreq / (t - init_time), repr(client))
                                        #+ repr(result))
                                #t = time.time()

                                logger.debug(
                                    "ObS2:" + str(self.port)[-3:]
                                    + ": %s result is: " % repr(client)
                                    + repr(result))

                                # send result
                                frontend.send_multipart(
                                    (client, b'', pickle.dumps(result)))

                                ## timing debugging code...
                                #t1 = time.time()
                                #overhead = t1 - rec_time - exc_time
                                #ovh_total += overhead
                                #exc_total += res_time - pop_time
                                #rec_dur = rec1_time - rec_time
                                #rcv_total += rec_dur
                                #send_dur = t1 - t
                                #send_total += send_dur
                                #wait_dur = pop_time - call_time
                                #other_dur = overhead - rec_dur - send_dur - wait_dur
                                #other_total += other_dur
                                #getwk_dur = call_time - rec1_time
                                #getres_dur = t - res_time
                                #getwk_total += getwk_dur
                                #getres_total += getres_dur

                                #logger.info('req time: %f total, %f recv, %f exec, %f send, %f overhd, %f exc avg, %f ovh avg, %f rcv avg, %f send avg, %f wait avg, %f other avg, %f ovh tot, %d req, %f getwk avg, %f getres avg' % (t1 - rec_time, pop_time - rec_time, res_time - pop_time, t1 - res_time, overhead, exc_total / nreq, ovh_total / nreq, rcv_total / nreq, send_total / nreq, wait_total / nreq, other_total / nreq, ovh_total, nreq, getwk_total / nreq, getres_total / nreq))

                        for worker_id, worker in ended_workers.items():
                            worker.join()
                            del self.workers[worker_id]

            except Exception as e:
                logger.exception(e)
                # print("An exception occurred in the server of the remote object")
                # traceback.print_exc()

    def get_worker(self, worker_id):
        worker = self.workers.get(worker_id)
        old_res = None
        if worker:
            with worker.lock:
                if worker.running():
                    worker.hold_running()
                    return worker
                old_res = worker.replies
            del self.workers[worker_id]
        worker = WorkerThread(worker_id, self.context, self.poller)
        if old_res:
            # the older worker gets deleted, but it may still hold
            # unprocessed results. Thus we must keep them in the new worker
            # for the same thread.
            worker.replies = old_res
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
    The Proxy object is created on client side with the uri of the object
    afterwards you can call any method you want on it,
    to access variable attributes you will have to create properties
    (accessors)
    """

    # we need to refcount the used ports in order to garbage-collect the
    # sockets in ProxyMethod
    ports_count = {}
    class_lock = threading.RLock()

    def __init__(self, uri):
        self.context = zmq.Context.instance()
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
        logger.debug("Proxy: " + str(self.classname) + ":"
                     + str(self.object_id) + ":" + str(self._port))
        # TODO
        # logging.debug(self.classname, self.object_id, self._port)
        self.timeout = -1
        self.running_methods = set()
        with Proxy.class_lock:
            Proxy.ports_count[self._port] \
                = Proxy.ports_count.get(self._port, 0) + 1

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
        with Proxy.class_lock:
            count = Proxy.ports_count.get(self._port, 0) - 1
            if count != 0:
                Proxy.ports_count[self._port] = count
            else:
                del Proxy.ports_count[self._port]

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

    # per-thread socket
    sockets = {}
    lock = threading.RLock()

    def __init__(self, proxy, method):
        self.proxy = weakref.proxy(proxy)
        self.method = method
        self.stop_request = False

    def get_socket(self):
        # garbage-collect sockets (could be done elsewhere, at another time)
        thread_names = set([thread.name for thread in threading.enumerate()])
        with Proxy.class_lock:
            used_ports = set([p for p, n in Proxy.ports_count.items()
                              if n > 0])
        with ProxyMethod.lock:
            ProxyMethod.sockets = dict(
                [(thread_port, socket)
                 for thread_port, socket in six.iteritems(ProxyMethod.sockets)
                 if thread_port[0] in thread_names
                    and thread_port[1] in used_ports])

            thread = threading.current_thread().name
            socket = ProxyMethod.sockets.get((thread, self.proxy._port))
            if socket:
                return socket
            socket = self.proxy.context.socket(zmq.REQ)
            ProxyMethod.sockets[(thread, self.proxy._port)] = socket
        # caveat: note that connect will succeed even if there is
        # no port waiting for a connection, as such you have to
        # check yourself that the connection has succeeded
        # (cf how the database server engine is handled
        socket.connect("tcp://localhost:" + self.proxy._port)
        return socket

    def __call__(self, *args, **kwargs):
        try:
            timeout = 5000  # ms
            logger = logging.getLogger('zro.ObjectServer')
            logger.debug('Execute method: %s.%s(*%s, **%s)'
                         % (self.proxy.classname, self.method, repr(args),
                            repr(kwargs)))
            socket = self.get_socket()
            logger.debug('socket: %s, thread: %s, port: %s'
                         % (repr(socket), threading.current_thread().name,
                            self.proxy._port))
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
                    logger.error(
                        'Connection timeout in ProxyMethod.__call__, '
                        'port %s, for: %s.%s(*%s, **%s)'
                        % (self.proxy._port, self.proxy.classname, self.method,
                           repr(args), repr(kwargs)))
                    # reset socket for this thread (by deleting it)
                    with self.lock:
                        del ProxyMethod.sockets[(thread_id, self.proxy._port)]
                    raise RuntimeError(
                        'Connection timeout in ProxyMethod.__call__, '
                        'port %s, for: %s.%s(*%s, **%s)'
                        % (self.proxy._port, self.proxy.classname, self.method,
                           repr(args), repr(kwargs)))
            result = pickle.loads(msg)
            logger.debug(
              "remote call result for %s.%s/%s:     %s"
              % (self.proxy.classname, self.method, self.proxy.object_id,
                 str(result)))

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
