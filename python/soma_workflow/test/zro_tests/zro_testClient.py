# -*- coding: utf-8 -*-
'''
@authors: Manuel Boissenin, Yann Cointepas, Denis Riviere

@organization: NAO, UNATI, Neurospin, Gif-sur-Yvette, France

'''

from __future__ import absolute_import
from __future__ import print_function
import soma_workflow.zro as zro
from six.moves import input
import threading

object_uri = input("Please enter object URI: ")

socket_lock = threading.Lock()

test_proxy = zro.Proxy(object_uri)

# It is also possible to open multiple socket by
# creating multiple proxies to the remote object
# In that case, there is no need to use a lock.
# proxy2 = zro.Proxy(object_uri)
# proxy3 = zro.Proxy(object_uri)


def toRunInThread(prox, socket_lock):
    i = 0
    while i < 30:
        i = i + 1
        socket_lock.acquire()
        print(prox.modify_string(str(i) + " "
                                 + 50 * "a string that is not so long, check possible interference with thread"))
        socket_lock.release()
    print("thread finished")

ma_thread = threading.Thread(
    target=toRunInThread, args=(test_proxy, socket_lock))
# ma_thread = threading.Thread(target=toRunInThread, args=(proxy2,
# socket_lock))
ma_thread.start()
ma_thread = threading.Thread(
    target=toRunInThread, args=(test_proxy, socket_lock))
ma_thread.start()

socket_lock.acquire()
result = test_proxy.add(40, 2)
socket_lock.release()

print(type(result))
print(result)

# ma_thread.start()

string1 = 10 * \
    "mqksdjfmqskdjfmqskdfjmqsdpqerygqmhdkjqsdmfjqskdfjqmsdjkfmqsjkdfqsmdf"
string2 = 100 * "mlkjmkljmkjmkljmlmmkljmkljmkljmlkjmlkjmkj"
string3 = 1000 * "qsdfqsdfqsdfqsdfqsdfqsdfqsdfqsdfqsdfqsdfq"

socket_lock.acquire()
result = test_proxy.modify_string(string1)
socket_lock.release()
print(result)

socket_lock.acquire()
result = test_proxy.modify_string(string2)
socket_lock.release()
print(result)

socket_lock.acquire()
result = test_proxy.modify_string(string3)
socket_lock.release()
print(result)

socket_lock.acquire()
print(test_proxy.print_variable())
socket_lock.release()

try:
    socket_lock.acquire()
    result = test_proxy.add(40, 'deux')
except Exception as e:
    print("Exception as expected: " + str(e))
socket_lock.release()
