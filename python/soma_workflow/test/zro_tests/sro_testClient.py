# -*- coding: utf-8 -*-
'''
@authors: Manuel Boissenin, Yann Cointepas, Denis Riviere

@organization: NAO, UNATI, Neurospin, Gif-sur-Yvette, France

'''

from __future__ import absolute_import
from __future__ import print_function
from . import sro
from six.moves import input

object_uri = input("Please enter object URI: ")

test_proxy = sro.Proxy(object_uri)

result = test_proxy.add(40, 2)
print(type(result))
print(result)

print(test_proxy.print_variable())

try:
    result = test_proxy.add(40, 'deux')
except Exception as e:
    print("Exception as expected: " + str(e))
