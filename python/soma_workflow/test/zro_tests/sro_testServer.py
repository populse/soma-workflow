# -*- coding: utf-8 -*-
'''
@authors: Manuel Boissenin, Yann Cointepas, Denis Riviere

@organization: NAO, UNATI, Neurospin, Gif-sur-Yvette, France

'''

from __future__ import absolute_import
from __future__ import print_function
from . import sro


class TestObject(object):

    @staticmethod
    def add(a, b):
        return a + b

    def __init__(self, variable):
        self.variable = variable

    def print_variable(self):
        print(self.variable)
        return self.variable

test = TestObject("Hello")
# TODO add doc
server = sro.ObjectServer()
objectURI = server.register(test)
print(objectURI)
server.serve_forever()
