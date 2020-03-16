# -*- coding: utf-8 -*-
'''
@authors: Manuel Boissenin, Yann Cointepas, Denis Riviere

@organization: NAO, UNATI, Neurospin, Gif-sur-Yvette, France

'''

from __future__ import absolute_import
from __future__ import print_function
import soma_workflow.zro as zro


class TestObject(object):

    @staticmethod
    def add(a, b):
        return a + b

    def __init__(self, variable):
        self.variable = variable

    def modify_string(self, string):
        return "A long string sent by the client: " + string

    def print_variable(self):
        print(self.variable)
        return self.variable

if __name__ == '__main__':

    test = TestObject("Hello")
    # TODO add doc
    # server = zro.ObjectServer(4444)
    server = zro.ObjectServer()
    objectURI = server.register(test)
    print(objectURI)
    server.serve_forever()
