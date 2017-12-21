import sro

class TestObject:
    @staticmethod
    def add(a, b):
        return a + b
    def __init__(self, variable):
        self.variable = variable

    def print_variable(self):
        print(self.variable)
        return self.variable

test = TestObject("Hello")
#TODO add doc
#server = zro.ObjectServer(4444)
server = sro.ObjectServer()
objectURI = server.register(test)
print(objectURI)
server.serve_forever()