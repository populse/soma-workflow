import zro
from six.moves import input

object_uri = input("Please enter object URI: ")

test_proxy = zro.Proxy(object_uri)

result = test_proxy.add(40, 2)
print(type(result))
print(result)

print(test_proxy.print_variable())

try:
    result = test_proxy.add(40, 'deux')
except Exception as e:
    print("Exception as expected: " + str(e))