
from __future__ import print_function

from traits.api import Undefined


def list_to_sequence(item, src_param, value, dst_param, dst_value):
    ''' item-th element of list value
    '''
    return value[item]


def sequence_to_list(item, src_param, value, dst_param, dst_value):
    ''' set the value value as item-th element of list dest_value
    '''
    if dst_value is None:
        dst_value = [Undefined] * (item + 1)
    if len(dst_value) <= item:
        dst_value += [Undefined] * (item + 1 - len(dst_value))
    dst_value[item] = value
    return dst_value

