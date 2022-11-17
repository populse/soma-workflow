# -*- coding: utf-8 -*-

from __future__ import print_function

from __future__ import absolute_import
try:
    from traits.api import Undefined
except ImportError:
    # no traits: use None
    Undefined = None


def list_to_sequence(item, src_param, value, dst_param, dst_value):
    ''' item-th element of list value
    '''
    return value[item]


def sequence_to_list(item, src_param, value, dst_param, dst_value):
    ''' set the value value as item-th element of list dst_value
    '''
    if dst_value is None:
        dst_value = [Undefined] * (item + 1)
    if len(dst_value) <= item:
        dst_value += [Undefined] * (item + 1 - len(dst_value))
    dst_value[item] = value
    return dst_value


def append_to_list(src_param, value, dst_param, dst_value):
    ''' appends the value value at the end of list dst_value
    '''
    if dst_value is None:
        dst_value = []
    dst_value.append(value)
    return dst_value


def list_all_but_one(item, src_param, value, dst_param, dst_value):
    ''' remove item-th element from the input list.
    Useful in a leave-one-out pattern
    '''
    return value[:item] + value[item + 1:]


def list_cv_train_fold(fold, nfolds, src_param, value, dst_param, dst_value):
    ''' take all but fold-th division in a list divided into nfold folds.
    Useful in a nfolds cross-validation pattern
    '''
    nitems = len(value)
    fold_size = nitems // nfolds
    nsupp = nitems % nfolds
    begin = fold_size * fold
    begin += min(begin, nsupp)
    end = fold_size * (fold + 1)
    end += min(end, nsupp)
    return value[:begin] + value[end:]


def list_cv_test_fold(fold, nfolds, src_param, value, dst_param, dst_value):
    ''' take fold-th division in a list divided into nfold folds.
    Useful in a nfolds cross-validation pattern
    '''
    nitems = len(value)
    fold_size = nitems // nfolds
    nsupp = nitems % nfolds
    begin = fold_size * fold
    begin += min(begin, nsupp)
    end = fold_size * (fold + 1)
    end += min(end, nsupp)
    return value[begin:end]


def list_cat(item, src_param, value, dst_param, dst_value):
    ''' concatenates lists: extend value (list) after dst_value
    '''
    if dst_value is None:
        dst_value = []
    dst_value += value
    return dst_value


def sequence_max(shift, src_param, value, dst_param, dst_value):
    ''' get maximum value from a list
    '''
    #print('shift', shift,
          #'src_param:', src_param,
          #'dst_param:', dst_param, 
          #'dst_value:', dst_value, 
          #'value', value)
    if dst_value is None:
        dst_value = type(value)(0) + shift
    dst_value = max(dst_value, value + shift)
    return dst_value


def sequence_min(shift, src_param, value, dst_param, dst_value):
    ''' get minimum value from a list
    '''
    if dst_value is None:
        dst_value = type(value)(value + shift)

    dst_value = min(dst_value, value + shift)
    return dst_value


def sequence_sum(shift, src_param, value, dst_param, dst_value):
    ''' sum value from a list
    '''
    if dst_value is None:
        dst_value = type(value)(0) + shift
    
    dst_value += value

    return dst_value
