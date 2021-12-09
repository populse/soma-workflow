# -*- coding: utf-8 -*-

'''
Small library of custom :class:`~client_types.EngineExecutionJob` subclasses.

Provides jobs for map/reduce patterns, cross-validation folding, and lists manipulations.
'''

from __future__ import absolute_import
from soma_workflow.client_types import Job, EngineExecutionJob, BarrierJob
from six.moves import range
from six.moves import zip


class MapJob(EngineExecutionJob):

    '''
    Map job: converts lists into series of single items. Typically an input
    named ``inputs`` is a list of items. The job will separate items and
    output each of them as an output parameter. The i-th item will be output as
    ``output_<i>`` by default.
    The inputs / outputs names can be customized using the named parameters
    ``input_names`` and ``output_names``. Several lists can be split in the
    same job.
    The job will also output a ``lengths`` parameter which will contain the
    input lists lengths. This lengths can typically be input in reduce jobs to
    perform the reverse operation (see :class:`ReduceJob`).

    * ``input_names`` is a list of input parameters names, each being a list to
    be split. The default is ``['inputs']``
    * ``output_names`` is a list of patterns used to build output parameters
    names. Each item is a string containing a substitution pattern ``"%d"``
    which will be replaced with a number. The default is ``['output_%d']``.
    Each pattern will be used to replace items from the corresponding input in
    the same order. Thus ``input_names``  and ``output_names`` should be the
    same length.
    * all other parameters given in ``param_dict`` are passed to the output
    dictionary of the job, so that the job acts as a
    :class:`~soma_workflow.client_types.BarrierJob` for parameters which are
    not "mapped".

    '''

    def __init__(self,
                 command=[],
                 referenced_input_files=None,
                 referenced_output_files=None,
                 name='map',
                 param_dict=None,
                 **kwargs):
        if param_dict is None:
            param_dict = {}
        if 'input_names' not in param_dict:
            param_dict['input_names'] = ['inputs']
        if 'output_names' not in param_dict:
            param_dict['output_names'] = ['output_%d']
        for inp in param_dict['input_names']:
            if inp not in param_dict:
                param_dict[inp] = []
        super(MapJob, self).__init__(
            command=[],
            referenced_input_files=referenced_input_files,
            referenced_output_files=referenced_output_files,
            name=name,
            param_dict=param_dict,
            has_outputs=True)

    @classmethod
    def engine_execution(cls, self):
        input_names = self.param_dict.get('input_names', ['inputs'])
        output_names = self.param_dict.get('output_names', ['output_%d'])
        out_dict = dict(self.param_dict)
        for name in ['input_names', 'output_names'] + input_names \
                + output_names:
            if name in out_dict:
                del out_dict[name]
        lengths = []
        for inp, out in zip(input_names, output_names):
            inputs = self.param_dict[inp]
            for i, item in enumerate(inputs):
                out_dict[out % i] = item
            lengths.append(len(inputs))
        out_dict['lengths'] = lengths
        return out_dict


class ReduceJob(EngineExecutionJob):

    '''
    Reduce job: converts series of inputs into lists. Typically a series of
    inputs named ``input_0`` .. ``input_<n>`` will be output as a single list
    named ``outputs``.

    Several input series can be handled by the job, and input names can be
    customized.

    * The numbers of inputs for each series is given as the ``lengths`` input
    parameter. It is typically linked from the output of a :class:`MapJob`.
    * Input parameters names patterns are given as the ``input_names``
    parameter. It is a list of patterns, each containing a ``%d``pattern for
    the input number. The defaut value is ``['input_%d']``.
    * Output parameters names are given as the ``output_names`` parameter. The
    default is ``['outputs']``.
    * all other parameters given in ``param_dict`` are passed to the output
    dictionary of the job, so that the job acts as a
    :class:`~soma_workflow.client_types.BarrierJob` for parameters which are
    not "reduced".

    '''

    def __init__(self,
                 command=[],
                 referenced_input_files=None,
                 referenced_output_files=None,
                 name='reduce',
                 param_dict=None,
                 **kwargs):
        if param_dict is None:
            param_dict = {}
        if 'input_names' not in param_dict:
            param_dict['input_names'] = ['input_%d']
        if 'output_names' not in param_dict:
            param_dict['output_names'] = ['outputs']
        if 'lengths' not in param_dict:
            param_dict['lengths'] = [0]
        super(ReduceJob, self).__init__(
            command=[],
            referenced_input_files=referenced_input_files,
            referenced_output_files=referenced_output_files,
            name=name,
            param_dict=param_dict,
            has_outputs=True)
        self.resize_inputs()

    def resize_inputs(self):
        for param, l in zip(self.param_dict['input_names'],
                            self.param_dict['lengths']):
            for i in range(l):
                p = param % i
                if p not in self.param_dict:
                    self.param_dict[p] = ''
            i = l
            while param % i in self.param_dict:
                del self.param_dict[param % i]
                i += 1

    @classmethod
    def engine_execution(cls, self):
        input_names = self.param_dict.get('input_names', ['input_%d'])
        output_names = self.param_dict.get('output_names', ['outputs'])
        lengths = self.param_dict['lengths']
        out_dict = dict(self.param_dict)
        for name in ['input_names', 'output_names', 'lengths'] + input_names \
                + output_names:
            if name in out_dict:
                del out_dict[name]
        for inp, out, length in zip(input_names, output_names, lengths):
            out_list = [self.param_dict[inp % i] for i in range(length)]
            out_dict[out] = out_list
        return out_dict


class LeaveOneOutJob(EngineExecutionJob):

    '''
    Removes an element from an input list, outputs it on a single separate
    output.

    The input list should be specified as the ``inputs`` parameter, and the
    item index as ``index``. The output parameters ``train`` and
    ``test`` will be assigned the modified list and extracted element,
    respectively.
    '''

    def __init__(self,
                 command=[],
                 referenced_input_files=None,
                 referenced_output_files=None,
                 name='leave_one_out',
                 param_dict=None,
                 **kwargs):
        if param_dict is None:
            param_dict = {}
        if 'inputs' not in param_dict:
            param_dict['inputs'] = []
        if 'index' not in param_dict:
            param_dict['index'] = 0
        super(LeaveOneOutJob, self).__init__(
            command=[],
            referenced_input_files=referenced_input_files,
            referenced_output_files=referenced_output_files,
            name=name,
            param_dict=param_dict,
            has_outputs=True)

    @classmethod
    def engine_execution(cls, self):
        inputs = self.param_dict['inputs']
        index = self.param_dict['index']
        output_item = inputs[index]
        output_list = inputs[:index] + inputs[index + 1:]
        out_dict = {
            'train': output_list,
            'test': output_item,
        }
        return out_dict


class CrossValidationFoldJob(EngineExecutionJob):

    '''
    Separates an input list into folds, one (larger) for training, one
    (smaller) for testing.

    The input list ``inputs`` is separated into folds. The number of folds
    should be specified as the ``nfolds`` parameter, the fold number as
    ``fold``. Outputs are ``train` and ``test`` parameters.
    '''

    def __init__(self,
                 command=[],
                 referenced_input_files=None,
                 referenced_output_files=None,
                 name='cross_validation',
                 param_dict=None,
                 **kwargs):
        if param_dict is None:
            param_dict = {}
        if 'inputs' not in param_dict:
            param_dict['inputs'] = []
        if 'nfolds' not in param_dict:
            param_dict['nfolds'] = 2
        if 'fold' not in param_dict:
            param_dict['fold'] = 0
        super(CrossValidationFoldJob, self).__init__(
            command=[],
            referenced_input_files=referenced_input_files,
            referenced_output_files=referenced_output_files,
            name=name,
            param_dict=param_dict,
            has_outputs=True)

    @classmethod
    def engine_execution(cls, self):
        inputs = self.param_dict['inputs']
        fold = self.param_dict['fold']
        nfolds = self.param_dict['nfolds']
        nitems = len(inputs)
        fold_size = nitems // nfolds
        nsupp = nitems % nfolds
        begin = fold_size * fold
        begin += min(begin, nsupp)
        end = fold_size * (fold + 1)
        end += min(end, nsupp)
        train = inputs[:begin] + inputs[end:]
        test = inputs[begin:end]
        out_dict = {
            'train': train,
            'test': test,
        }
        return out_dict


class ListCatJob(EngineExecutionJob):

    '''
    Concatenates several lists into a single list

    The input lists should be specified as the ``inputs`` parameter (a list of
    lists, thus). The output parameter ``outputs``  will be assigned the concatenated list.
    '''

    def __init__(self,
                 command=[],
                 referenced_input_files=None,
                 referenced_output_files=None,
                 name='list_cat',
                 param_dict=None,
                 **kwargs):
        if param_dict is None:
            param_dict = {}
        if 'inputs' not in param_dict:
            param_dict['inputs'] = []
        super(ListCatJob, self).__init__(
            command=[],
            referenced_input_files=referenced_input_files,
            referenced_output_files=referenced_output_files,
            name=name,
            param_dict=param_dict,
            has_outputs=True)

    @classmethod
    def engine_execution(cls, self):
        inputs = self.param_dict['inputs']
        outputs = []
        for in_list in inputs:
            outputs += in_list
        out_dict = {
            'outputs': outputs,
        }
        return out_dict


class StrCatJob(EngineExecutionJob):

    '''
    Concatenates inputs into a string

    Inputs listed in ``input_names`` are concatenated into an output string.
    Inputs may be strings or lists of strings. Listes are also concatenated
    into a string.
    The output parameter is given as the ``output_name`` parameter, if given,
    and defaults to `` output`` otherwise.
    ``input_names`` is optional and defaults to ``inputs`` (thus by default the
    job expects a single list).
    '''

    def __init__(self,
                 command=[],
                 referenced_input_files=None,
                 referenced_output_files=None,
                 name='strcat',
                 param_dict=None,
                 **kwargs):
        if param_dict is None:
            param_dict = {}
        if 'input_names' not in param_dict:
            param_dict['input_names'] = ['inputs']
        if 'output_name' not in param_dict:
            param_dict['output_name'] = 'output'
        output_name = param_dict['output_name']
        if output_name not in param_dict:
            param_dict[output_name] = ''
        for iname in param_dict['input_names']:
            if iname not in param_dict:
                param_dict[iname] = ''
        super(StrCatJob, self).__init__(
            command=[],
            referenced_input_files=referenced_input_files,
            referenced_output_files=referenced_output_files,
            name=name,
            param_dict=param_dict,
            has_outputs=True)

    @classmethod
    def engine_execution(cls, self):
        input_names = self.param_dict['input_names']
        output_name = self.param_dict['output_name']
        outputs = []
        for name in input_names:
            value = self.param_dict[name]
            if isinstance(value, list):
                outputs.append(''.join(value))
            else:
                outputs.append(value)
        output = ''.join(outputs)
        out_dict = {
            output_name: output,
        }
        return out_dict
