# -*- coding: utf-8 -*-

import unittest
import sys

import soma_workflow.client as wfclient


class WorkflowApiTests(unittest.TestCase):

    '''
    Soma_workflow workflow API tests.
    '''

    def test_workflow_simple(self):
        job1 = wfclient.Job(['ls', '-l', '/tmp'])
        job2 = wfclient.Job(['ls', '-l', '/tmp'])
        job3 = wfclient.Job(['ls', '-l', '/tmp'])
        workflow = wfclient.Workflow(
            [job1, job2, job3],
            dependencies=[(job1, job2), (job1, job3)])
        self.assertTrue(len(workflow.jobs) == 3)
        self.assertTrue(len(workflow.dependencies) == 2)

    def test_workflow_merge(self):
        job1 = wfclient.Job(['ls', '-l', '/tmp'], name='job1')
        job2 = wfclient.Job(['ls', '-l', '/tmp'], name='job2')
        job3 = wfclient.Job(['ls', '-l', '/tmp'], name='job3')
        group = wfclient.Group([job1, job2, job3], name='group1')
        workflow1 = wfclient.Workflow(
            [job1, job2, job3],
            dependencies=[(job1, job2), (job1, job3)],
            root_group=[group])
        job4 = wfclient.Job(['ls', '-l', '/tmp'], name='job4')
        job5 = wfclient.Job(['ls', '-l', '/tmp'], name='job5')
        job6 = wfclient.Job(['ls', '-l', '/tmp'], name='job6')
        workflow2 = wfclient.Workflow(
            [job4, job5, job6], name='workflow2',
            dependencies=[(job4, job6), (job5, job6)])
        group2 = workflow1.add_workflow(workflow2, as_group='group2')
        self.assertTrue(len(workflow1.jobs) == 6)
        self.assertTrue(len(workflow1.root_group) == 2)
        self.assertTrue(len(workflow1.groups) == 2)
        self.assertTrue(len(workflow1.dependencies) == 4)
        # make both wf sequential
        workflow1.add_dependencies([(group, group2)])
        # 4 hub barrier jobs have been added
        self.assertTrue(len(workflow1.jobs) == 10)
        self.assertTrue(len(workflow1.groups) == 2)
        self.assertTrue(len(workflow1.dependencies) == 17)

        # do it again using sets for deps
        workflow3 = wfclient.Workflow(
            [job1, job2, job3],
            dependencies=set([(job1, job2), (job1, job3)]),
            root_group=[group])
        workflow4 = wfclient.Workflow(
            [job4, job5, job6], name='workflow4',
            dependencies=set([(job4, job6), (job5, job6)]))
        group3 = workflow3.add_workflow(workflow4, as_group='group3')
        self.assertTrue(len(workflow3.jobs) == 6)
        self.assertTrue(len(workflow3.root_group) == 2)
        self.assertTrue(len(workflow3.groups) == 2)
        self.assertTrue(len(workflow3.dependencies) == 4)
        # make both wf sequential
        workflow3.add_dependencies([(group, group3)])
        # 4 hub barrier jobs have been added
        self.assertTrue(len(workflow3.jobs) == 10)
        self.assertTrue(len(workflow3.groups) == 2)
        self.assertTrue(len(workflow3.dependencies) == 17)

def test():
    suite = unittest.TestLoader().loadTestsFromTestCase(WorkflowApiTests)
    runtime = unittest.TextTestRunner(verbosity=2).run(suite)
    res = runtime.wasSuccessful()
    return res

if __name__ == '__main__':
    res = test()
    sys.exit(int(not res))
