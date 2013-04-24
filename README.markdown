Soma-workflow
=============


Main Features
-------------

**Unified interface to multiple computing resources:** 
Submission of jobs or workflows with an unique interface to various parallel resources: multiple core machines or clusters which can be managed by various systems (such as Grid Engine, Condor, Torque/PBS, LSF..)

**Workflow management:** 
Soma-workflow provides the possibility to submit a set of tasks (called jobs) with execution dependencies without dealing with individual task submission.

**Python API and Graphical User Interface:** 
The Python API was designed to be easily used by non expert user, but also complete to meet external software needs: submission, control and monitoring of jobs and workflows. The GUI provides an easy and quick way of monitoring workflows on various computing resources. The workflows can also be submitted and controlled using the GUI.

**Quick start on multiple core machines:** 
Soma-workflow is directly operational on any multiple core machine. 
    
**Transparent remote access to computing resources:** 
When the computing resource is remote, Soma-workflow can be used as a client-server application. The communication with a remote computing resource is done transparently for the user through a ssh port forwarding tunnel. The client/server architecture enables the user to close the client application at any time. The workflows and jobs execution are not stopped. The user can open a client at any time to check the status of his work.

**File transfer and file path mapping tools:** 
If the user's machine and the remote computing resource do not have a shared file system, Soma-workflow provides tools to handle file transfers and/or path name matchings.

Documentation
-------------

Visit Soma-workflow on http://neurospin.github.com/soma-workflow


Installation
------------

Before install soma-workflow, soma softwares are required: 

* Python version >= 2.7.3 and < 3.0
* Qt version 4.6.2 or more, PyQt version 4.7.2 or more or more are required if you want to use the graphical interface 
* matplotlib version 0.99.

To provide you quickly with a functional application, your own multiple core machine can be used directly and without any configuration to distribute computation, no matter the installation mode chosen.

1: Download the latest tarball and expand it, for example in **~/soma-workflow**. We can also use git to download it as:

    $ git clone git@github.com:neurospin/soma-workflow.git ~/soma-workflow

2: Run the python setup script :

    $ cd ~/soma-workflow
    $ python setup.py install --prefix ~/mylocal

where "~/mylocal" is your installation path. You can add "~/mylocal/python" for "PYTHONPATH" and add "~/mylocal/bin" for "PATH". 

3: At the end, you should restart your terminal:
    
    $ exit 

Open a new terminal to enter:

    $ soma_workflow_gui

Now, you can see the soma-workflow graphical interface to control your cluster. 
