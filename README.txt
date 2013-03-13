
Soma-workflow
=============


Main Features
-------------

  **Unified interface to multiple computing resources:** 
    Submission of jobs or workflows with an unique interface to various 
    parallel resources: multiple core machines or clusters which can be 
    managed by various systems (such as Grid Engine, Condor, Torque/PBS, LSF..)

  **Workflow management:**
    Soma-workflow provides the possibility to submit a set of tasks (called jobs) 
    with execution dependencies without dealing with individual task submission.

  **Python API and Graphical User Interface:**
    The Python API was designed to be easily used by non expert user, but also
    complete to meet external software needs: submission, control and monitoring 
    of jobs and workflows. The GUI provides an easy and quick way of monitoring 
    workflows on various computing resources. The workflows can also be 
    submitted and controlled using the GUI.

  **Quick start on multiple core machines:**
    Soma-workflow is directly operational on any multiple core machine. 
    
  **Transparent remote access to computing resources:** 
    When the computing resource is remote, Soma-workflow can be used as a   
    client-server application. The communication with a remote computing 
    resource is done transparently for the user through a ssh port forwarding 
    tunnel. The client/server architecture enables the user to close the client 
    application at any time. The workflows and jobs execution are not stopped. 
    The user can open a client at any time to check the status of his 
    work.

  **File transfer and file path mapping tools:** 
    If the user's machine and the remote computing resource do not have a shared 
    file system, Soma-workflow provides tools to handle file transfers and/or 
    path name matchings.

Documentation
-------------

  Visit Soma-workflow_ main page!

  An extensive documentation_ is available, with ready to use examples_.

  .. _Soma-workflow: http://www.brainvisa.info/soma-workflow


Installation (Client Side)
------------

  Qt version 4.6.2 or more, PyQt version 4.7.2 or more or PySide version 1.1.1 or
  more are required if you want to use the graphical interface. 

  To provide you quickly with a functional application, your own multiple core 
  machine can be used directly and without any configuration to distribute 
  computation, no matter the installation mode chosen.

  **(Recommended) Only configurate your environment variables without installation**

    1: Download the latest tarball and expand it, for example in ~/soma-workflow.

    2: Edit the file "~/.bashrc" to add these lines:

        SOMAWF_PATH=~/soma-workflow
        export PATH=$SOMAWF_PATH/bin:$PATH
        export PYTHONPATH=$SOMAWF_PATH/python:$PYTHONPATH

  **(Recommended) With setup.py only for my account in a local directory:**

    1: Download the latest tarball and expand it.

    2: Make all diretories for your path, for example here is ~/mylocal.

    In terminal:

      $ mkdir ~/mylocal

    3: Edit the file "~/.bashrc" to add these lines:
    
        SOMAWF_PATH=~/mylocal
        export PATH=$SOMAWF_PATH/bin:$PATH
        export PYTHONPATH=$SOMAWF_PATH/lib/python2.7/site-packages/:$PYTHONPATH
    
    Attention: the version of python2.7 in PYTHONPATH should be same with python which you are using. For example, when your python version is 2.6, you should use export PYTHONPATH=$SOMAWF_PATH/lib/python2.6/site-packages/:$PYTHONPATH

    4: Install Soma-workflow in the local directory ~/mylocal

      $ sudo python setup.py install --prefix ~/mylocal
   
  **(Easy, but not recommended) With setup.py for all users in the default folder:**

    1: Download the latest tarball and expand it.

    2: Install Soma-workflow in the /usr/local/lib and /usr/local/bin directory::

      $ sudo python setup.py install 


  **With easy_install:**

    
    This command will just install Soma-workflow::

      $ easy_install --prefix ~/.local "soma-workflow"

    To enable plotting in the GUI, this command will install matplotlib as well::

      $ easy_install --prefix ~/.local "soma-workflow[plotting]"

    To install the client interface to a remote computing resource, this command 
    will install Soma-workflow, Pyro and Paramiko ::

      $ easy_install --prefix ~/.local "soma-workflow[client]"

    To install the client application with plotting enabled, this command 
    will install Soma-workflow, Pyro, Paramiko and matplotlib::

      $ easy_install --prefix ~/.local "soma-workflow[client,plotting]"


Configuration (Client Side)
------------

  Make a configure file on the client side as the below format:

    [Cluster_Name]

    CLUSTER_ADDRESS     = ip address or domain
    SUBMITTING_MACHINES = name

    QUEUES = long short
    LOGIN = userid
