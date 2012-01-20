
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
  .. _examples: http://www.brainvisa.info/doc/soma-workflow-2.1/sphinx/examples.html
  .. _documentation: http://www.brainvisa.info/doc/soma-workflow-2.1/sphinx/index.html


Installation
------------

  Qt version 4.6.2 or more, PyQt version 4.7.2 or more are required if you want to 
  use the graphical interface. 

  To provide you quickly with a functional application, your own multiple core 
  machine can be used directly and without any configuration to distribute 
  computation, no matter the installation mode chosen.

  We recommend to install Soma-workflow in a local directory (no special rights required & easy clean up at any time removing the local directory)

    1. Create a local directory such as *~/.local/lib/python2.6/site-packages* and create the bin directory: *~/.local/bin*

    2. Setup the environment variables with the commands::

        $ export PYTHONPATH=$HOME/.local/lib/python2.6/site-packages:$PYTHONPATH

        $ export PATH=$HOME/.local/bin:$PATH

      You can copy these lines in your ~/.bashrc for an automatic setup of the variables at login.

    3. Install Soma-workflow using setup.py or easy_install.


  **With setup.py:**

    Download the latest tarball and expand it.

    Install Soma-workflow in the ~/.local directory::

      $ python setup.py install --user

    If you chose a different name for you local directory (ex: ~/mylocal) use instead the following command::

      $ python setup.py install --prefix ~/mylocal

    Installation on the system with administrator rights::

        $ python setup.py install

  **With easy_install**

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




