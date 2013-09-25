=============
Soma-workflow
=============


Main Features
=============

  **Unified interface to multiple computing resources:** 
    Submission of jobs or workflows with a unique interface to various 
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
=============

  Visit Soma-workflow (http://www.brainvisa.info/soma-workflow) main page!

  An extensive documentation (http://www.brainvisa.info/doc/soma-workflow-2.4/sphinx/index.html) is available, 
  with ready to use examples (http://www.brainvisa.info/doc/soma-workflow-2.4/sphinx/examples.html).


Download
========

  Soma-workflow is available on pypi to download: https://pypi.python.org/pypi/soma-workflow.



Installation
=============

  Qt version 4.6.2 or more, PyQt version 4.7.2 or more or PySide version 1.1.1 or
  more are required if you want to use the graphical interface. 

  To provide you quickly with a functional application, your own multiple core 
  machine can be used directly and without any configuration to distribute 
  computation, no matter the installation mode chosen.


  **Full installation in Ubuntu:**
  
  Installation required packages,
  python-qt4 (graphical interface), 
  python-matplotlib (option), 
  python-paramiko (option: required to connect server), 
  pyro (option: required to connect server) :
      
      $ sudo apt-get update

      $ sudo apt-get install python-qt4 python-matplotlib python-paramiko pyro

  Download soma-workflow and go to soma-workflow directory:
      
      $ sudo python setup.py install

  Try soma-workflow in terminal with:
      
      $ soma_workflow_gui

  **Windows (tested on Vista 64 bit for local mode)**

  1) Go to http://www.python.org/download/ to download and install python Windows Installer according to your windows version, for example in C:\Python27.

  2) Set environment variable PATH that contains python directory (see http://docs.python.org/2/using/windows.html), for example in C:\Python27.

  3) Download and install PyQt4 for python27 in http://www.riverbankcomputing.com/software/pyqt/download
 
  4) Go to https://pypi.python.org/pypi/soma-workflow to download soma-workflow and extract it into a directory, for example, C:\soma-workflow.

  5) In command line mode (start -> cmd), run:

     a. cd C:\\soma-workflow

     b. python setup.py install

  6) Run soma_workflow_gui

     a. cd C:\\Python27\\Scripts

     b. python soma_workflow_gui
    
  You can use soma-workflow for local mode on the windows platform.

  **Manually Installation (for all other systems)**

  Download and install those packages: 
     python-qt4 (graphical interface), 
     python-matplotlib (option), 
     python-paramiko (required to connect server), 
     pyro (required to connect server)   
  
  1) Download soma-workflow into soma-workflow-path

  2) Set PYTHONPATH contains soma-workflow-path/python, and PATH contains soma-workflow-path/bin

    2.1) For example, in Linux, add lines in ~/.bashrc
        
         $ export PYTHONPATH=soma-workflow-path/python:$PYTHONPATH

         $ export PATH=soma-workflow-path/bin:$PATH

  3) Try soma-workflow in terminal with:

    $ soma_workflow_gui 



