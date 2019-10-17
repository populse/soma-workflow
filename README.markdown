Soma-workflow
=============

[![](https://travis-ci.org/populse/soma-workflow.svg?branch=master)](https://travis-ci.org/populse/soma-workflow) [![Build status](https://ci.appveyor.com/api/projects/status/9n7jp4p3eenv1o24/branch/master?svg=true)](https://ci.appveyor.com/project/denisri/soma-workflow-87l7c/branch/master) [![](https://codecov.io/github/populse/soma-workflow/coverage.svg?branch=master)](https://codecov.io/github/populse/soma-workflow) [![](https://img.shields.io/badge/license-CeCILL--B-blue.svg)](https://github.com/populse/soma-workflow/blob/master/LICENSE.en) [![](https://img.shields.io/pypi/v/soma-workflow.svg)](https://pypi.python.org/pypi/soma-workflow/)                                           [![](https://img.shields.io/badge/python-2.6%2C%202.7%2C%203.3%2C%203.4%2C%203.5%2C%203.6%2C%203.7-yellow.svg)](#)                                                                      [![](https://img.shields.io/badge/platform-Linux%2C%20OSX%2C%20Windows-orange.svg)](#)


Main Features
-------------

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
-------------

  Visit Soma-workflow (http://www.brainvisa.info/soma-workflow/sphinx/) main page!

  An extensive documentation (http://www.brainvisa.info/doc/soma-workflow/sphinx/index.html) is available,
  with ready to use examples (http://www.brainvisa.info/doc/soma-workflow/sphinx/examples.html).
  
  The git master branch doc is also updated on github: https://populse.github.io/soma-workflow/


Download
--------

  Soma-workflow is available on pypi to download: https://pypi.python.org/pypi/soma-workflow.


Installation
------------

  Qt version 4.6.2 or more, PyQt version 4.7.2 or more or PySide version 1.1.1 or
  more are required if you want to use the graphical interface. 

  To provide you quickly with a functional application, your own multiple core 
  machine can be used directly and without any configuration to distribute 
  computation, no matter the installation mode chosen.


  **Full installation in Ubuntu:**
  
  Installation required packages,
  python-qt4 or PyQt5 (graphical interface), 
  python-matplotlib (option), 
  python-paramiko (option: required to connect server), 
  python-zmq (option: required to connect server) :
      
      $ sudo apt-get update

      $ sudo apt-get install python-qt4 python-matplotlib python-paramiko python-zmq
      
  Either:
  * Users: install using pip:
     
      $ pip install soma-workflow

  * developers: Download soma-workflow and go to soma-workflow directory:
      
      $ sudo python setup.py install

  Try soma-workflow in terminal with:
      
      $ soma_workflow_gui

  **Windows (tested on Vista 64 bit for local mode)**

  1) Go to http://www.python.org/download/ to download and install python Windows Installer according to your windows version, for example in C:\Python27.

  2) Set environment variable PATH that contains python directory (see http://docs.python.org/2/using/windows.html), for example in C:\Python27.

  3) Download and install PyQt4 for python27 in http://www.riverbankcomputing.com/software/pyqt/download
 
  4) Go to https://pypi.python.org/pypi/soma-workflow to download soma-workflow and extract it into a directory, for example, C:\soma-workflow.

  5) In command line mode (start -> cmd), run:

     a. cd C:\soma-workflow

     b. python setup.py install

  6) Run soma_workflow_gui

     a. cd C:\Python27\Scripts

     b. python soma_workflow_gui
    
  You can use soma-workflow for local mode on the windows platform.

  **Manually Installation (for all other systems)**

  Download and install those packages: 
     python-qt4 or PyQt5 (graphical interface), 
     python-matplotlib (option), 
     python-paramiko (required to connect server), 
     python-zmq (required to connect server)   
  
  1) Download soma-workflow into soma-workflow-path

  2) Set PYTHONPATH contains soma-workflow-path/python, and PATH contains soma-workflow-path/bin

    2.1) For example, in Linux, add lines in ~/.bashrc
        
         $ export PYTHONPATH=soma-workflow-path/python:$PYTHONPATH

         $ export PATH=soma-workflow-path/bin:$PATH

  3) Try soma-workflow in terminal with:

    $ soma_workflow_gui 
