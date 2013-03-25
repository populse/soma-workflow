
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


Four main steps should be finished before using soma-workflow, Installation (Client Side), Configuration File (Client Side), Installation (Sever Side), and Configuration File (Server Side)


Installation (Server Side)
------------
  
Soma-workflow depends on some softwares which are required to be installed beforehand:

* Python version >= 2.7.3 and < 3.0
* Drmaa version >= 1.0.13 and <2.0
* cmake version >= 2.6
* sip version >= 4.13

After the above softwares have been installed on the server, we first use ssh connection to connect your remote server (cluster) and then run the **setup_server.py**: 

1: We assume that your account on the server is **userid** and the server ip address or server domain is **serveradd**. Use ssh to connect your remote server:

    $ ssh userid@serveradd

2: Download the latest tarball and expand it, for example in **~/soma-workflow**. We can also use git to download it as:

    $ git clone git@github.com:neurospin/soma-workflow.git ~/soma-workflow

  3: Run the python script **setup_server.py** and **~/soma-workflow**
       
    $ cd ~/soma-workflow
    $ python setup_server.py

  4: At the end, you may need to go back to client side using:
   
    $ exit

  Now, a server named like **userid@serveradd** has been created on your server using the DRMAA system. In the next section, we will setup client side using similar operations.

Installation (Client Side)
------------

  Qt version 4.6.2 or more, PyQt version 4.7.2 or more or
  more are required if you want to use the graphical interface and optionally matplotlib version 0.99.

  To provide you quickly with a functional application, your own multiple core 
  machine can be used directly and without any configuration to distribute 
  computation, no matter the installation mode chosen.


  **(Recommended) Only configurate your environment variables without installation**

    1: Download the latest tarball and expand it, for example in ~/soma-workflow. We can also use git to download it in ~/  as : 

     $ cd ~
     $ git clone git@github.com:neurospin/soma-workflow.git

    2: Edit the file "~/.bashrc" to add these lines:

        SOMAWF_PATH=~/soma-workflow
        export PATH=$SOMAWF_PATH/bin:$PATH
        export PYTHONPATH=$SOMAWF_PATH/python:$PYTHONPATH

  **(Easy, but not recommended) With setup.py for all users in the default folder:**

    1: Download the latest tarball and expand it.

    2: Install Soma-workflow in the /usr/local/lib and /usr/local/bin directory::

      $ sudo python setup.py install 



Configuration File (Client Side)
------------

  Make a configure file for the remote server (~/.soma-workflow.cfg) on the client side as the below format:

    [Cluster_Name_userid]

    CLUSTER_ADDRESS     = ip_address_or_domain
    SUBMITTING_MACHINES = ip_address_or_domain

    QUEUES = long short
    LOGIN = userid

   Replace userid as the login id in remote server.


Configuration File (Server Side)
------------
       
  1: Make a configure file for the remote server (~/.soma-workflow.cfg) on the server side as the below format:
   
	[Cluster_Name_userid]
	DATABASE_FILE  = /home/userid/soma-workflow/soma_workflow.db
	TRANSFERED_FILES_DIR = /home/userid/soma-workflow/transfered-files/
	NAME_SERVER_HOST  = ip_address_or_domain
	SERVER_NAME = soma_workflow_database_userid

	SERVER_LOG_FILE   = /home/userid/soma-workflow/logs/log_server
	SERVER_LOG_FORMAT = %(asctime)s => line %(lineno)s: %(message)s
	SERVER_LOG_LEVEL  = INFO
	ENGINE_LOG_DIR  = /home/userid/soma-workflow/logs
	ENGINE_LOG_FORMAT = %(asctime)s => %(module)s line %(lineno)s: %(message)s              %(threadName)s
	ENGINE_LOG_LEVEL  = INFO

	MAX_JOB_IN_QUEUE = {15} short{15} long{10}

   Replace userid as the login id in remote server. Make the below directories :

	$ mkdir /home/userid/soma-workflow
	$ mkdir /home/userid/soma-workflow/logs
	$ mkdir /home/userid/soma-workflow/transfered-files


At the end, we have finished four main steps. In the client side, we close all terminals and open a new terminal to run "$ soma_workflow_gui". 
You can now use soma_workflow_gui for paraelle computing. An example is shown in 
http://www.brainvisa.info/doc/soma-workflow-2.4/sphinx/examples.html



