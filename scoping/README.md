## Setting up a virtual environment (MacOS/Linux)

In order to set up all the dependencies to run the Python code, you will need to first ensure you have all the correct dependencies. In order to do this, we are going to create a virtual environment. If you don't already have virtualenv installed, do so now using the following command:

	python3 -m pip install --user virtualenv

Once you have virtualenv installed, you will create a virtual environment for this project with the following steps:

1. Find out where your default Python3 installation is located:
	which python3
2. Create a new virtual environment set up your virtual environment:
	virtualenv -p /path/to/python3 venv

Be sure to replace /path/to/python3 with the result of the 'which python3' command.

Next, activate the virtual environment:

	source venv/bin/activate
	
Once inside your virtual environment, you will install the dependencies:

	pip3 install -r requirements.txt
	
Create a new kernel for the Jupyter Notebook:

	ipython kernel install --user --name=dc_traffic_bot
	
Open your Jupyter Notebook file!

	jupyter notebook dataset_linking_analysis.ipynb

Once inside Jupyter, select kernel > Change kernel > dc_traffic_bot to use the virtualenv you'e created.

	
	
	

