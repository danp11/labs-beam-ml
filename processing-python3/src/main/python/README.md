LuciDoItDoIt ![Python Server CI](https://github.com/Talend/labs-beam-ml/workflows/Python%20Server%20CI/badge.svg)
============ 

`LuciDoItDoIt` is a server that does stuff, and also things. 


Building and running
--------------------

```
# Setting up and using virtualenv.
python -m venv env/
source env/bin/activate

./setup.py test

# Create a distribution
pip install wheel
./setup.py sdist bdist_wheel

# Install from repo
./setup.py install

# Or install from wheel
pip install dist/lucidoitdoit-*.whl

# After installing, you can run the script.

# Print usage instructions
lucidoitdoit --help

# Run a server.
lucidoitdoit server

# In the same virtualenv while the server is running.
lucidoitdoit client 'output = input.upper().split(" ")' "a b c"
```


Building and running with the wrapper
-------------------------------------

```
# Whether or not you are in the virtual env
export LUCIDOITDOIT_WHL=$(find $PWD -name lucidoitdoit-0.1-py3-none-any.whl)

bin/lucisetup a1b2c3

# The server should be started at a random available port written into
# /tmp/a1b2c3/lucidoitdoit.socket
```


Testing and automation
----------------------

```
pip install --upgrade setuptools pip nose black flake8
black lucidoitdoit tests bin/lucidoitdoit
flake8 lucidoitdoit tests bin/lucidoitdoit
nosetests
```
