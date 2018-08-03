simple-pyms: Python Microservices Without The Headaches
=======================================================

Simple PyMS (SPMS) provides base class abstractions for a very simple 
microservice architecture.  Currently SPMS provides a baseline component that 
sends heartbeats and a monitor component that tracks microservices and their 
statuses.  

The WAMP protocol is used to handle message brokering and RPC.  So, to use PyMS
you will need a
[WAMP router](https://wamp-proto.org/implementations/index.html#routers) such
as [crossbar.io](https://crossbar.io/).

What Does It Do?
----------------
SPMS makes creating microservices (WAMP Components) and monitoring them easy.
There are two components to deal with - Monitor and Microservice.  Monitor
monitors Service instances.

So, create a single Monitor instance and one or more Microservice instances.
Connect them to the router and you have a poor man's microservice architecture. 

Configuring Your Wamp Router
----------------------------

To configure your wamp router, you will need to setup roles for your components.
Each microservice component will need to have permissions setup properly for
the microservice uri: `py.ms.*`

    permissions:
      # Microservice activities
      - uri: py.ms
        match: prefix
        allow:
          call: true
          register: true
          publish: true
          subscribe: true
          
Creating A Simple Component
---------------------------

```python

from simplepyms.microservice import mk_component
from autobahn.twisted.component import run
component = mk_component('component-name', 'ws://localhost:8080/ws', 'realm')
run(component)
```

The code above creates a component that has stop, restart and heartbeat 
notification built in.  Add functionality to make your component more 
meaningful.

```python

from simplepyms.microservice import mk_component
from autobahn.twisted.component import run
component = mk_component('component-name', 'ws://localhost:8080/ws', 'realm')

@component.register('py.ms.add')
def add(a, b):
    return a + b

run(component)
```