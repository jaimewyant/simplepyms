"""
Topics used by the microservice modules.
"""

#: Posted by the monitor, specifying what the heartbeat rate should be for
#: microservice components.
TOPIC_HEARTBEAT_RATE = 'py.ms.heartbeatrate'

#: Heartbeat event posted by microservices.
TOPIC_HEARTBEAT = 'py.ms.heartbeat'

#: Hung service posted by the MicroserviceMonitor along with the name of the
#: hung service.
TOPIC_HUNGSERVICE = 'py.ms.hungservice'

#: Dead service event posted by the MicroserviceMonitor along with the name of
#: the hung service.
TOPIC_DEADSERVICE = 'py.ms.deadservice'
