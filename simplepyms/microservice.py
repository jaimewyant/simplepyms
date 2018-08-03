"""
Twisted Based Wamp Component using new-style components.
"""
import psutil
import os
import sys
import txaio
from autobahn.twisted.component import Component
from autobahn.twisted.wamp import Session
from autobahn.wamp import SubscribeOptions, SessionDetails, CloseDetails
from twisted.internet.task import LoopingCall
from twisted.internet import reactor
from simplepyms.topics import TOPIC_HEARTBEAT_RATE, TOPIC_HEARTBEAT

log = txaio.make_logger()


class Microservice(Component):
    """
    Microservice provides functionality required for monitoring via
    MicroserviceMonitor.

    Published Topics
    Topic Name                   Argument (object)
    ---------------------------  -----------------------------------------------
    py.ms.heartbeat              Heartbeat:
                                   name: name of the microservice
                                   load: cpu load
                                   cnt: number of heartbeats published

    RPC Endpoints
    Endpoint Name                  Arguments / Return Value
    -----------------------------  ---------------------------------------------
    py.ms.<ms name>.healthcheck    No arguments.  Return value is always True.
                                   This endpoint is used to see if the
                                   microservice is executing.
    py.ms.<ms name>.restart        Request a restart.  Probably will not work
                                   if the service is dead.
    py.ms.<ms name>.stop           Stop the microservice.  This leaves the
                                   session.
    """

    def __init__(self, name, *args, **kwargs):
        self.name = name
        self.heartbeat_cnt = 0
        self.heartbeat_loop = None
        self.session = None
        self.restart_component = False
        # The first call of this is meaningless, it is used to setup subsequent
        # calls
        psutil.cpu_percent()
        super().__init__(*args, **kwargs)

    def publish_heartbeat(self):
        """
        Publish the heartbeat topic.

        Executed by the heartbeat_loop.
        """
        self.heartbeat_cnt += 1
        return self.session.publish(TOPIC_HEARTBEAT, {
            'name': self.name,
            'load': psutil.cpu_percent(),
            'cnt': self.heartbeat_cnt
        })

    def on_restart(self):
        """
        Restarts the component.

        Restarting the component assumes the command line that started the
        component can simply be re-executed.
        """
        self.restart_component = True
        reactor.callLater(0.1, self.on_stop)

    def on_stop(self):
        """
        Stop the component.
        """
        if self.heartbeat_loop:
            self.heartbeat_loop.stop()
        self.session.leave()

    def on_heartbeat_rate(self, heartbeat_rate):
        """
        Handle the heartbeat rate being set / reset.

        :returns: The heartbeat loop deferred.
        """
        if self.heartbeat_loop is not None:
            self.heartbeat_loop.stop()
        self.heartbeat_loop = LoopingCall(self.publish_heartbeat)
        return self.heartbeat_loop.start(heartbeat_rate)

    @staticmethod
    def rpc_healthcheck():
        return True


def mk_component(name, transports, realm, authentication=None):

    component = Microservice(
        name,
        transports=transports,
        realm=realm,
        authentication=authentication
    )

    @component.on_join
    def on_join(session: Session, session_details: SessionDetails):
        component.session = session
        component.session_details = session_details
        session.subscribe(component.on_heartbeat_rate, TOPIC_HEARTBEAT_RATE,
                          options=SubscribeOptions(get_retained=True))
        session.register(component.rpc_healthcheck,
                         f'py.ms.{component.name}.healthcheck')
        session.register(component.on_restart,
                         f'py.ms.{component.name}.restart')
        session.register(component.on_stop,
                         f'py.ms.{component.name}.stop')

    @component.on_leave
    def on_leave(session: Session, close_details: CloseDetails):
        if component.restart_component:
            log.info(f'Restart requested for {session} / {close_details}')
            log.info('Executing {exec} {args}', exec=sys.executable,
                     args=sys.argv)
            os.execl(sys.executable, sys.executable, *sys.argv)

    return component
