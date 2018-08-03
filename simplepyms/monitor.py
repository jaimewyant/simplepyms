# How often to publish heartbeats.
import statistics
import time
from collections import deque, defaultdict

from autobahn.twisted.component import Component
from autobahn.twisted.wamp import Session
from autobahn.wamp import PublishOptions, SessionDetails
from twisted.internet.task import LoopingCall

from simplepyms.topics import (TOPIC_DEADSERVICE, TOPIC_HEARTBEAT,
                               TOPIC_HUNGSERVICE, TOPIC_HEARTBEAT_RATE)

HEARTBEAT_RATE = 10


class ServiceStats:
    """
    Tracks service stats over time.
    """

    def __init__(self, max_records=100):
        """Initialize the service stats instance.

        :param max_records: Maximum number of history records to track.
        """
        self.records = deque(maxlen=max_records)

    def store(self, heartbeat):
        """Record heartbeat."""
        heartbeat['timestamp'] = time.time()
        self.records.append(heartbeat)

    def avg_cpu_utilization(self):
        """Average cpu utilization."""
        return statistics.mean([heartbeat['load']
                                for heartbeat in self.records])

    def _last_heartbeat(self):
        """
        Return the last heartbeat or None if not available.
        """
        if len(self.records) == 0:
            return None
        return self.records[-1]

    last_heartbeat = property(_last_heartbeat)


class Monitor(Component):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # List of services being monitored.  Maps service name to ServiceStats.
        self.services = defaultdict(ServiceStats)

        # Every HEARTBEAT_RATE seconds, check for dead or hung services.
        self.dead_service_check_loop = LoopingCall(self.dead_service_check)

        # Set by on_join.
        self.session = None

    def publish_heartbeat_rate(self, heartbeat_rate):
        """
        Publish a heartbeat rate topic.

        This topic is retained so that new clients receive the event.
        """
        return self.session.publish(TOPIC_HEARTBEAT_RATE, heartbeat_rate,
                                    options=PublishOptions(retain=True))

    def on_heartbeat(self, heartbeat_obj):
        self.services[heartbeat_obj['name']].store(heartbeat_obj)

    def dead_service_check(self):
        """Called periodically to check for dead services.

        While checking for services, one of the following two events may be
        published:
        """
        for service_name in self.services:
            svc_stats = self.services[service_name]

            # 3 missed heartbeats and the service is considered dead.
            now = time.time()

            three_heartbeats_away = HEARTBEAT_RATE * 3
            last_heartbeat = svc_stats.last_heartbeat['timestamp']
            if now > last_heartbeat + three_heartbeats_away:
                self.session.publish(TOPIC_DEADSERVICE, service_name)
            elif now > last_heartbeat + HEARTBEAT_RATE:
                self.session.publish(TOPIC_HUNGSERVICE, service_name)


def mk_monitor(transports, realm, authenticationd=None):
    component = Monitor(transports=transports,
                        realm=realm,
                        authentication=authenticationd)

    @component.on_join
    def on_join(session: Session, session_details: SessionDetails):
        component.session = session
        component.session_details = session_details
        session.subscribe(component.on_heartbeat, TOPIC_HEARTBEAT)
        component.publish_heartbeat_rate(HEARTBEAT_RATE)
        component.dead_service_check_loop.start(HEARTBEAT_RATE)

    return component
