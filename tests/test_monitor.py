import time
from unittest.mock import MagicMock

from autobahn.wamp import PublishOptions
from pytest_twisted import inlineCallbacks

from simplepyms.monitor import mk_monitor, ServiceStats, HEARTBEAT_RATE
from simplepyms.topics import TOPIC_HEARTBEAT, TOPIC_HEARTBEAT_RATE, \
    TOPIC_HUNGSERVICE, TOPIC_DEADSERVICE


def test_on_heartbeat():
    monitor = mk_monitor('ws://localhost:8080/ws', 'realm')
    monitor.on_heartbeat(dict(name='simple', load=1, cnt=2))
    monitor.on_heartbeat(dict(name='simple', load=1, cnt=3))
    last_heartbeat = dict(name='simple', load=1, cnt=4)
    monitor.on_heartbeat(last_heartbeat)
    assert 'simple' in monitor.services
    assert monitor.services['simple'].avg_cpu_utilization() == 1
    assert len(monitor.services['simple'].records) == 3
    assert monitor.services['simple'].last_heartbeat == last_heartbeat


def test_service_stats_last_heartbeat_when_empty():
    assert ServiceStats().last_heartbeat is None


@inlineCallbacks
def test_on_join():
    monitor = mk_monitor('ws://localhost:8080/ws', 'realm')

    # Fires the join event.
    mock_session = MagicMock()
    yield monitor.fire('join', mock_session, None)

    assert mock_session.subscribe.called_once_with(monitor.on_heartbeat,
                                                   TOPIC_HEARTBEAT)

    assert mock_session.publish.called_with(TOPIC_HEARTBEAT_RATE,
                                            HEARTBEAT_RATE,
                                            options=PublishOptions(retain=True))

    assert monitor.dead_service_check_loop is not None
    assert monitor.dead_service_check_loop.running


@inlineCallbacks
def test_monitor_deadservice_check(monkeypatch):
    monitor = mk_monitor('ws://localhost:8080/ws', 'realm')
    mock_session = MagicMock()
    yield monitor.fire('join', mock_session, None)

    # Load up the first heartbeat.
    monitor.on_heartbeat(dict(name='name', cnt=0, load=1))

    # First check hung service.
    future = time.time() + HEARTBEAT_RATE + 1
    monkeypatch.setattr(time, 'time', lambda: future)

    monitor.dead_service_check()
    mock_session.publish.assert_called_with(TOPIC_HUNGSERVICE, 'name')

    # Fast forward time 100 seconds
    future = time.time() + 100
    monkeypatch.setattr(time, 'time', lambda: future)
    monitor.dead_service_check()
    mock_session.publish.assert_called_with(TOPIC_DEADSERVICE, 'name')
