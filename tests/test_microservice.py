import os
import sys
from unittest.mock import MagicMock

import psutil
import pytest
from autobahn.twisted.util import sleep
from pytest_twisted import inlineCallbacks
from twisted.internet.task import LoopingCall

from simplepyms.microservice import mk_component, Microservice
from simplepyms.topics import TOPIC_HEARTBEAT


@pytest.fixture
def ms_component(monkeypatch):
    component = mk_component('simple', 'ws://localhost:8080/ws', 'realm')
    mock_session = MagicMock()
    monkeypatch.setattr(component, 'session', mock_session)
    return component


@inlineCallbacks
def test_microservice_heartbeat(monkeypatch, ms_component):
    monkeypatch.setattr(psutil, 'cpu_percent', lambda: 1)

    assert ms_component.rpc_healthcheck()

    # Send heartbeat every one second.
    ms_component.on_heartbeat_rate(0.5)
    yield sleep(1.5)
    ms_component.heartbeat_loop.stop()

    session = ms_component.session
    assert session.publish.call_args[0][0] == TOPIC_HEARTBEAT
    assert session.publish.call_args[0][1]['name'] == 'simple'
    assert session.publish.call_args[0][1]['load'] == 1
    assert 'cnt' in session.publish.call_args[0][1]


@inlineCallbacks
def test_microservice_restart(ms_component: Microservice):
    yield ms_component.fire('join', ms_component.session, None)
    ms_component.on_heartbeat_rate(0.1)
    yield ms_component.on_restart()
    # Wait long enough for the session leave to trigger.
    yield sleep(.25)
    ms_component.session.leave.assert_called_once()
    assert ms_component.heartbeat_loop.running == False


def test_on_heartbeat_rate(monkeypatch, ms_component: Microservice):
    mock_heartbeat_loop = MagicMock()
    monkeypatch.setattr(ms_component, 'heartbeat_loop', mock_heartbeat_loop)
    ms_component.on_heartbeat_rate(5)
    mock_heartbeat_loop.stop.assert_called_once()
    assert type(ms_component.heartbeat_loop) == LoopingCall


@inlineCallbacks
def test_onleave_restart(monkeypatch, ms_component: Microservice):
    """
    Test that onleave attempts to restart the microservice.
    """
    mock_execl = MagicMock()
    monkeypatch.setattr(os, 'execl', mock_execl)
    ms_component.on_restart()

    yield ms_component.fire('leave', ms_component.session, None)

    mock_execl.assert_called_once_with(sys.executable, sys.executable,
                                       *sys.argv)
