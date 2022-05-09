from unittest.mock import MagicMock

from heron.tools.tracker.src.python.app import app
from heron.tools.tracker.src.python.tracker import Tracker
from heron.tools.tracker.src.python import state, constants

import pytest

from fastapi.testclient import TestClient

@pytest.fixture
def tracker(monkeypatch):
  mock = MagicMock(Tracker)
  monkeypatch.setattr(state, "tracker", mock)
  return mock

@pytest.fixture
def client(tracker):
  return TestClient(app)

def test_clusters(client, tracker):
  c1, c2 = MagicMock(), MagicMock()
  c1.configure_mock(name="c1")
  c2.configure_mock(name="c2")

  tracker.state_managers = [c1, c2]
  response = client.get("/clusters")
  assert response.json() == ["c1", "c2"]
  assert response.status_code == 200

def test_machines(client):
  response = client.get("/machines", json={
      "cluster": ["c1", "c3"],
      "environ": ["e1", "e3"],
  })
  assert response.json() == {}

def test_topologies(client):
  response = client.get("/topologies", json={
      "cluster": [],
      "environ": [],
  })
  assert response.json() == {}
