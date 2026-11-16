from __future__ import annotations

import json
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE))

from fastapi.testclient import TestClient  # noqa: E402

from app.main import create_app  # noqa: E402


def make_client(tmp_path: Path) -> TestClient:
    app = create_app(tmp_path / "test.db")
    return TestClient(app)


def topology(
    nodes: list[dict],
    containers: list[dict],
) -> dict:
    return {"nodes": nodes, "containers": containers}


def test_import_and_snapshot(tmp_path):
    client = make_client(tmp_path)
    payload = json.loads((BASE / "examples" / "cluster.json").read_text())
    with client:
        resp = client.post("/api/topology", json=payload)
        assert resp.status_code == 200, resp.text
        state = resp.json()
        assert state["topology_loaded"]
        assert len(state["nodes"]) == 4
        a = next(n for n in state["nodes"] if n["id"] == "node-a")
        assert a["cpu_capacity"] == 16
        assert a["used_cpu"] == 8
        assert a["remaining_cpu"] == 8
        api = next(s for s in state["services"] if s["name"] == "api")
        assert api["ready"] == 3 and api["min_ready"] == 2
        assert all(c["migration"] is None for c in state["containers"])


def test_reservation_before_cutover(tmp_path):
    client = make_client(tmp_path)
    payload = topology(
        nodes=[
            {"id": "n1", "name": "one", "cpu_capacity": 8},
            {"id": "n2", "name": "two", "cpu_capacity": 8},
        ],
        containers=[
            {"id": "c1", "service": "svc", "min_ready": 1, "node_id": "n1", "cpu_request": 2},
            {"id": "c2", "service": "svc", "min_ready": 1, "node_id": "n2", "cpu_request": 2},
        ],
    )
    with client:
        assert client.post("/api/topology", json=payload).status_code == 200
        state = client.post("/api/nodes/n1/drain").json()
        n2 = next(n for n in state["nodes"] if n["id"] == "n2")
        assert n2["reserved_cpu"] == 2
        assert n2["used_cpu"] == 4
        assert n2["remaining_cpu"] == 4
        # Old replica still serving, so used CPU on n1 is unchanged.
        n1 = next(n for n in state["nodes"] if n["id"] == "n1")
        assert n1["used_cpu"] == 2
        assert n1["serving_containers"] == 1
        # Starting replica is not counted as ready.
        svc = next(s for s in state["services"] if s["name"] == "svc")
        assert svc["ready"] == 2
        assert svc["starting_replicas"] == 1
        migration = state["open_drains"][0]["pending_decisions"][0]
        assert migration["state"] == "starting"
        assert migration["target_node_id"] == "n2"

        # Resolve ready -> cutover moves the replica, drain completes.
        state = client.post(
            f"/api/migrations/{migration['id']}/resolve", json={"outcome": "ready"}
        ).json()
        c1 = next(c for c in state["containers"] if c["id"] == "c1")
        assert c1["node_id"] == "n2"
        n1 = next(n for n in state["nodes"] if n["id"] == "n1")
        assert n1["serving_containers"] == 0
        assert n1["is_empty"] is True
        assert state["open_drains"] == []
        assert state["drains"][0]["status"] == "completed"


def test_failed_start_keeps_old_replica(tmp_path):
    client = make_client(tmp_path)
    payload = topology(
        nodes=[
            {"id": "n1", "cpu_capacity": 8},
            {"id": "n2", "cpu_capacity": 8},
        ],
        containers=[
            {"id": "c1", "service": "svc", "min_ready": 1, "node_id": "n1", "cpu_request": 2},
        ],
    )
    with client:
        client.post("/api/topology", json=payload)
        state = client.post("/api/nodes/n1/drain").json()
        migration = state["open_drains"][0]["pending_decisions"][0]
        state = client.post(
            f"/api/migrations/{migration['id']}/resolve",
            json={"outcome": "failed", "reason": "模拟启动失败"},
        ).json()

        c1 = next(c for c in state["containers"] if c["id"] == "c1")
        assert c1["node_id"] == "n1"
        n2 = next(n for n in state["nodes"] if n["id"] == "n2")
        assert n2["reserved_cpu"] == 0
        assert n2["remaining_cpu"] == 8
        n1 = next(n for n in state["nodes"] if n["id"] == "n1")
        assert n1["serving_containers"] == 1
        assert n1["is_empty"] is False
        assert state["open_drains"][0]["status"] == "incomplete"
        blocked = state["blocked_containers"]
        assert len(blocked) == 1
        assert blocked[0]["container_id"] == "c1"
        assert blocked[0]["reason_code"] == "REPLICA_START_FAILED"
        svc = next(s for s in state["services"] if s["name"] == "svc")
        assert svc["ready"] == 1


def test_no_capacity_blocks_and_old_replica_serves(tmp_path):
    client = make_client(tmp_path)
    payload = topology(
        nodes=[
            {"id": "n1", "cpu_capacity": 4},
            {"id": "n2", "cpu_capacity": 4},
        ],
        containers=[
            {"id": "c1", "service": "svc", "min_ready": 1, "node_id": "n1", "cpu_request": 4},
            {"id": "c2", "service": "other", "min_ready": 1, "node_id": "n2", "cpu_request": 4},
        ],
    )
    with client:
        client.post("/api/topology", json=payload)
        state = client.post("/api/nodes/n1/drain").json()
        blocked = state["blocked_containers"]
        assert len(blocked) == 1
        assert blocked[0]["container_id"] == "c1"
        assert blocked[0]["reason_code"] == "INSUFFICIENT_CAPACITY"
        n1 = next(n for n in state["nodes"] if n["id"] == "n1")
        assert n1["serving_containers"] == 1
        assert n1["is_empty"] is False
        assert n1["status"] == "draining"
        assert state["open_drains"][0]["status"] == "incomplete"
        assert state["open_drains"][0]["progress_percent"] == 0.0


def test_concurrent_drains_do_not_double_book(tmp_path):
    client = make_client(tmp_path)
    payload = topology(
        nodes=[
            {"id": "n1", "cpu_capacity": 8},
            {"id": "n2", "cpu_capacity": 8},
            {"id": "n3", "cpu_capacity": 4},
        ],
        containers=[
            {"id": "c1", "service": "a", "min_ready": 1, "node_id": "n1", "cpu_request": 3},
            {"id": "c2", "service": "b", "min_ready": 1, "node_id": "n2", "cpu_request": 3},
        ],
    )
    with client:
        client.post("/api/topology", json=payload)
        first = client.post("/api/nodes/n1/drain").json()
        assert first["open_drains"][0]["pending_decisions"][0]["target_node_id"] == "n2"
        # n2 now holds c2 plus a 3 CPU reservation -> only 2 free.
        second = client.post("/api/nodes/n2/drain")
        assert second.status_code == 409
        # n3 has 4 free, enough for one 3 CPU replica only.
        state = client.post("/api/nodes/n2/drain")
        # n2 is already draining? No -> first request was for n1.
        # n2 has an inbound reservation, so draining it is refused.
        assert state.status_code == 409


def test_restart_resumes_unfinished_drain(tmp_path):
    db = tmp_path / "restart.db"
    payload = topology(
        nodes=[
            {"id": "n1", "cpu_capacity": 8},
            {"id": "n2", "cpu_capacity": 8},
        ],
        containers=[
            {"id": "c1", "service": "svc", "min_ready": 1, "node_id": "n1", "cpu_request": 2},
        ],
    )
    first = TestClient(create_app(db))
    with first:
        first.post("/api/topology", json=payload)
        state = first.post("/api/nodes/n1/drain").json()
        mid = state["open_drains"][0]["pending_decisions"][0]["id"]
        assert state["open_drains"][0]["status"] == "active"

    second = TestClient(create_app(db))
    with second:
        state = second.get("/api/state").json()
        assert state["open_drains"][0]["status"] == "active"
        pending = state["open_drains"][0]["pending_decisions"]
        assert len(pending) == 1 and pending[0]["id"] == mid
        n2 = next(n for n in state["nodes"] if n["id"] == "n2")
        assert n2["reserved_cpu"] == 2

        state = second.post(
            f"/api/migrations/{mid}/resolve", json={"outcome": "ready"}
        ).json()
        n1 = next(n for n in state["nodes"] if n["id"] == "n1")
        assert n1["is_empty"] is True
        assert state["drains"][0]["status"] == "completed"


def test_resume_after_failure_reports_incomplete_not_empty(tmp_path):
    db = tmp_path / "resume.db"
    payload = topology(
        nodes=[
            {"id": "n1", "cpu_capacity": 8},
            {"id": "n2", "cpu_capacity": 8},
        ],
        containers=[
            {"id": "c1", "service": "svc", "min_ready": 1, "node_id": "n1", "cpu_request": 2},
        ],
    )
    client = TestClient(create_app(db))
    with client:
        client.post("/api/topology", json=payload)
        state = client.post("/api/nodes/n1/drain").json()
        mid = state["open_drains"][0]["pending_decisions"][0]["id"]
        state = client.post(
            f"/api/migrations/{mid}/resolve", json={"outcome": "failed"}
        ).json()
        assert state["open_drains"][0]["status"] == "incomplete"

        # A new drain on the same node is refused; resume retries.
        assert client.post("/api/nodes/n1/drain").status_code == 409
        state = client.post("/api/nodes/n1/resume").json()
        assert len(state["open_drains"][0]["pending_decisions"]) == 1
        assert state["open_drains"][0]["status"] == "active"

        new_mid = state["open_drains"][0]["pending_decisions"][0]["id"]
        state = client.post(
            f"/api/migrations/{new_mid}/resolve", json={"outcome": "ready"}
        ).json()
        assert state["open_drains"] == []
        n1 = next(n for n in state["nodes"] if n["id"] == "n1")
        assert n1["is_empty"] is True
        drain = state["drains"][0]
        assert drain["status"] == "completed"
        assert drain["moved"] == 1
        assert drain["total"] == 1


def test_validation_rejects_bad_topology(tmp_path):
    client = make_client(tmp_path)
    with client:
        too_many_nodes = topology(
            [{"id": f"n{i}", "cpu_capacity": 2} for i in range(5)], []
        )
        resp = client.post("/api/topology", json=too_many_nodes)
        assert resp.status_code == 422

        over_capacity = topology(
            [{"id": "n1", "cpu_capacity": 1}],
            [{"id": "c1", "service": "s", "min_ready": 1, "node_id": "n1", "cpu_request": 2}],
        )
        resp = client.post("/api/topology", json=over_capacity)
        assert resp.status_code == 422

        unknown_node = topology(
            [{"id": "n1", "cpu_capacity": 2}],
            [{"id": "c1", "service": "s", "min_ready": 1, "node_id": "n9", "cpu_request": 1}],
        )
        assert client.post("/api/topology", json=unknown_node).status_code == 422

        too_many_containers = topology(
            [{"id": "n1", "cpu_capacity": 100}],
            [
                {"id": f"c{i}", "service": "s", "min_ready": 1, "node_id": "n1", "cpu_request": 1}
                for i in range(9)
            ],
        )
        assert client.post("/api/topology", json=too_many_containers).status_code == 422


def test_drain_excludes_draining_targets(tmp_path):
    client = make_client(tmp_path)
    payload = topology(
        nodes=[
            {"id": "n1", "cpu_capacity": 4},
            {"id": "n2", "cpu_capacity": 4},
            {"id": "n3", "cpu_capacity": 4},
        ],
        containers=[
            {"id": "c1", "service": "a", "min_ready": 1, "node_id": "n1", "cpu_request": 2},
            {"id": "c2", "service": "b", "min_ready": 1, "node_id": "n3", "cpu_request": 4},
        ],
    )
    with client:
        client.post("/api/topology", json=payload)
        state = client.post("/api/nodes/n1/drain").json()
        assert state["open_drains"][0]["pending_decisions"][0]["target_node_id"] == "n2"
        state = client.post("/api/nodes/n2/drain")
        assert state.status_code == 409
        state = client.post("/api/nodes/n3/drain")
        # c2 needs 4 CPU; n2 has only 2 left after c1's reservation, and n1 is
        # itself draining, so the drain starts but c2 is blocked and keeps
        # serving on n3.
        assert state.status_code == 200
        body = state.json()
        blocked_ids = {item["container_id"] for item in body["blocked_containers"]}
        assert blocked_ids == {"c2"}
        n3 = next(n for n in body["nodes"] if n["id"] == "n3")
        assert n3["serving_containers"] == 1
        assert n3["is_empty"] is False
        assert client.post("/api/nodes/n1/drain").status_code == 409


def test_migrations_are_retried_after_resume(tmp_path):
    client = make_client(tmp_path)
    payload = topology(
        nodes=[
            {"id": "n1", "cpu_capacity": 4},
            {"id": "n2", "cpu_capacity": 4},
            {"id": "n3", "cpu_capacity": 4},
        ],
        containers=[
            {"id": "c1", "service": "svc", "min_ready": 1, "node_id": "n1", "cpu_request": 4},
            {"id": "c2", "service": "other", "min_ready": 1, "node_id": "n2", "cpu_request": 4},
        ],
    )
    with client:
        client.post("/api/topology", json=payload)
        state = client.post("/api/nodes/n1/drain").json()
        # n2 full, n3 empty -> c1 moves to n3.
        assert state["open_drains"][0]["pending_decisions"][0]["target_node_id"] == "n3"
        mid = state["open_drains"][0]["pending_decisions"][0]["id"]
        state = client.post(
            f"/api/migrations/{mid}/resolve", json={"outcome": "failed"}
        ).json()
        assert state["open_drains"][0]["status"] == "incomplete"
        # Retry while n3 still has room.
        state = client.post("/api/nodes/n1/resume").json()
        assert state["open_drains"][0]["pending_decisions"][0]["target_node_id"] == "n3"


def test_node_empty_only_after_no_serving_replicas(tmp_path):
    client = make_client(tmp_path)
    payload = topology(
        nodes=[
            {"id": "n1", "cpu_capacity": 8},
            {"id": "n2", "cpu_capacity": 4},
        ],
        containers=[
            {"id": "c1", "service": "a", "min_ready": 1, "node_id": "n1", "cpu_request": 2},
            {"id": "c2", "service": "b", "min_ready": 1, "node_id": "n1", "cpu_request": 6},
        ],
    )
    with client:
        client.post("/api/topology", json=payload)
        state = client.post("/api/nodes/n1/drain").json()
        # c2 (6 CPU) has no room on the 4-CPU n2; c1 (2 CPU) fits. The
        # scheduler places the largest replicas first, so c2 is blocked and
        # c1 still moves.
        assert len(state["open_drains"][0]["pending_decisions"]) == 1
        assert {b["container_id"] for b in state["blocked_containers"]} == {"c2"}
        mid = state["open_drains"][0]["pending_decisions"][0]["id"]
        state = client.post(
            f"/api/migrations/{mid}/resolve", json={"outcome": "ready"}
        ).json()
        n1 = next(n for n in state["nodes"] if n["id"] == "n1")
        # c2 still serves on n1, so the node must not be reported empty.
        assert n1["serving_containers"] == 1
        assert n1["status"] == "draining"
        assert n1["is_empty"] is False
        assert state["open_drains"][0]["status"] == "incomplete"
        assert state["drains"][0]["status"] != "completed"
        assert state["unfinished_drain_count"] == 1


def test_reservation_reduces_remaining_capacity_and_is_released(tmp_path):
    client = make_client(tmp_path)
    payload = topology(
        nodes=[
            {"id": "n1", "cpu_capacity": 8},
            {"id": "n2", "cpu_capacity": 8},
        ],
        containers=[
            {"id": "c1", "service": "a", "min_ready": 1, "node_id": "n1", "cpu_request": 3},
        ],
    )
    with client:
        client.post("/api/topology", json=payload)
        state = client.post("/api/nodes/n1/drain").json()
        n2 = next(n for n in state["nodes"] if n["id"] == "n2")
        assert n2["reserved_cpu"] == 3
        assert n2["remaining_cpu"] == 5
        mid = state["open_drains"][0]["pending_decisions"][0]["id"]
        state = client.post(
            f"/api/migrations/{mid}/resolve", json={"outcome": "failed"}
        ).json()
        n2 = next(n for n in state["nodes"] if n["id"] == "n2")
        assert n2["reserved_cpu"] == 0
        assert n2["remaining_cpu"] == 8


def test_target_must_not_be_draining_and_round_trip(tmp_path):
    client = make_client(tmp_path)
    payload = topology(
        nodes=[
            {"id": "n1", "cpu_capacity": 8},
            {"id": "n2", "cpu_capacity": 8},
            {"id": "n3", "cpu_capacity": 8},
        ],
        containers=[
            {"id": "c1", "service": "a", "min_ready": 1, "node_id": "n1", "cpu_request": 2},
        ],
    )
    with client:
        client.post("/api/topology", json=payload)
        state = client.post("/api/nodes/n1/drain").json()
        mid = state["open_drains"][0]["pending_decisions"][0]["id"]
        client.post(f"/api/migrations/{mid}/resolve", json={"outcome": "ready"})
        n1 = next(n for n in client.get("/api/state").json()["nodes"] if n["id"] == "n1")
        assert n1["is_empty"] is True

        resp = client.post("/api/nodes/n1/restore")
        assert resp.status_code == 200, resp.text
        n1 = next(n for n in resp.json()["nodes"] if n["id"] == "n1")
        assert n1["status"] == "ready"
        assert n1["can_drain"] is True
        # Re-draining an empty node completes immediately.
        state = client.post("/api/nodes/n1/drain").json()
        assert state["drains"][0]["status"] == "completed"
        assert state["open_drains"] == []


def test_resolve_rejects_stale_migration(tmp_path):
    client = make_client(tmp_path)
    payload = topology(
        nodes=[
            {"id": "n1", "cpu_capacity": 8},
            {"id": "n2", "cpu_capacity": 8},
        ],
        containers=[
            {"id": "c1", "service": "a", "min_ready": 1, "node_id": "n1", "cpu_request": 2},
        ],
    )
    with client:
        client.post("/api/topology", json=payload)
        state = client.post("/api/nodes/n1/drain").json()
        mid = state["open_drains"][0]["pending_decisions"][0]["id"]
        first = client.post(f"/api/migrations/{mid}/resolve", json={"outcome": "ready"})
        assert first.status_code == 200
        again = client.post(f"/api/migrations/{mid}/resolve", json={"outcome": "ready"})
        assert again.status_code == 409
        assert again.json()["error"]["code"] == "MIGRATION_NOT_PENDING"
        missing = client.post("/api/migrations/9999/resolve", json={"outcome": "ready"})
        assert missing.status_code == 404


def test_restart_keeps_reservations_and_pending(tmp_path):
    db = tmp_path / "restart2.db"
    payload = topology(
        nodes=[
            {"id": "n1", "cpu_capacity": 8},
            {"id": "n2", "cpu_capacity": 8},
        ],
        containers=[
            {"id": "c1", "service": "a", "min_ready": 1, "node_id": "n1", "cpu_request": 3},
        ],
    )
    client = TestClient(create_app(db))
    with client:
        client.post("/api/topology", json=payload)
        state = client.post("/api/nodes/n1/drain").json()
        mid = state["open_drains"][0]["pending_decisions"][0]["id"]

    reopened = TestClient(create_app(db))
    with reopened:
        state = reopened.get("/api/state").json()
        n2 = next(n for n in state["nodes"] if n["id"] == "n2")
        assert n2["reserved_cpu"] == 3
        assert n2["remaining_cpu"] == 5
        # Draining n2 is refused while it holds an inbound reservation.
        assert reopened.post("/api/nodes/n2/drain").status_code == 409
        state = reopened.post(
            f"/api/migrations/{mid}/resolve", json={"outcome": "ready"}
        ).json()
        assert state["drains"][0]["status"] == "completed"
        n2 = next(n for n in state["nodes"] if n["id"] == "n2")
        assert n2["used_cpu"] == 3
        assert n2["reserved_cpu"] == 0


def test_example_topology_endpoint_matches_file(tmp_path):
    client = make_client(tmp_path)
    with client:
        resp = client.get("/api/topology/example")
        assert resp.status_code == 200
        payload = resp.json()
        on_disk = json.loads((BASE / "examples" / "cluster.json").read_text())
        assert payload == on_disk
        assert client.post("/api/topology", json=payload).status_code == 200


def two_replica_payload() -> dict:
    return topology(
        nodes=[
            {"id": "n1", "cpu_capacity": 8},
            {"id": "n2", "cpu_capacity": 8},
            {"id": "n3", "cpu_capacity": 8},
        ],
        containers=[
            {"id": "c1", "service": "svc", "min_ready": 1, "node_id": "n1", "cpu_request": 2},
            {"id": "c2", "service": "svc", "min_ready": 1, "node_id": "n1", "cpu_request": 2},
        ],
    )


def test_abort_keeps_cutovers_and_releases_reservations(tmp_path):
    client = make_client(tmp_path)
    with client:
        client.post("/api/topology", json=two_replica_payload())
        state = client.post("/api/nodes/n1/drain").json()
        pending = state["open_drains"][0]["pending_decisions"]
        assert len(pending) == 2
        # One replica cuts over; the other is still starting when the
        # operator calls off the maintenance.
        first = pending[0]
        state = client.post(
            f"/api/migrations/{first['id']}/resolve", json={"outcome": "ready"}
        ).json()
        moved_id = first["container_id"]

        state = client.post("/api/nodes/n1/abort").json()
        assert state["open_drains"] == []
        drain = state["drains"][0]
        assert drain["status"] == "cancelled"
        assert drain["moved"] == 1
        assert drain["total"] == 2
        assert drain["cancelled"] == 1
        assert drain["starting"] == 0
        assert drain["finished_at"]

        # The cut-over replica stays on its target and keeps using capacity.
        moved = next(c for c in state["containers"] if c["id"] == moved_id)
        assert moved["node_id"] != "n1"
        target = next(n for n in state["nodes"] if n["id"] == moved["node_id"])
        assert target["serving_cpu"] == 2
        # The not-yet-ready replica's reservation is released; its old
        # replica keeps serving on the source node.
        cancelled_id = "c2" if moved_id == "c1" else "c1"
        kept = next(c for c in state["containers"] if c["id"] == cancelled_id)
        assert kept["node_id"] == "n1"
        assert kept["migration"] is None
        assert all(n["reserved_cpu"] == 0 for n in state["nodes"])
        # The source node is usable again and is not reported empty.
        n1 = next(n for n in state["nodes"] if n["id"] == "n1")
        assert n1["status"] == "ready"
        assert n1["serving_containers"] == 1
        assert n1["is_empty"] is False
        assert n1["can_drain"] is True
        # The cancelled attempt is recorded on the drain's history.
        cancelled_attempts = [
            a for a in drain["attempts"] if a["state"] == "cancelled"
        ]
        assert len(cancelled_attempts) == 1
        assert cancelled_attempts[0]["container_id"] == cancelled_id
        assert cancelled_attempts[0]["error_code"] == "DRAIN_ABORTED"
        svc = next(s for s in state["services"] if s["name"] == "svc")
        assert svc["ready"] == 2


def test_abort_then_late_ready_is_rejected(tmp_path):
    """Abort commits first: a late 'replica ready' must not cut over."""
    client = make_client(tmp_path)
    with client:
        client.post("/api/topology", json=two_replica_payload())
        state = client.post("/api/nodes/n1/drain").json()
        mid = state["open_drains"][0]["pending_decisions"][0]["id"]
        container_id = state["open_drains"][0]["pending_decisions"][0]["container_id"]

        state = client.post("/api/nodes/n1/abort").json()
        assert state["drains"][0]["status"] == "cancelled"

        late = client.post(f"/api/migrations/{mid}/resolve", json={"outcome": "ready"})
        assert late.status_code == 409
        assert late.json()["error"]["code"] == "MIGRATION_NOT_PENDING"

        state = client.get("/api/state").json()
        container = next(c for c in state["containers"] if c["id"] == container_id)
        assert container["node_id"] == "n1"
        assert all(n["reserved_cpu"] == 0 for n in state["nodes"])
        drain = client.get(f"/api/drains/{state['drains'][0]['id']}").json()
        assert drain["status"] == "cancelled"
        assert drain["moved"] == 0
        assert drain["cancelled"] == 2


def test_ready_committed_before_abort_is_kept(tmp_path):
    """Ready commits first: the cutover stands, abort cancels the rest."""
    client = make_client(tmp_path)
    with client:
        client.post("/api/topology", json=two_replica_payload())
        state = client.post("/api/nodes/n1/drain").json()
        pending = state["open_drains"][0]["pending_decisions"]
        first, second = pending[0], pending[1]

        client.post(f"/api/migrations/{first['id']}/resolve", json={"outcome": "ready"})
        state = client.post("/api/nodes/n1/abort").json()

        moved = next(c for c in state["containers"] if c["id"] == first["container_id"])
        assert moved["node_id"] != "n1"
        kept = next(c for c in state["containers"] if c["id"] == second["container_id"])
        assert kept["node_id"] == "n1"
        drain = state["drains"][0]
        assert drain["status"] == "cancelled"
        assert drain["moved"] == 1
        assert drain["cancelled"] == 1
        # Resolving the cancelled migration afterwards is refused.
        late = client.post(
            f"/api/migrations/{second['id']}/resolve", json={"outcome": "ready"}
        )
        assert late.status_code == 409


def test_abort_requires_an_open_drain(tmp_path):
    client = make_client(tmp_path)
    with client:
        client.post("/api/topology", json=two_replica_payload())
        resp = client.post("/api/nodes/n1/abort")
        assert resp.status_code == 409
        assert resp.json()["error"]["code"] == "NO_OPEN_DRAIN"
        assert client.post("/api/nodes/ghost/abort").status_code == 404
        # A completed drain cannot be aborted either.
        state = client.post("/api/nodes/n1/drain").json()
        for migration in state["open_drains"][0]["pending_decisions"]:
            client.post(
                f"/api/migrations/{migration['id']}/resolve", json={"outcome": "ready"}
            )
        resp = client.post("/api/nodes/n1/abort")
        assert resp.status_code == 409
        assert resp.json()["error"]["code"] == "NO_OPEN_DRAIN"


def test_aborted_capacity_is_reusable_by_other_drains(tmp_path):
    client = make_client(tmp_path)
    payload = topology(
        nodes=[
            {"id": "n1", "cpu_capacity": 8},
            {"id": "n2", "cpu_capacity": 8},
            {"id": "n3", "cpu_capacity": 8},
        ],
        containers=[
            {"id": "c1", "service": "a", "min_ready": 1, "node_id": "n1", "cpu_request": 4},
            {"id": "c2", "service": "b", "min_ready": 1, "node_id": "n3", "cpu_request": 6},
        ],
    )
    with client:
        client.post("/api/topology", json=payload)
        state = client.post("/api/nodes/n1/drain").json()
        assert state["open_drains"][0]["pending_decisions"][0]["target_node_id"] == "n2"
        # n2 now holds a 4-CPU reservation, leaving 4 free: c2 (6 CPU) is
        # blocked because n1 is draining and n2 is too small.
        state = client.post("/api/nodes/n3/drain").json()
        assert {b["container_id"] for b in state["blocked_containers"]} == {"c2"}
        assert state["open_drains"][1]["status"] == "incomplete"

        # Aborting n1's drain releases the reservation; n3's drain can then
        # reserve that exact capacity on retry.
        state = client.post("/api/nodes/n1/abort").json()
        n2 = next(n for n in state["nodes"] if n["id"] == "n2")
        assert n2["reserved_cpu"] == 0
        assert n2["remaining_cpu"] == 8
        state = client.post("/api/nodes/n3/resume").json()
        n3_drain = next(d for d in state["open_drains"] if d["node_id"] == "n3")
        assert n3_drain["pending_decisions"][0]["target_node_id"] == "n2"
        n2 = next(n for n in state["nodes"] if n["id"] == "n2")
        assert n2["reserved_cpu"] == 6


def test_redrain_after_abort_reschedules_from_current_placement(tmp_path):
    client = make_client(tmp_path)
    with client:
        client.post("/api/topology", json=two_replica_payload())
        state = client.post("/api/nodes/n1/drain").json()
        pending = state["open_drains"][0]["pending_decisions"]
        first = pending[0]
        client.post(f"/api/migrations/{first['id']}/resolve", json={"outcome": "ready"})
        moved_id = first["container_id"]
        client.post("/api/nodes/n1/abort")

        # Draining the source again only schedules what is still there.
        state = client.post("/api/nodes/n1/drain").json()
        assert len(state["open_drains"]) == 1
        new_drain = state["open_drains"][0]
        assert new_drain["total"] == 1
        assert len(new_drain["pending_decisions"]) == 1
        attempt = new_drain["pending_decisions"][0]
        assert attempt["container_id"] != moved_id
        assert attempt["source_node_id"] == "n1"
        assert attempt["target_node_id"] != "n1"

        state = client.post(
            f"/api/migrations/{attempt['id']}/resolve", json={"outcome": "ready"}
        ).json()
        n1 = next(n for n in state["nodes"] if n["id"] == "n1")
        assert n1["is_empty"] is True
        by_id = {d["id"]: d for d in state["drains"]}
        assert by_id[new_drain["id"]]["status"] == "completed"
        # The aborted drain's frozen record is untouched by the new drain.
        aborted = next(d for d in state["drains"] if d["status"] == "cancelled")
        assert aborted["moved"] == 1
        assert aborted["cancelled"] == 1


def test_abort_persists_across_restart(tmp_path):
    db = tmp_path / "abort-restart.db"
    first = TestClient(create_app(db))
    with first:
        first.post("/api/topology", json=two_replica_payload())
        state = first.post("/api/nodes/n1/drain").json()
        pending = state["open_drains"][0]["pending_decisions"]
        first.post(
            f"/api/migrations/{pending[0]['id']}/resolve", json={"outcome": "ready"}
        )
        state = first.post("/api/nodes/n1/abort").json()
        drain_id = state["drains"][0]["id"]

    second = TestClient(create_app(db))
    with second:
        state = second.get("/api/state").json()
        assert state["open_drains"] == []
        drain = next(d for d in state["drains"] if d["id"] == drain_id)
        assert drain["status"] == "cancelled"
        assert drain["moved"] == 1
        assert drain["cancelled"] == 1
        states = {a["container_id"]: a["state"] for a in drain["attempts"]}
        assert sorted(states.values()) == ["cancelled", "completed"]
        n1 = next(n for n in state["nodes"] if n["id"] == "n1")
        assert n1["status"] == "ready"
        assert n1["serving_containers"] == 1
        # The per-drain endpoint serves the same persisted record.
        detail = second.get(f"/api/drains/{drain_id}").json()
        assert detail["status"] == "cancelled"
        assert detail["cancelled"] == 1


def test_legacy_database_is_migrated_to_support_cancelled(tmp_path):
    """Databases written before 'cancelled' existed are rebuilt in place."""
    import sqlite3

    db = tmp_path / "legacy.db"
    legacy_schema = """
    CREATE TABLE nodes (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        cpu_capacity INTEGER NOT NULL CHECK (cpu_capacity > 0),
        status TEXT NOT NULL DEFAULT 'ready'
            CHECK (status IN ('ready', 'draining', 'drained')),
        position INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE services (
        name TEXT PRIMARY KEY,
        min_ready INTEGER NOT NULL CHECK (min_ready >= 0),
        position INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE containers (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        service TEXT NOT NULL REFERENCES services(name),
        cpu_request INTEGER NOT NULL CHECK (cpu_request > 0),
        node_id TEXT NOT NULL REFERENCES nodes(id),
        status TEXT NOT NULL DEFAULT 'serving' CHECK (status IN ('serving')),
        position INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE drains (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        node_id TEXT NOT NULL REFERENCES nodes(id),
        status TEXT NOT NULL
            CHECK (status IN ('active', 'incomplete', 'completed')),
        created_at TEXT NOT NULL,
        finished_at TEXT
    );
    CREATE TABLE migrations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        drain_id INTEGER NOT NULL REFERENCES drains(id) ON DELETE CASCADE,
        container_id TEXT NOT NULL REFERENCES containers(id),
        source_node_id TEXT NOT NULL REFERENCES nodes(id),
        target_node_id TEXT REFERENCES nodes(id),
        state TEXT NOT NULL
            CHECK (state IN ('starting', 'completed', 'failed', 'blocked')),
        reserved_cpu INTEGER NOT NULL CHECK (reserved_cpu > 0),
        error_code TEXT,
        error TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    CREATE UNIQUE INDEX idx_one_open_drain_per_node
        ON drains(node_id) WHERE status IN ('active', 'incomplete');
    CREATE UNIQUE INDEX idx_one_starting_migration_per_container
        ON migrations(container_id) WHERE state = 'starting';
    """
    conn = sqlite3.connect(db)
    conn.executescript(legacy_schema)
    conn.execute("INSERT INTO nodes VALUES ('n1', 'one', 8, 'draining', 0)")
    conn.execute("INSERT INTO nodes VALUES ('n2', 'two', 8, 'ready', 1)")
    conn.execute("INSERT INTO services VALUES ('svc', 1, 0)")
    conn.execute("INSERT INTO containers VALUES ('c1', 'c1', 'svc', 2, 'n1', 'serving', 0)")
    conn.execute("INSERT INTO drains (node_id, status, created_at) VALUES ('n1', 'active', '2026-01-01T00:00:00+00:00')")
    conn.execute(
        "INSERT INTO migrations (drain_id, container_id, source_node_id, target_node_id, "
        "state, reserved_cpu, created_at, updated_at) "
        "VALUES (1, 'c1', 'n1', 'n2', 'starting', 2, '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')"
    )
    conn.commit()
    conn.close()

    client = TestClient(create_app(db))
    with client:
        # Rows survived the rebuild, including the in-flight reservation.
        state = client.get("/api/state").json()
        assert state["open_drains"][0]["status"] == "active"
        n2 = next(n for n in state["nodes"] if n["id"] == "n2")
        assert n2["reserved_cpu"] == 2
        # The widened CHECK constraints accept the abort transition.
        state = client.post("/api/nodes/n1/abort").json()
        assert state["drains"][0]["status"] == "cancelled"
        assert state["drains"][0]["cancelled"] == 1
        n1 = next(n for n in state["nodes"] if n["id"] == "n1")
        assert n1["status"] == "ready"

    # And the migrated database keeps working across another restart.
    reopened = TestClient(create_app(db))
    with reopened:
        state = reopened.get("/api/state").json()
        assert state["drains"][0]["status"] == "cancelled"
