"""Smaller parts: settings, topics, kill switch watcher, metrics endpoint, model service stub."""
import json
import urllib.error
import urllib.request

import pytest
from tests.unit.fakes import FakeBroker, FakeConsumer, FakeProducer

from safety_gate import config
from safety_gate.config import Settings, Topics
from safety_gate.control import SwitchUnknown, SwitchWatcher
from safety_gate.metrics import GateMetrics, serve
from safety_gate.model_stub import ModelService
from safety_gate.topology import topic_specs


# --- task 13: settings are passed in ---------------------------------------------
def test_importing_config_reads_nothing_from_the_environment():
    assert not any(name.isupper() and name.endswith("_TOPIC") for name in vars(config))


def test_settings_come_from_the_environment_only_when_asked():
    settings = Settings.from_env({
        "KAFKA_BOOTSTRAP": "broker:9093", "RAW_TOPIC": "my.raw", "GATE_BATCH_SIZE": "25",
        "GATE_SWITCH_CHECK_MS": "500", "GATE_DEDUP": "off", "METRICS_PORT": "9100",
        "KAFKA_SECURITY_PROTOCOL": "SASL_SSL", "KAFKA_SASL_USERNAME": "gate", "KAFKA_SASL_PASSWORD": "pw",
    })
    assert settings.bootstrap == "broker:9093" and settings.topics.raw == "my.raw"
    assert settings.batch_size == 25 and settings.switch_check_s == 0.5
    assert settings.dedup_enabled is False and settings.metrics_port == 9100
    assert settings.client_config() == {"bootstrap.servers": "broker:9093", "security.protocol": "SASL_SSL",
                                        "sasl.username": "gate", "sasl.password": "pw"}


def test_a_typo_in_a_number_is_reported_clearly():
    with pytest.raises(ValueError, match="GATE_BATCH_SIZE must be a number"):
        Settings.from_env({"GATE_BATCH_SIZE": "many"})


def test_each_test_can_have_its_own_topics():
    a, b = Topics().with_prefix("test-a"), Topics().with_prefix("test-b")
    assert a.raw == "test-a.raw.inference.requests.v1"
    assert set(vars(a).values()).isdisjoint(vars(b).values())


def test_two_settings_objects_get_different_instance_ids():
    assert Settings().instance_id != Settings().instance_id


# --- task 3: retention --------------------------------------------------------------
def test_topics_that_hold_prompts_are_deleted_after_a_fixed_time():
    specs = {s.name: s for s in topic_specs(Topics())}
    topics = Topics()
    assert specs[topics.raw].config["retention.ms"] == str(24 * 3600 * 1000)        # 1 day
    assert specs[topics.dlq].config["retention.ms"] == str(7 * 24 * 3600 * 1000)    # 7 days
    assert specs[topics.audit].config["retention.ms"] == "-1"                        # hashes only: kept
    assert specs[topics.audit].partitions == 1
    assert specs[topics.control].config["cleanup.policy"] == "compact"
    assert specs[topics.dedup].partitions == specs[topics.raw].partitions


# --- kill switch watcher ------------------------------------------------------------------
def command(broker, topic, mode, reason=""):
    broker.append(topic, key="gate", value=json.dumps({"mode": mode, "reason": reason}))


def test_watcher_rebuilds_the_switch_from_the_whole_topic():
    broker = FakeBroker()
    command(broker, "control", "DISABLED", "incident")
    watcher = SwitchWatcher(FakeConsumer(broker, eof=True), "control")
    assert watcher.refresh().disabled and watcher.switch.reason == "incident"


def test_watcher_fails_closed_when_the_topic_does_not_answer():
    consumer = FakeConsumer(FakeBroker(), eof=True)
    consumer.fail_watermarks = True
    with pytest.raises(SwitchUnknown):
        SwitchWatcher(consumer, "control").refresh()


def test_garbage_on_the_control_topic_does_not_flip_the_switch():
    broker = FakeBroker()
    command(broker, "control", "DISABLED")
    broker.append("control", key="gate", value=b"\xff not json")
    assert SwitchWatcher(FakeConsumer(broker, eof=True), "control").refresh().disabled


# --- task 9: metrics and health --------------------------------------------------------------
def get(port, path):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()


def test_metrics_and_health_are_served_over_http():
    metrics = GateMetrics()
    server = serve(0, metrics, host="127.0.0.1")
    port = server.server_address[1]
    try:
        assert get(port, "/healthz")[0] == 503                   # not running yet
        metrics.heartbeat()
        metrics.record_decision("BLOCK", ("pii",), 0.001)

        status, body = get(port, "/healthz")
        assert status == 200 and json.loads(body)["status"] == "ok"
        status, body = get(port, "/metrics")
        assert status == 200
        assert 'gate_blocked_total{reason="pii"} 1.0' in body
        assert "gate_decision_seconds_bucket" in body and "gate_consumer_lag" in body
        assert get(port, "/nothing")[0] == 404

        metrics.stopped()
        assert get(port, "/healthz")[0] == 503
    finally:
        server.shutdown()


def test_health_turns_bad_when_the_loop_is_stuck():
    metrics = GateMetrics()
    metrics.heartbeat()
    assert metrics.health(max_silence_s=30)[0] is True
    assert metrics.health(max_silence_s=0)[0] is False


# --- task 19: the model service honours the kill switch ----------------------------------------
def make_service(broker, calls):
    settings = Settings()
    def model(request):
        calls.append(request["request_id"])
        return "an answer"
    service = ModelService(settings, FakeConsumer(broker, "model"), FakeProducer(broker),
                           SwitchWatcher(FakeConsumer(broker, eof=True), settings.topics.control), model)
    service.start()
    return service, settings.topics


def test_model_is_called_for_a_clean_request():
    broker, calls = FakeBroker(), []
    service, topics = make_service(broker, calls)
    broker.append(topics.clean, key="r-1", value=json.dumps({"request_id": "r-1", "prompt": "hi"}))
    service.step()

    assert calls == ["r-1"]
    assert json.loads(broker.values(topics.responses)[0]) == {"request_id": "r-1", "status": "OK", "answer": "an answer"}


def test_request_already_in_the_clean_topic_is_not_sent_to_the_model_after_the_switch_is_pulled():
    broker, calls = FakeBroker(), []
    service, topics = make_service(broker, calls)
    broker.append(topics.clean, key="r-1", value=json.dumps({"request_id": "r-1", "prompt": "hi"}))   # passed the gate
    command(broker, topics.control, "DISABLED", "incident")                                          # then: switch
    service.step()

    assert calls == []                                           # the model was never called
    response = json.loads(broker.values(topics.responses)[0])
    assert response["status"] == "REJECTED" and response["reason"] == "kill_switch"

    command(broker, topics.control, "ENABLED")
    broker.append(topics.clean, key="r-2", value=json.dumps({"request_id": "r-2", "prompt": "hi"}))
    service.step()
    assert calls == ["r-2"]
