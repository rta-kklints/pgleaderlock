import logging

from pgleaderlock._logging import NDLogger, get_logger


class TestNDLogger:
    def test_format_simple(self):
        log = logging.getLogger("test_nd")
        nd = NDLogger(log)
        msg = nd._format("event_name", {"key": "val"})
        assert "event_name" in msg
        assert "key=val" in msg

    def test_format_no_extra(self):
        nd = get_logger("test_fmt")
        msg = nd._format("ev", {})
        assert "ev" in msg

    def test_bind_creates_new(self):
        nd = get_logger("test")
        bound = nd.bind(service="foo")
        assert bound is not nd
        msg = bound._format("ev", {})
        assert "service=foo" in msg

    def test_bind_merges_context(self):
        nd = get_logger("test").bind(a=1)
        bound = nd.bind(b=2)
        msg = bound._format("ev", {})
        assert "a=1" in msg
        assert "b=2" in msg

    def test_bind_does_not_mutate_parent(self):
        nd = get_logger("test_immutable").bind(x=1)
        nd.bind(y=2)
        msg = nd._format("ev", {})
        assert "x=1" in msg
        assert "y=2" not in msg

    def test_logging_methods(self, caplog):
        log = logging.getLogger("test_methods")
        log.setLevel(logging.DEBUG)
        nd = NDLogger(log)
        with caplog.at_level(logging.DEBUG, logger="test_methods"):
            nd.debug("debug_event", x=1)
            nd.info("info_event", x=2)
            nd.warning("warn_event", x=3)
            nd.error("error_event", x=4)
        assert any("debug_event" in r.message for r in caplog.records)
        assert any("info_event" in r.message for r in caplog.records)
        assert any("warn_event" in r.message for r in caplog.records)
        assert any("error_event" in r.message for r in caplog.records)

    def test_exception_method(self, caplog):
        log = logging.getLogger("test_exception")
        log.setLevel(logging.DEBUG)
        nd = NDLogger(log)
        with caplog.at_level(logging.ERROR, logger="test_exception"):
            try:
                raise ValueError("boom")
            except ValueError:
                nd.exception("exc_event", detail="test")
        assert any("exc_event" in r.message for r in caplog.records)

    def test_get_logger_returns_ndlogger(self):
        nd = get_logger("mylogger")
        assert isinstance(nd, NDLogger)

    def test_get_logger_default_name(self):
        nd = get_logger()
        assert isinstance(nd, NDLogger)
