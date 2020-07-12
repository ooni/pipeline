"""
OONI Fastpath

Contextual logger
"""

from collections import deque
from logging import getLogger
import logging
import logging.handlers


try:
    from systemd.journal import JournalHandler  # debdeps: python3-systemd

    no_journal_handler = False
except ImportError:
    # this will be the case on macOS for example
    no_journal_handler = True


class SmartHandler(logging.Handler):
    """Thread-safe and efficient circular buffer
    All logs below flush_level are buffered temporarily. If log >= flush_level
    is received, all buffered logs are outputted.
    If a log >= output_level and < flush_level is received is outputted without
    flushing previous logs
    """

    BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(30, 38)
    RED_SEQ = f"\033[1;{RED}m"
    RESET = "\033[0m"

    def __init__(self, capacity, target, output_level, flush_level, devel):
        logging.Handler.__init__(self)
        self._buffer = deque(maxlen=capacity)
        self._flush_level = flush_level
        self._output_level = output_level
        self._target = target

    def emit(self, record):
        self._buffer.append(record)
        if record.levelno >= self._flush_level:
            # high-level log: flush buffer and also color this record
            record.levelname = self.RED_SEQ + record.levelname + self.RESET
            self.flush()

        elif record.levelno >= self._output_level:
            self._target.handle(record)

    def flush(self):
        try:
            # marker = logging.makeLogRecord(dict(msg="--log--"))
            # self._target.handle(marker)
            while True:
                r = self._buffer.popleft()
                self._target.handle(r)
        except IndexError:
            return

    def reset_context(self):
        self._buffer.clear()

    def close(self):
        # There a bug somewhere in the stdlib when the logger is always
        # flushed on close
        logging.Handler.close(self)


DEV_FMT = "%(relativeCreated)d %(process)d %(levelname)s %(name)s %(message)s"


def setup_logger(name: str, devel_format=DEV_FMT, devel=False) -> logging.Logger:
    """Logs to journald or stdout. Provides contextual/transactional logging
    """
    log = logging.getLogger(name)
    if devel:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(devel_format))

    else:
        handler = JournalHandler(SYSLOG_IDENTIFIER="fastpath")

    mh = SmartHandler(40, handler, logging.INFO, logging.ERROR, devel)
    log.mh = mh
    log.addHandler(mh)
    log.setLevel(logging.DEBUG)
    return log
