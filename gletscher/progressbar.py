import time
from collections import deque

class ProgressBar(object):

    def __init__(self, title, max, formatter=str, bar_width=50):
        self._title = title
        self._current = 0
        self._max = max
        self._formatter = formatter
        self._bar_width = bar_width
        self._speed_data = deque(maxlen=10)

    def set_progress(self, progress):
        if progress > self._max:
            raise Exception(
                "can't set progress to %d (max is %d)" % (progress, self._max))
        self._current = progress
        self._speed_data.append((time.time(), progress))

    def print(self):
        speed = 0
        if len(self._speed_data) >= 2:
            time_delta = self._speed_data[-1][0] - self._speed_data[0][0]
            progress_delta = self._speed_data[-1][1] - self._speed_data[0][1]
            speed = progress_delta / time_delta

        completed = self._bar_width * self._current // self._max
        bar = "[%s%s]" % ("=" * completed, " " * (self._bar_width - completed))
        print("%s %s %s / %s (%s/s)" % (
                self._title,
                bar,
                self._formatter(self._current),
                self._formatter(self._max),
                self._formatter(speed)),
            end="\r")

    def complete(self):
        print(" ")

