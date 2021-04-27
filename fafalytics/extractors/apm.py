from .base import Extractor

class Milliseconds(int):
    @property
    def in_minutes(self):
        return self/60/1000
    @classmethod
    def from_minutes(cls, minutes):
        return cls(minutes*60*1000)
    def per_minute_or_none(self, value):
        if not self or value is None:
            return None
        return value/self.in_minutes
Minute = Milliseconds.from_minutes

class APM(Extractor):
    ACTIONS = frozenset(('issue', 'command_count_increase', 'command_count_decrease', 'factory_issue'))
    def __init__(self, thresholds):
        self.actions = {0: {'overall': 0}, 1: {'overall': 0}}
        self.initial_thresholds = thresholds
        self.thresholds = thresholds.copy()
        self.last_offset = Milliseconds(0)
    def feed(self, command):
        if command['type'] not in self.ACTIONS:
            return
        self.actions[command['player']]['overall'] += 1
        self.last_offset = Milliseconds(command['offset_ms'])
        for threshold, label in tuple(self.thresholds.items()):
            if self.last_offset < threshold:
                continue
            for player in self.actions:
                self.actions[player][label] = self.actions[player]['overall']
            self.thresholds.pop(threshold)
    def emit(self):
        def pkey(player, label):
            return 'player%d.mean_apm.%s' % (player, label)
        results = {}
        for player, stats in self.actions.items():
            results[pkey(player, 'overall')] = self.last_offset.per_minute_or_none(stats['overall'])
            for threshold, label in self.initial_thresholds.items():
                results[pkey(player, label)] = threshold.per_minute_or_none(stats.get(label))
        return results
