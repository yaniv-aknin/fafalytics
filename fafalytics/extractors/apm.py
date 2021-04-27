from .base import Extractor

class APM(Extractor):
    ACTIONS = frozenset(('issue', 'command_count_increase', 'command_count_decrease', 'factory_issue'))
    THREE_MINUTES_IN_MS = 3*60*1000
    FIVE_MINUTES_IN_MS = 5*60*1000
    def __init__(self):
        self.actions = {0: 0, 1: 0}
        self.actions_3m = None
        self.actions_5m = None
        self.last_offset = None
    @property
    def last_offset_in_minutes(self):
        if self.last_offset is None:
            raise ValueError('no command seen')
        return self.last_offset / 1000 / 60
    def feed(self, command):
        if command['type'] not in self.ACTIONS:
            return
        self.last_offset = int(command['offset_ms'])
        self.actions[command['player']] += 1
        if command['offset_ms'] > self.THREE_MINUTES_IN_MS and self.actions_3m is None:
            self.actions_3m = dict(self.actions)
        if command['offset_ms'] > self.FIVE_MINUTES_IN_MS and self.actions_5m is None:
            self.actions_5m = dict(self.actions)
    def emit(self):
        result = {
            'player1.mean_apm.overall': None,
            'player2.mean_apm.overall': None,
            'player1.mean_apm.first_3m': None,
            'player2.mean_apm.first_3m': None,
            'player1.mean_apm.first_5m': None,
            'player2.mean_apm.first_5m': None,
        }
        if self.last_offset:
            result.update({
                'player1.mean_apm.overall': self.actions[0] / self.last_offset_in_minutes,
                'player2.mean_apm.overall': self.actions[1] / self.last_offset_in_minutes,
            })
        if self.actions_3m is not None:
            result.update({
                'player1.mean_apm.first_3m': self.actions_3m[0] / 3,
                'player2.mean_apm.first_3m': self.actions_3m[1] / 3,
            })
        if self.actions_5m is not None:
            result.update({
                'player1.mean_apm.first_5m': self.actions_5m[0] / 5,
                'player2.mean_apm.first_5m': self.actions_5m[1] / 5,
            })
        return result
