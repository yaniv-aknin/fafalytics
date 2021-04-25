from .base import Extractor
from ..units import id_by_categories as C

class TimeToFirst(Extractor):
    "A feature extractor focused on game timeline. 'Time to first T2 mexer', etc"
    FEATURES = {
        't1_factory': C('factory tech1'),
        't1_land': C('factory tech1 land'),
        't1_air': C('factory tech1 air'),
        't1_naval': C('factory tech1 naval'),
        't2_factory': C('factory tech2 -supportfactory'),
        't2_support': C('tech2 supportfactory'),
        't1_mexer': C('tech1 massextraction'),
        't2_mexer': C('tech2 massextraction'),
        't3_mexer': C('tech3 massextraction'),
    }
    def __init__(self):
        self.features = self.FEATURES.copy()
        self.result = {0: {k: None for k in self.FEATURES},
                       1: {k: None for k in self.FEATURES}}
    def feed(self, command):
        if not self.features:
            return
        if command['type'] != 'issue':
            return
        assert command['player'] in (0, 1)
        # casting offset_ms to int because the parser multiplied it by the
        # 'advance' arg of the 'Advance' command, and that was parsed as an
        # uncasted '1.0' float; I would change it only in parsing.py, but
        # then I'd have to re-parse all 500k game dataset, which sucks
        self.update(command['cmd_data']['blueprint_id'],
                    command['player'], int(command['offset_ms']))
    def update(self, unit_id, player, offset):
        for feature, unit_ids in tuple(self.features.items()):
            if unit_id not in unit_ids:
                continue
            if self.result[player][feature]:
                continue
            self.result[player][feature] = offset
            other_player = (player + 1) % 2
            if self.result[other_player][feature]:
                self.features.pop(feature)
    def emit(self):
        return {'player%d' % (player+1): {'first': features}
                for player, features in self.result.items()}
