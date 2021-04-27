from collections import Counter

from .base import ExtractByCommand
from ..parsing import ISSUE_TYPE_ID_TO_NAME

from replay_parser.constants import ActionType

class CommandMix(ExtractByCommand):
    "A feature extractor focused on commands issued. 'How much reclaim', etc"
    # these are the top-9 commands as found by teolicy through sampling 1,000
    # 1v1 ladder games played between Jan and Mar 2021
    # see: https://faforever.zulipchat.com/#narrow/stream/203478-general/topic/Bulk.20data.20access/near/236097943
    TOP_COMMANDS = frozenset((
        ActionType.Move, ActionType.BuildMobile, ActionType.BuildFactory, ActionType.Reclaim,
        ActionType.Attack, ActionType.Guard, ActionType.AggressiveMove, ActionType.Patrol,
        ActionType.Upgrade))
    def __init__(self):
        self.result = {0: {'first_5m': Counter(), 'overall': Counter()},
                       1: {'first_5m': Counter(), 'overall': Counter()}}
    def issue(self, command):
        command_type = command['cmd_data']['command_type']
        if command_type not in self.TOP_COMMANDS:
            command_type = -1
        self.result[command['player']]['overall'][command_type] += 1
        if command['offset_ms'] < 5*60*1000:
            self.result[command['player']]['first_5m'][command_type] += 1
    def emit(self):
        result = {}
        for player, mode_to_counter in self.result.items():
            player_obj = result['player%d.command_ratio' % (player+1)] = {}
            for mode, counter in mode_to_counter.items():
                mode_obj = player_obj[mode] = {}
                total = mode_obj['Total'] = sum(counter.values())
                for command_type, count in counter.items():
                    ratio = round(count/total, 4)
                    mode_obj[ISSUE_TYPE_ID_TO_NAME[command_type]] = ratio
        return result
