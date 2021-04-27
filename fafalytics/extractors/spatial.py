from shapely.geometry import Polygon, box

from .base import ExtractByCommand

class Spatial(ExtractByCommand):
    "A feature extractor focused on spatial data (command coordinates on the map)"
    def __init__(self, width, height):
        self.area = box(0, 0, width, height).area
        self.result = {0: {'first': {'1m': None, '3m': None, '5m': None}, 'overall': None},
                       1: {'first': {'1m': None, '3m': None, '5m': None}, 'overall': None}}
        self.coordinates = {0: [], 1: []}
        self.minutes = {'1m': 1000*60, '3m': 1000*60*3, '5m': 1000*60*5}
    def extract_coordinates(self, command):
        # coordinates are ordered x (horizontal), z (altitude), y (veritcal)
        return ((command['cmd_data']['target']['position'] or (None, None, None))[0],
                (command['cmd_data']['target']['position'] or (None, None, None))[2])
    def hull_coverage(self, coordinates):
        "Ratio of the area of the convex hull for the given coordinates relative to the entire map"
        return Polygon(coordinates).convex_hull.area/self.area
    def issue(self, command):
        coordinates = self.extract_coordinates(command)
        if not all(coordinates):
            return
        player = command['player']
        for label, milliseconds in tuple(self.minutes.items()):
            if command['offset_ms'] < milliseconds:
                continue
            if self.result[player]['first'][label] is not None:
                continue
            if len(self.coordinates[player]) < 3:
                return
            self.result[player]['first'][label] = self.hull_coverage(self.coordinates[player])
            if self.result[(player+1)%2]['first'][label] is not None:
                self.minutes.pop(label)
        self.coordinates[player].append(coordinates)
    def emit(self):
        result = {}
        for player, player_results in self.result.items():
            result['player%d.command_area' % (player+1)] = player_results
            if len(self.coordinates[player])<3:
                player_results['overall'] = None
                continue
            player_results['overall'] = self.hull_coverage(self.coordinates[player])
        return result
