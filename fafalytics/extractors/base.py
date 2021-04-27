class Extractor:
    def __str__(self):
        return self.__class__.__name__

class ExtractByCommand(Extractor):
    def feed(self, command):
        if not hasattr(self, command['type']):
            return
        getattr(self, command['type'])(command)
