class CrabException(Exception):
    def __init__(self, msg):
        Exception.__init__(self, msg)
        self._msg = msg
        return

    def __str__(self):
        return self._msg

class DBException(Exception):
    def __init__(self, msg):
        Exception.__init__(self, msg)
        self._msg = msg
        return

    def __str__(self):
        return self._msg
