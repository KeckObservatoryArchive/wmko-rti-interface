import json
import requests


class ObserverLogs:
    def __init__(self, first, last):
        self.first = first
        self.last = last
        self.db = db
