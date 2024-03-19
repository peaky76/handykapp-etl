class IdHook:

    def __init__(self):
        self.val = ''

    def __call__(self, val: str):
        self.val = val