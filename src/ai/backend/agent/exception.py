class InitializationError(Exception):
    pass


class InsufficientResource(Exception):
    pass

class NoKernelError(Exception):
    pass

class PVCError(Exception):
    def __init__(self, message):
        super.__init__(self, message)
        self.message = message