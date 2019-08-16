class InitializationError(Exception):
    pass


class InsufficientResource(Exception):
    pass

class NoKernelError(Exception):
    pass

class K8sError(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message

class ScaleFailedError(Exception):
    pass

        