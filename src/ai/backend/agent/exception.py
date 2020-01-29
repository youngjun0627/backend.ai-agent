class InitializationError(Exception):
    pass


class UnsupportedResource(Exception):
    pass


class InsufficientResource(Exception):
    pass


class K8sError(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message


class StorageAgentError(RuntimeError):
    '''
    A dummy exception class to distinguish agent-side errors passed via
    agent rpc calls.

    It carries two args tuple: the exception type and exception arguments from
    the agent.
    '''

    def __init__(self, *args, exc_repr: str = None):
        super().__init__(*args)
        self.exc_repr = exc_repr

