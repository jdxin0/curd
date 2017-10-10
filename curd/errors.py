class Error(Exception):
    pass


class WrappedError(Error):
    BASE_MESSAGE = 'WrappedError'

    def __init__(self, *args, origin_error=None, **kwargs):
        self._origin_error = origin_error
        if self._origin_error:
            message = self.BASE_MESSAGE + ': ' + str(self._origin_error)
        else:
            message = self.BASE_MESSAGE
        super().__init__(message, *args, **kwargs)


class ConnectError(WrappedError):
    '''
    errors when connect to database
    '''
    
    BASE_MESSAGE = 'ConnectError'


class UnexpectedError(WrappedError):
    '''
    uncategorized errors
    '''
    
    BASE_MESSAGE = 'UnexpectedError'


class OperationFailure(WrappedError):
    '''
    errors like timeout, mysql gone away, retriable
    '''
    
    BASE_MESSAGE = 'OperationFailure'


class ProgrammingError(WrappedError):
    BASE_MESSAGE = 'ProgrammingError'


class DuplicateKeyError(Error):
    pass
