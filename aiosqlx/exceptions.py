class AiosqlxError(Exception):
    """
    The base error for the library
    """


class AsyncDatabaseError(AiosqlxError):
    """
    Base error for anything in the database
    module
    """


class DatabaseInitializationError(AsyncDatabaseError):
    """
    Error raised when the database has not been
    initialized properly.
    """
