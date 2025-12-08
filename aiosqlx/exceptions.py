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
    def __init__(self, message: str, *, is_open: bool) -> None:
        super().__init__(message)
        self.is_open = is_open


