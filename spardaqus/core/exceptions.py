class SpardaqusTimeout(UserWarning):
    pass


class SpardaqusWaitExpired(UserWarning):
    pass


class SpardaqusMissingSparkSQLStructMetadata(LookupError):
    pass


class SpardaqusSparkSQLStructParseError(TypeError):
    pass


class SpardaqusSparkSQLStructMissingRequiredElementsError(TypeError):
    pass