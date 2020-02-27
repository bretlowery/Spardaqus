class SpardaqusTimeout(UserWarning):
    pass


class SpardaqusWaitExpired(UserWarning):
    pass


class SpardaqusMissingSparkSQLStructMetadata(LookupError):
    pass


class SpardaqusUnknownSparkSQLStruct(TypeError):
    pass


class SpardaqusSparkSQLStructParseError(ValueError):
    pass


class SpardaqusSparkSQLStructMissingRequiredElementsError(ValueError):
    pass


class SpardaqusMessageParseError(ValueError):
    pass