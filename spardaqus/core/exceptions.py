class SpardaqusTimeout(UserWarning):
    pass


class SpardaqusWaitExpired(UserWarning):
    pass


class SpardaqusMissingSparkSQLStructMetadata(LookupError):
    pass


class SpardaqusUnknownSparkSQLStruct(TypeError):
    pass

