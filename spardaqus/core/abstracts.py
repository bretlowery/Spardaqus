from abc import ABCMeta as NativeABCMeta


def abstractattribute(obj=None):
    """Decorator used to identify an abstract attribute that must be implemented in any subclasses."""
    if obj is None:
        obj = DummyAttribute()
    obj.__is_abstract_attribute__ = True
    return obj


def abstractmethod(funcobj):
    """Decorator used to identify an abstract method that must be implemented in any subclasses."""
    funcobj.__isabstractmethod__ = True
    return funcobj


class DummyAttribute:
    pass


class ABCMeta(NativeABCMeta):
    """M<etaclass ancestor, supports abstract attributes."""
    def __call__(cls, *args, **kwargs):
        instance = NativeABCMeta.__call__(cls, *args, **kwargs)
        abstract_attributes = {
            name
            for name in dir(instance)
            if getattr(getattr(instance, name), '__is_abstract_attribute__', False)
        }
        if abstract_attributes:
            raise NotImplementedError(
                "Can't instantiate abstract class {} with"
                " abstract attributes: {}".format(
                    cls.__name__,
                    ', '.join(abstract_attributes)
                )
            )
        return instance

