import inspect
from collections.abc import Callable

class ClassPropertyDescriptor():
    '''Descriptor class for creating class properties

    Args:
     - fget: Function to get the value
     - fset: Function to set the value
    '''
    def __init__(self, fget: classmethod, fset: classmethod=None):
        # Store getter and setter as attributes
        self.fget = fget
        self.fset = fset

    def __get__(self, instance, owner=None):
        # This gets called when an attribute with this type is accessed from an instance
        if owner is None:
            owner = type(instance)
        # Call the stored getter function
        return self.fget.__get__(instance, owner)()

    def __set__(self, instance, value):
        # This gets called when an attribute with this type is set from an instance
        if not self.fset:
            raise AttributeError("can't set attribute")
        if inspect.isclass(instance):
            type_ = instance
            instance = None
        else:
            type_ = type(instance)
        # Call the stored setter function by passing value to it
        return self.fset.__get__(instance, type_)(value)

    def setter(self, func: Callable|classmethod):
        '''Decorator to associate a setter with a getter function
        '''
        if not isinstance(func, (classmethod, staticmethod)):
            func = classmethod(func)
        self.fset = func
        return self  # Return this descriptor


def classproperty(func: Callable|classmethod):
    '''Decorator for creating class properties
    '''
    if not isinstance(func, (classmethod, staticmethod)):
        func = classmethod(func)

    return ClassPropertyDescriptor(func)  # Return a new descriptor


class ClassPropertyMetaClass(type):
    '''Metaclass for using class properties
    '''
    def __setattr__(cls, key, value):
        obj = None
        if key in cls.__dict__:
            obj = cls.__dict__.get(key)
        if obj and type(obj) is ClassPropertyDescriptor:
            # If the attribute is a class property call the __set__ method explicitly
            # intead of __setattr__
            return obj.__set__(cls, value)

        return super().__setattr__(key, value)