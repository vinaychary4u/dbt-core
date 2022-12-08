from dbt.events.functions import warn_or_error
from dbt.events.types import FunctionDeprecated

# Work on deprecation decorator

# string_types = (type(b''), type(u''))

# def deprecated(*args, **kwargs):
#     """
#     This is a decorator which can be used to mark functions
#     as deprecated. It will result in a warning being emitted
#     when the function is used.
#     **Classic usage:**
#     To use this, decorate your deprecated function with **@deprecated** decorator:
#     .. code-block:: python
#        from deprecated import deprecated
#        @deprecated
#        def some_old_function(x, y):
#            return x + y
#     You can also decorate a class or a method:
#     .. code-block:: python
#        from deprecated import deprecated
#        class SomeClass(object):
#            @deprecated
#            def some_old_method(self, x, y):
#                return x + y
#        @deprecated
#        class SomeOldClass(object):
#            pass
#     You can give a *reason* message to help the developer to choose another function/class,
#     and a *version* number to specify the starting version number of the deprecation.
#     .. code-block:: python
#        from deprecated import deprecated
#        @deprecated(reason="use another function", version='1.2.0')
#        def some_old_function(x, y):
#            return x + y
#     The *category* keyword argument allow you to specify the deprecation warning class of your choice.
#     By default, :exc:`DeprecationWarning` is used but you can choose :exc:`FutureWarning`,
#     :exc:`PendingDeprecationWarning` or a custom subclass.
#     .. code-block:: python
#        from deprecated import deprecated
#        @deprecated(category=PendingDeprecationWarning)
#        def some_old_function(x, y):
#            return x + y
#     The *action* keyword argument allow you to locally change the warning filtering.
#     *action* can be one of "error", "ignore", "always", "default", "module", or "once".
#     If ``None``, empty or missing, the the global filtering mechanism is used.
#     See: `The Warnings Filter`_ in the Python documentation.
#     .. code-block:: python
#        from deprecated import deprecated
#        @deprecated(action="error")
#        def some_old_function(x, y):
#            return x + y
#     """
#     if args and isinstance(args[0], string_types):
#         kwargs['reason'] = args[0]
#         args = args[1:]

#     if args and not callable(args[0]):
#         raise TypeError(repr(type(args[0])))

#     if args:
#         reason = kwargs.get('reason')
#         version = kwargs.get('version')
#         category = kwargs.get('category', DeprecationWarning)
#         action = kwargs.get('action')

#         wrapped = args[0]
#         if inspect.isclass(wrapped):
#             wrapped = adapter(wrapped)
#             return wrapped

#         elif inspect.isroutine(wrapped):

#             @wrapt.decorator(adapter=adapter)
#             def wrapper_function(wrapped_, instance_, args_, kwargs_):
#                 msg = adapter.get_deprecated_msg(wrapped_, instance_)
#                 if action:
#                     with warnings.catch_warnings():
#                         warnings.simplefilter(action, category)
#                         warnings.warn(msg, category=category, stacklevel=_routine_stacklevel)
#                 else:
#                     warnings.warn(msg, category=category, stacklevel=_routine_stacklevel)
#                 return wrapped_(*args_, **kwargs_)

#             return wrapper_function(wrapped)

#         else:
#             raise TypeError(repr(type(wrapped)))

#     return functools.partial(deprecated, **kwargs)


# def deprecated(func):

#     def wrapper_func(*args, **kwargs):
#         if args:
#             reason = kwargs.get('reason')
#             version = kwargs.get('version')
#             category = kwargs.get('category', DeprecationWarning)
#             action = kwargs.get('action')

#         func(*args, **kwargs)
#         # Do something after the function.
#     return wrapper_func


def deprecated(reason="", version="", suggested_action=""):
    print("Inside decorator")

    def inner(func):

        # code functionality here
        function_name = func.__name__
        warn_or_error(
            FunctionDeprecated(function_name, reason, suggested_action, version)
        )  # TODO: pass in event?

        return func

    # returning inner function
    return inner
