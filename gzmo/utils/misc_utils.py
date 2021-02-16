import importlib

def _str_to_fn(self, fn_as_str):
    """
    If the argument is not a string, return whatever was passed in.
    Parses a string such as package.module.function, imports the module
    and returns the function.
    :param fn_as_str: The string to parse. If not a string, return it.
    """
    if not isinstance(fn_as_str, str):
        return fn_as_str

    path, _, function = fn_as_str.rpartition('.')
    module = importlib.import_module(path)
    return getattr(module, function)
