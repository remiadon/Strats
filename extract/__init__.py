
def execute_and_pass(getter):
    def decorator(fn):
        def _inner(*args, **kwargs):
            extras = getter()
            return fn(*args, **kwargs, **extras)
        return _inner
    return decorator
