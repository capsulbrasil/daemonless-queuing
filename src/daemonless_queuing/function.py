import typing
import importlib
import re
from datetime import datetime

TYPE_WHITELIST = [
    'str',
    'int',
    'float',
    'date',
    'bool'
]

def get_function(path: str):
    *_, func_name = path.split('.')
    module_name = '.'.join(_)
    module = importlib.import_module(module_name)
    return getattr(module, func_name)


def parse_params_string(ps: str):
    ps = re.sub(r'"([^",]*),([^"]*)"', r'"\1<<comma>>\2"', ps)
    args: list[typing.Any] = []
    kwargs: dict[str, typing.Any] = {}

    for param in ps.split(','):
        param = param.strip()
        if not param:
            continue

        key = None
        param = param.replace('<<comma>>', ',')
        param = re.sub(r'(^"|"$)', '', param)

        if re.match(r'^([^=]+)=', param):
            key, param = param.split('=')[0], '='.join(param.split('=')[1:])

        if re.match(r'^([^:]+):', param):
            t, value = param.split(':')[0], ':'.join(param.split(':')[1:])
            if t not in TYPE_WHITELIST:
                raise TypeError('illegal type: {}'.format(t))

            if t == 'date':
                value = value.split('T')[0]
                segments = [int(seg) for seg in value.split('-')]
                param = datetime(*segments) # type: ignore

            elif t == 'bool':
                param = True if value == 'true' \
                    else False

            else:
                param = eval(t)(value)

        if key:
            kwargs[key] = param
        else:
            args += [param]

    return args, kwargs
