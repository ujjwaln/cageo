__author__ = 'ujjwal'
import json
import datetime
from functools import update_wrapper
from flask import make_response, request, current_app


def new_alchemy_encoder():

        _visited_objs = []

        class AlchemyEncoder(json.JSONEncoder):
            def default(self, obj):
                #if isinstance(obj.__class__, DeclarativeMeta):
                    # don't re-visit self
                # if obj in _visited_objs:
                #     return None
                # _visited_objs.append(obj)

                if isinstance(obj, datetime.datetime):
                    return obj.isoformat()
                elif isinstance(obj, datetime.date):
                    return obj.isoformat()
                elif isinstance(obj, datetime.timedelta):
                    return (datetime.datetime.min + obj).time().isoformat()

                # an SQLAlchemy class
                fields = {}
                for field in [x for x in dir(obj) if not x.startswith('_') and x != 'metadata']:
                    try:
                        fields[field] = obj.__getattribute__(field)
                    except:
                        print "Exception in %s" % field

                # a json-encodable dict
                return fields

                #return json.JSONEncoder.default(self, obj)
        return AlchemyEncoder


def crossdomain(origin=None, methods=None, headers=None,
                max_age=21600, attach_to_all=True,
                automatic_options=True):
    if methods is not None:
        methods = ', '.join(sorted(x.upper() for x in methods))
    if headers is not None and not isinstance(headers, basestring):
        headers = ', '.join(x.upper() for x in headers)
    if not isinstance(origin, basestring):
        origin = ', '.join(origin)
    if isinstance(max_age, datetime.timedelta):
        max_age = max_age.total_seconds()

    def get_methods():
        if methods is not None:
            return methods

        options_resp = current_app.make_default_options_response()
        return options_resp.headers['allow']

    def decorator(f):
        def wrapped_function(*args, **kwargs):
            if automatic_options and request.method == 'OPTIONS':
                resp = current_app.make_default_options_response()
            else:
                resp = make_response(f(*args, **kwargs))
            if not attach_to_all and request.method != 'OPTIONS':
                return resp

            h = resp.headers

            h['Access-Control-Allow-Origin'] = origin
            h['Access-Control-Allow-Methods'] = get_methods()
            h['Access-Control-Max-Age'] = str(max_age)
            if headers is not None:
                h['Access-Control-Allow-Headers'] = headers
            return resp

        f.provide_automatic_options = False
        return update_wrapper(wrapped_function, f)
    return decorator


def date_handler(obj):
    return obj.isoformat() if hasattr(obj, 'isoformat') else obj
