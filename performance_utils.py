# -*- coding: utf-8 -*-
# @Time    : 2023/11/8 20:48
# @Author  : changbodeng
# @Email   : changbodeng@tencent.com
# @File    : performance_utils.py

from functools import wraps
from time import time


def time_cost(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        pre_time = time()
        return_value = func(*args, **kwargs)
        post_time = time()
        print("function %s() execute cost: %s s" % (func.__name__, post_time - pre_time))
        return return_value

    return wrapper
