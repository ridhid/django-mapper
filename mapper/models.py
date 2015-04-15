from tests.utils import is_test_env

if is_test_env():
    from tests.models import *