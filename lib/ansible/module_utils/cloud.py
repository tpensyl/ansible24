#
# (c) 2016 Allen Sanabria, <asanabria@linuxdynasty.org>
#
# This file is part of Ansible
#
# Ansible is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Ansible is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Ansible.  If not, see <http://www.gnu.org/licenses/>.
#
"""
This module adds shared support for generic cloud modules

In order to use this module, include it as part of a custom
module as shown below.

from ansible.module_utils.cloud import *

The 'cloud' module provides the following common classes and functions:

    * CloudRetry
        - The base class to be used by other cloud providers, in order to
          provide a backoff/retry decorator based on status codes.

        - Example using the AWSRetry class which inherits from CloudRetry.
          @AWSRetry.retry(backoff=exponential_backoff(retries=10, delay=3))
          get_ec2_security_group_ids_from_names()

    * exponential_backoff
        - For use with the CloudRetry.backoff classmethod.

        - Exponentially back off of a cloud function call.

    * full_jitter_backoff
        - For use with the CloudRetry.backoff classmethod.

        - Exponentially back off of a cloud function call with jitter.

Backoff functions return callables. When executed, these callables
return generators that yield durations in seconds that should be waited for the
corresponding backoff strategy.

Backoff Example:
    >>> backoff = full_jitter_backoff(retries=5)
    >>> backoff
    <function backoff_backoff at 0x7f0d939facf8>
    >>> list(backoff())
    [3, 6, 5, 23, 38]
    >>> list(backoff())
    [2, 1, 6, 6, 31]
"""
import random
from functools import wraps
import syslog
import time

from ansible.module_utils.pycompat24 import get_exception


def exponential_backoff(retries=10, delay=3, backoff=2):
    """ Customizable exponential backoff strategy.
    Args:
        retries (int): Maximum number of times to retry a request.
        delay (float): Initial (base) delay.
        backoff (float): base of the exponent to use for exponential
            backoff.
    Returns:
        Callable that returns a generator. This generator yields durations in
        seconds to be used as delays for an exponential backoff strategy.
    """
    def backoff_gen():
        for retry in range(0, retries):
            yield delay * backoff ** retry
    return backoff_gen


def full_jitter_backoff(retries=10, delay=3, max_delay=60, _random=random):
    """ Implements the "Full Jitter" backoff strategy described here
    https://www.awsarchitectureblog.com/2015/03/backoff.html
    Args:
        retries (int): Maximum number of times to retry a request.
        delay (float): Approximate number of seconds to sleep for the first
            retry.
        max_delay (int): The maximum number of seconds to sleep for any retry.
            _random (random.Random or None): Makes this generator testable by
            allowing developers to explicitly pass in the a seeded Random.
    Returns:
        Callable that returns a generator. This generator yields durations in
        seconds to be used as delays for a full jitter backoff strategy.
    """
    def backoff_gen():
        for retry in range(0, retries):
            yield _random.randint(0, min(max_delay, delay * 2 ** retry))
    return backoff_gen


default_backoff = exponential_backoff(retries=10, delay=3, backoff=1.1)


class CloudRetry(object):
    """ CloudRetry can be used by any cloud provider, in order to implement a
        backoff algorithm/retry effect based on Status Code from Exceptions.
    """
    # This is the base class of the exception.
    # AWS Example botocore.exceptions.ClientError
    base_class = None

    @staticmethod
    def status_code_from_exception(error):
        """ Return the status code from the exception object
        Args:
            error (object): The exception itself.
        """
        pass

    @staticmethod
    def found(response_code):
        """ Return True if the Response Code to retry on was found.
        Args:
            response_code (str): This is the Response Code that is being matched against.
        """
        pass

    @classmethod
    def backoff(cls, backoff=default_backoff):
        """ Retry calling the Cloud decorated function using an exponential
        backoff.
        Kwargs:
            backoff (callable): Callable that returns a generator. The returned
            generator should yield sleep times for each retry of the decorated
            function.
        """
        def deco(f):
            @wraps(f)
            def retry_func(*args, **kwargs):
                for delay in backoff():
                    try:
                        return f(*args, **kwargs)
                    except Exception:
                        e = get_exception()
                        if isinstance(e, cls.base_class):
                            response_code = cls.status_code_from_exception(e)
                            if cls.found(response_code):
                                msg = "{0}: Retrying in {1} seconds...".format(str(e), delay)
                                syslog.syslog(syslog.LOG_INFO, msg)
                                time.sleep(delay)
                            else:
                                # Return original exception if exception is not a ClientError
                                raise e
                        else:
                            # Return original exception if exception is not a ClientError
                            raise e
                return f(*args, **kwargs)

            return retry_func  # true decorator

        return deco
