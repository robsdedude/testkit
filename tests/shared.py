"""
Shared utilities for writing tests
Common between integration tests using Neo4j and stub server.

Uses environment variables for configuration:

TEST_BACKEND_HOST  Hostname of backend, default is localhost
TEST_BACKEND_PORT  Port on backend host, default is 9876
"""

import inspect
import os
import re
import unittest

from nutkit import protocol
from nutkit.backend import Backend
import warnings


def get_backend_host_and_port():
    host = os.environ.get('TEST_BACKEND_HOST', '127.0.0.1')
    port = os.environ.get('TEST_BACKEND_PORT', 9876)
    return host, port


def new_backend():
    """ Returns connection to backend, caller is responsible for closing
    """
    host, port = get_backend_host_and_port()
    return Backend(host, port)


def driver_feature(*features):
    features = set(features)

    for feature in features:
        if not isinstance(feature, protocol.Feature):
            raise Exception('The arguments must be instances of Feature')

    def get_valid_test_case(*args, **kwargs):
        if not args or not isinstance(args[0], TestkitTestCase):
            raise Exception('Should only decorate TestkitTestCase methods')
        return args[0]

    def driver_feature_decorator(func):
        def wrapper(*args, **kwargs):
            test_case = get_valid_test_case(*args, **kwargs)
            needed = set(map(lambda f: f.value, features))
            supported = test_case._driver_features
            missing = needed - supported
            if missing:
                test_case.skipTest("Needs support for %s" % ", ".join(missing))
            return func(*args, **kwargs)
        return wrapper
    return driver_feature_decorator


class MemoizedSupplier:
    """ Momoize the function it annotates.
    This way the decorated function will always return the
    same value of the first interaction independent of the
    supplied params.
    """

    def __init__(self, func):
        self._func = func
        self._memo = None

    def __call__(self, *args, **kwargs):
        if self._memo is None:
            self._memo = self._func(*args, **kwargs)
        return self._memo


@MemoizedSupplier
def get_driver_features(backend):
    try:
        response = backend.sendAndReceive(protocol.GetFeatures())
        if not isinstance(response, protocol.FeatureList):
            raise Exception("Response is not instance of FeatureList")
        return set(response.features)
    except (OSError, protocol.BaseError) as e:
        warnings.warn("Could not fetch FeatureList: %s" % e)
        return set()


def get_driver_name():
    return os.environ['TEST_DRIVER_NAME']


class TestkitTestCase(unittest.TestCase):
    def setUp(self):
        super().setUp()
        id_ = re.sub(r"^([^\.]+\.)*?tests\.", "", self.id())
        self._backend = new_backend()
        self.addCleanup(self._backend.close)
        self._driver_features = get_driver_features(self._backend)
        response = self._backend.sendAndReceive(protocol.StartTest(id_))
        if isinstance(response, protocol.SkipTest):
            self.skipTest(response.reason)

        # TODO: remove this compatibility layer when all drivers are adapted
        if get_driver_name() in ("python", "java", "javascript",
                                 "go", "dotnet"):
            for exp, sub in (
                (r"^stub\.bookmarks\.test_bookmarks\.TestBookmarks",
                 "stub.bookmark.Tx"),
                (r"^stub\.disconnects\.test_disconnects\.TestDisconnects.",
                 "stub.disconnected.SessionRunDisconnected."),
                (r"^stub\.iteration\.[^.]+\.TestIterationSessionRun",
                 "stub.iteration.SessionRun"),
                (r"^stub\.iteration\.[^.]+\.TestIterationTxRun",
                 "stub.iteration.TxRun"),
                (r"^stub\.retry\.[^.]+\.", "stub.retry."),
                (r"^stub\.routing\.[^.]+\.", "stub.routing."),
                (r"^stub\.routing\.RoutingV4x1\.", "stub.routing.RoutingV4."),
                (r"^stub\.routing\.RoutingV4x3\.", "stub.routing.Routing."),
                (r"^stub\.session_run_parameters\."
                 r"[^.]+\.TestSessionRunParameters\.",
                 "stub.sessionparameters.SessionRunParameters."),
                (r"^stub\.tx_begin_parameters\.[^.]+\.TestTxBeginParameters\.",
                 "stub.txparameters.TxBeginParameters."),
                (r"^stub\.versions\.[^.]+\.TestProtocolVersions",
                 "stub.versions.ProtocolVersions"),
                (r"^stub\.transport\.[^.]+\.TestTransport\.",
                 "stub.transport.Transport."),
            ):
                id_ = re.sub(exp, sub, id_)
        response = self._backend.sendAndReceive(protocol.StartTest(id_))
        if isinstance(response, protocol.SkipTest):
            self.skipTest(response.reason)

        elif not isinstance(response, protocol.RunTest):
            raise Exception("Should be SkipTest or RunTest, "
                            "received {}: {}".format(type(response),
                                                     response))

    def script_path(self, *path):
        base_path = os.path.dirname(inspect.getfile(self.__class__))
        return os.path.join(base_path, "scripts", *path)
