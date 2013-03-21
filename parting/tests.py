import functools
import imp
import mock
import sys
from django.db import models
from django.test import TestCase


def _cleanup(*models):
    """ Function to delete models from the AppCache and remove them from
    the models module in which they're defined.
    """
    from django.db.models.loading import cache
    deleted = []

    # Note that we want to use the import lock here - the app loading is
    # in many cases initiated implicitly by importing, and thus it is
    # possible to end up in deadlock when one thread initiates loading
    # without holding the importer lock and another thread then tries to
    # import something which also launches the app loading. For details of
    # this situation see Django bug #18251.
    imp.acquire_lock()
    try:
        for app_label, model_dict in cache.app_models.items():
            for django_name, klass in model_dict.items():
                name = '{}.{}'.format(klass.__module__, klass.__name__)

                if name in models:
                    module = sys.modules[klass.__module__]
                    delattr(module, klass.__name__)
                    del model_dict[django_name]
                    deleted.append(name)

        if sorted(deleted) != sorted(models):
            expected = ', '.join(models)
            expected = expected if expected else '(none)'
            actual = ', '.join(deleted)
            actual = actual if actual else '(none)'
            raise AssertionError(
                'Expected to delete {}, actually deleted {}'.format(
                    expected,
                    actual))

        # Reset a load of state variables in the app cache
        cache.loaded = False
        cache._get_models_cache = {}
        cache.handled = {}
        cache.postponed = []
        cache.nesting_level = 0
        cache._populate()
    finally:
        imp.release_lock()


def cleanup_models(*models):
    """ Decorator that declares a test method generates test models that
    will need cleaning up.
    The decorator then cleans up the Django AppCache after the test
    has run, to prevent the test framework's attempts to flush the (then)
    non-existant model.

    The fully-qualified names of the test models should be passed.
    """
    def outer(func):
        @functools.wraps(func)
        def wrapped(self, *args, **kw):
            try:
                func(self, *args, **kw)
            finally:
                _cleanup(*models)
        return wrapped
    return outer


class PartitionedModelTests(TestCase):

    def test_base_model_require_abstract(self):
        """ When a model has a PartitionManager, it must be declared
        abstract
        """
        from parting import PartitionManager
        with self.assertRaises(AssertionError):

            class BadModel(models.Model):
                objects = PartitionManager()


class PartitionForeignKeyTestCase(TestCase):

    def test_must_be_abstract(self):
        """ Any model featuring a Partition foreign key must itself be
        abstract, otherwise the fk constraints won't work.
        """
        from parting import PartitionManager, PartitionForeignKey

        class PartitionModel(models.Model):
            objects = PartitionManager()

            class Meta:
                abstract = True

        with self.assertRaises(AssertionError):
            class ChildPartitionModel(models.Model):
                parent = PartitionForeignKey(PartitionModel)


class PartitionTests(TestCase):

    @mock.patch('testapp.models.TweetManager.current_partition_key')
    @cleanup_models('testapp.models.Tweet_foo')
    def test_current_partition(self, current_partition_key):
        """ Check that we can create the current partition model
        """
        from testapp.models import Tweet
        current_partition_key.return_value = u'foo'

        model = Tweet.objects.ensure_current_partition()
        self.assertEqual('Tweet_foo', model.__name__)
        self.assertTrue(issubclass(model, models.Model))

    @mock.patch('testapp.models.TweetManager.next_partition_key')
    @cleanup_models('testapp.models.Tweet_foo')
    def test_next_partition(self, next_partition_key):
        """ Check that we can create the next partition model
        """
        from testapp.models import Tweet
        next_partition_key.return_value = u'foo'

        model = Tweet.objects.ensure_next_partition()
        self.assertEqual('Tweet_foo', model.__name__)
        self.assertTrue(issubclass(model, models.Model))

    def test_no_overwrite(self):
        """ Check that we don't overwrite
        """
        from testapp.models import Tweet
        import testapp.models
        testapp.models.Tweet_foo = object()
        try:
            with self.assertRaises(AttributeError):
                Tweet.objects.ensure_partition('foo')
        finally:
            delattr(testapp.models, 'Tweet_foo')
