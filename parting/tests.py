import functools
import imp
import mock
import sys
from django.core.management.base import CommandError
from django.db import models
from django.test import TestCase, TransactionTestCase


def _raise(exc_info):
    raise exc_info[0], exc_info[1], exc_info[2]


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
            exc_info = None
            try:
                try:
                    func(self, *args, **kw)
                except:
                    exc_info = sys.exc_info()
            finally:
                try:
                    _cleanup(*models)
                except:
                    # OK, we got an error cleaning up. If the actual test
                    # passed, then we're happy to raise this cleanup exception.
                    # Otherwise, we raise the original exception - the
                    # cleanup one might get fixed when the test itself is
                    # fixed. Just allowing the cleanup exception to bubble
                    # up masks the problem.
                    cleanup_exc_info = sys.exc_info()
                    _raise(exc_info) if exc_info else _raise(cleanup_exc_info)
                else:
                    if exc_info:
                        _raise(exc_info)
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


class PartitionForeignKeyTests(TestCase):

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

    @cleanup_models(
        'parting.tests.PartitionModel_foo',
        'parting.tests.ChildPartitionModel_foo')
    def test_child_partions_generated(self):
        """ When a parent partition is generated, child partitions (as
        determined) by PartitionForeignKey relationships) should also be
        generated.
        """
        from parting import PartitionManager, PartitionForeignKey

        class PartitionModel(models.Model):
            objects = PartitionManager()

            class Meta:
                abstract = True

        class ChildPartitionModel(models.Model):
            parent = PartitionForeignKey(PartitionModel)
            objects = PartitionManager()

            class Meta:
                abstract = True

        # Generating the parent partition should cause a child partition
        # to also be created, with the same key.
        PartitionModel.objects.get_partition('foo')
        child_partition = ChildPartitionModel.objects.get_partition('foo')
        self.assertTrue(child_partition is not None)

    def test_multiple_fks_bad(self):
        """ If there are multiple PartitionForeignKeys, they must all point
        to the same model. This keeps everything simpler """
        from parting import PartitionManager, PartitionForeignKey

        class ParentModel1(models.Model):
            objects = PartitionManager()

            class Meta:
                abstract = True

        class ParentModel2(models.Model):
            objects = PartitionManager()

            class Meta:
                abstract = True

        with self.assertRaises(AssertionError):
            class ChildPartitionModel(models.Model):
                parent_1 = PartitionForeignKey(ParentModel1)
                parent_2 = PartitionForeignKey(ParentModel2)

                class Meta:
                    abstract = True

    @cleanup_models(
        'parting.tests.ParentModel_foo',
        'parting.tests.ChildPartitionModel_foo')
    def test_multiple_fks_good(self):
        """ If there are multiple PartitionForeignKeys, they must all point
        to the same model. This keeps everything simpler """
        from parting import PartitionManager, PartitionForeignKey

        class ParentModel(models.Model):
            objects = PartitionManager()

            class Meta:
                abstract = True

        class ChildPartitionModel(models.Model):
            parent_1 = PartitionForeignKey(
                ParentModel,
                related_name='parent_1_set')
            parent_2 = PartitionForeignKey(
                ParentModel,
                related_name='parent_2_set')

            objects = PartitionManager()

            class Meta:
                abstract = True

        p = ParentModel.objects.get_partition('foo')
        c = ChildPartitionModel.objects.get_partition('foo')

        self.assertEqual(p, c._meta.get_field('parent_1').rel.to)
        self.assertEqual(p, c._meta.get_field('parent_2').rel.to)

        # Check that there are no PartitionForeignKey instances hanging
        # around in the child's _meta
        for field in c._meta.fields:
            self.failIf(isinstance(field, PartitionForeignKey))
        for field in c._meta.local_fields:
            self.failIf(isinstance(field, PartitionForeignKey))
        for field, model in c._meta.get_fields_with_model():
            self.failIf(isinstance(field, PartitionForeignKey))


class PartitionTests(TestCase):

    @cleanup_models('testapp.models.Tweet_foo', 'testapp.models.Star_foo')
    def test_get_partition(self):
        """ Check that once a partition is generated, we can fetch it
        with get_partition
        """
        from testapp.models import Star, Tweet
        expected_partition = Tweet.partitions.get_partition('foo')
        assert expected_partition
        partition = Tweet.partitions.get_partition('foo')
        self.assertEqual(expected_partition, partition)

        # We should also now be able to get the 'foo' partition for Star,
        # and its FK should point to the Tweet partition
        star_partition = Star.partitions.get_partition('foo')
        fk = star_partition._meta.get_field('tweet')
        self.assertEqual(partition, fk.rel.to)

        # We should also find that our custom manager is in place
        self.assertTrue(hasattr(partition.objects, 'my_custom_method'))

    def test_get_missing_partition(self):
        """ Attempting to fetch a missing partition will just return None
        (mirroring the behaviour of Django's get_model), as long as we don't
        auto-create
        """
        from testapp.models import Tweet
        self.assertEqual(
            None,
            Tweet.partitions.get_partition('foo', create=False)
        )

    @cleanup_models('testapp.models.Tweet_foo')
    def test_no_overwrite(self):
        """ Check that we don't overwrite
        """
        from testapp.models import Tweet
        import testapp.models
        testapp.models.Tweet_foo = object()
        with self.assertRaises(AttributeError):
            Tweet.partitions.get_partition('foo')

    @cleanup_models('testapp.models.Tweet_foo')
    def test_get_field_by_name(self):
        """ Check that get_field_by_name on a foreign key that was
        generated from a PartitionForeignKey returns a real FK, not the
        PFK.
        """
        from testapp.models import Star, Tweet

        Tweet.partitions.get_partition('foo')
        star_partition = Star.partitions.get_partition('foo')

        fk, _, _, _ = star_partition._meta.get_field_by_name('tweet')
        self.assertTrue(isinstance(fk, models.ForeignKey))

    @cleanup_models('testapp.models.Tweet_foo', 'testapp.models.Star_foo')
    def test_get_partition_key(self):
        """ Check that we can find out what the partition key a model was
        generated from. This can be useful if an application knows that
        a number of related models were generated using the same key.
        """
        from testapp.models import Star, Tweet
        from parting.models import get_partition_key
        tweet_partition = Tweet.partitions.get_partition('foo')
        star_partition = Star.partitions.get_partition('foo')

        self.assertEqual('foo', get_partition_key(tweet_partition))
        self.assertEqual('foo', get_partition_key(star_partition))


class CommandTests(TransactionTestCase):

    def setUp(self):
        from django.db import connection
        tables = set(connection.introspection.table_names())
        self.failIf(tables)

    def _run(self, *args, **kwargs):
        from parting.management.commands import ensure_partition
        command = ensure_partition.Command()
        command.handle(*args, **kwargs)

    def check_tables(self, *names):
        """ Check the named tables exist in the database, and clean them
        up if they do
        """
        from django.db import connection
        names = set(names)
        tables = set(connection.introspection.table_names())
        missing_tables = names - tables
        if missing_tables:
            self.fail(
                'The following tables are missing: {}'.format(
                    ', '.join(t for t in missing_tables)))

        # Yay hack!
        cursor = connection.cursor()
        for name in tables:
            cursor.execute(
                'DROP TABLE {}'.format(
                    connection.ops.quote_name(name)
                ))

    def test_missing_model(self):
        """ The command requires at least 1 argument, a model
        """
        with self.assertRaises(CommandError):
            self._run()

    def test_both_current_next(self):
        """ Check we can't specify both current and next """
        with self.assertRaises(CommandError):
            self._run(
                'testapp.models.Tweet',
                current_only=True,
                next_only=True)

    def test_ensure_names(self):
        """ Check that we can pass an explicit model and partition key,
        and the tables will appear
        """
        self._run('testapp.models.Tweet', 'foo')
        self.check_tables('testapp_tweet_foo', 'testapp_star_foo')

    @cleanup_models('testapp.models.Tweet_baz', 'testapp.models.Star_baz')
    @mock.patch('testapp.models.TweetPartitionManager.current_partition_key')
    def test_current_partition(self, current_partition_key):
        """ Check that we can pass --current and the current partition will
        be created """
        current_partition_key.return_value = 'baz'
        self._run('testapp.models.Tweet', current_only=True)
        self.check_tables('testapp_tweet_baz', 'testapp_star_baz')

    @cleanup_models('testapp.models.Tweet_baz', 'testapp.models.Star_baz')
    @mock.patch('testapp.models.TweetPartitionManager.next_partition_key')
    def test_next_partition(self, next_partition_key):
        next_partition_key.return_value = 'baz'
        self._run('testapp.models.Tweet', next_only=True)
        self.check_tables('testapp_tweet_baz', 'testapp_star_baz')

    @cleanup_models(
        'testapp.models.Tweet_baz',
        'testapp.models.Star_baz',
        'testapp.models.Tweet_foo',
        'testapp.models.Star_foo',
    )
    @mock.patch('testapp.models.TweetPartitionManager.current_partition_key')
    @mock.patch('testapp.models.TweetPartitionManager.next_partition_key')
    def test_no_switches(self, next_partition_key, current_partition_key):
        """ If we pass no switches, then the current and next partitions will
        be created. """
        current_partition_key.return_value = 'foo'
        next_partition_key.return_value = 'baz'
        self._run('testapp.models.Tweet')
        self.check_tables(
            'testapp_tweet_baz',
            'testapp_star_baz',
            'testapp_tweet_foo',
            'testapp_star_foo',
        )

    def test_bad_model(self):
        """ Check that a non-existant model causes a CommandError """
        with self.assertRaises(CommandError):
            self._run('doesnotexist')
