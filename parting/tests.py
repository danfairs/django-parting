from django.db import models
from django.test import TestCase, TransactionTestCase


class PartitionedModelTests(TestCase):

    def test_base_model_require_abstract(self):
        """ When a model has a PartitionManager, it must be declared
        abstract
        """
        from parting import PartitionManager

        with self.assertRaises(AssertionError):

            class BadModel(models.Model):
                objects = PartitionManager()



class PartitionTests(TransactionTestCase):


    def test_current_partition(self):
        """ Check that we can create the current partition
        """
        from testapp.models import Tweet
        Tweet.objects.ensure_current_partition()


