from django.test import TestCase, TransactionTestCase





class PartitionTests(TransactionTestCase):


    def test_current_partition(self):
        """ Check that we can create the current partition
        """
        Tweet.objects.ensure_current_partition()


