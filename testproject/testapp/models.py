from django.db import models
from django.utils import timezone
from parting import PartitionForeignKey, PartitionManager
from dateutil.relativedelta import relativedelta


def _key_from_dt(dt):
    return dt.strftime('%Y_%m')


class CustomManager(models.Manager):
    def my_custom_method(self):
        return u'hi!'


class TweetPartitionManager(PartitionManager):

    def current_partition_key(self):
        return _key_from_dt(timezone.now())

    def next_partition_key(self):
        return _key_from_dt(timezone.now() + relativedelta(months=+1))

    def get_managers(self, partition):
        return [
            ('objects', CustomManager()),
        ]


class Tweet(models.Model):

    json = models.TextField()
    created = models.DateTimeField(default=timezone.now)

    partitions = TweetPartitionManager()

    class Meta:
        abstract = True


class Star(models.Model):

    user = models.TextField()
    tweet = PartitionForeignKey(Tweet)

    partitions = PartitionManager()

    class Meta:
        abstract = True
