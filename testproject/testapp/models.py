from django.db import models
from django.utils import timezone
from parting import PartitionForeignKey, PartitionManager
from dateutil.relativedelta import relativedelta


def _key_from_dt(dt):
    return dt.strftime('%Y_%m')


class TweetManager(PartitionManager):

    def current_partition_key(self):
        return _key_from_dt(timezone.now())

    def next_partition_key(self):
        return _key_from_dt(timezone.now() + relativedelta(months=+1))


class Tweet(models.Model):

    json = models.TextField()
    created = models.DateTimeField(default=timezone.now)

    objects = TweetManager()

    class Meta:
        abstract = True


class Star(models.Model):

    user = models.TextField()
    tweet = PartitionForeignKey(Tweet)

    objects = PartitionManager()

    class Meta:
        abstract = True
