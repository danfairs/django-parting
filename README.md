django-parting
==============

django-parting helps you partition tables with large amounts of data.

WARNING: This is currently very raw, and has only been developed against
PostgreSQL and Django 1.4. Patches welcome!

Partitioned Models
==================

django-parting helps manage partitioned models. Partitioned models are those
whose data is stored in several underlying tables.

django-parting supports grouping partitioned models together into a
partition family. A partition family is a set of related models, which may
have foreign keys between themselves, and whose data is all logically related.

For example, consider the following two models:

from django.utils import timezone

class Tweet(models.Model):
    json = models.TextField()
    user = models.TextField()
    created_at = models.DateTimeField(default=timezone.now)


class Retweet(models.Model):
    retweet = models.ForeignKey(Tweet)
    retweeted = models.ForeignKey(Tweet)


A Retweet instance relates two tweets: the retweeting tweet, and the retweeted
tweet.

There are lots of Tweets in the world. Too many to store in a single database
table. We're therefore going to partition this so we end up with a table
per month.

However, it's not quite as simple as is might be - the Retweet model has
a foreign key relationship with the Tweet table. If we split up the table
in which tweets are stored, that prevents us having those relationships.

To make those relationships work, we have to also partition the Retweet table
in the same way - and we need to make sure the Retweet records go into the
correct partition with the relationships to the correct parent Tweet table.

We do that using a PartitionForeignKey.

from django.utils import timezone
from parting import PartitionManager

def _key_for_date(dt):
    return dt.strftime('%Y%m')

class TweetPartitionManager(PartitionManager):

    def partition_key(self, tweet):
        return _key_for_date(tweet.created_at)

    def current_partition(self):
        return _key_for_date(timezone.now())

    def next_partition(self):
        one_months_time = timezone.now() + datetime.timedelta(months=1)
        return _key_for_date(one_months_time)


class Tweet(models.Model):
    json = models.TextField()
    user = models.TextField()
    created_at = models.DateTimeField(default=timezone.now)

    objects = TweetPartitionManager()


class Retweet(models.Model):
    retweet = models.ForeignKey(Tweet)
    retweeted = models.ForeignKey(Tweet)



