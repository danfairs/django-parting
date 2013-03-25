django-parting
==============

django-parting helps you partition tables with large amounts of data.

WARNING: This is currently very raw, and has only been developed against
PostgreSQL and Django 1.4. Patches welcome!

Partitioned Models
==================

django-parting helps manage partitioned models. Partitioned models are those
whose data is stored in several underlying tables.

For example, consider the following two models:

    from django.utils import timezone

    class Tweet(models.Model):
        json = models.TextField()
        user = models.TextField()
        created_at = models.DateTimeField(default=timezone.now)


    class Star(models.Model):
        tweet = models.ForeignKey(Tweet)
        user = models.TextField()

A Star instance relates an original tweet with a user, who has 'starred' that
tweet.

There are lots of Tweets in the world. Too many to store in a single database
table. We're therefore going to partition this so we end up with a table
per month.

However, it's not quite as simple as is might be - the Star model has
a foreign key relationship with the Tweet table. If we split up the table
in which tweets are stored, that prevents us having those relationships.

To make those relationships work, we have to also partition the Star table
in the same way - and we need to make sure the Star records go into the
correct partition with the relationships to the correct parent Tweet table.

We'd also like to use django-parting's ability to ensure partitions are created
automatically.

We do that using a PartitionForeignKey.

    from django.utils import timezone
    from parting import PartitionManager
    from dateutil.relativedelta import relativedelta

    def _key_for_date(dt):
        return dt.strftime('%Y%m')

    class TweetPartitionManager(PartitionManager):

        def current_partition(self):
            """ Provide the key for the 'current' partition
            """
            return _key_for_date(timezone.now())

        def next_partition(self):
            """ Provide the key for the 'next' partition
            """
            one_months_time = timezone.now() + relativedelta(months=+1)
            return _key_for_date(one_months_time)


    class Tweet(models.Model):
        json = models.TextField()
        user = models.TextField()
        created_at = models.DateTimeField(default=timezone.now)

        partitions = TweetPartitionManager()

        class Meta:
            abstract = True


    class Star(models.Model):
        tweet = models.PartitionForeignKey(Tweet)
        user = models.TextField()

        partitions = TweetPartitionManager()

        class Meta:
            abstract = True

Note both models are abstract. This is because we don't store data in them
directly, but in partitions based on those models. They also both have a
TweetPartitionManager instance. (Note that this isn't a real Django manager,
but it makes the API feel more Django-like)

Now, if you run a syncdb, nothing will happen. A management command
is provided which, by default, will create the current and next partitions
for a named model, assuming they don't exist:

    $ python manage.py ensure_partition myapp.models.Tweet

That will call `current_partition()` and `next_partition()` defined on the
Tweet's PartitionManager instance, and use the result as part of the generated
model and table name.

If you want to see what would be generated, you can pass `--sqlall` as a
switch:

    $ python manage.py ensure_partition myapp.models.Tweet --sqlall
    CREATE TABLE "testapp_tweet_2013_03" (
        "id" integer NOT NULL PRIMARY KEY,
        "json" text NOT NULL,
        "created" datetime NOT NULL
    )
    ;
    CREATE TABLE "testapp_star_2013_03" (
        "id" integer NOT NULL PRIMARY KEY,
        "tweet_id" integer NOT NULL REFERENCES "testapp_tweet_2013_03" ("id"),
        "user" text NOT NULL
    )
    ;
    CREATE TABLE "testapp_tweet_2013_04" (
        "id" integer NOT NULL PRIMARY KEY,
        "json" text NOT NULL,
        "created" datetime NOT NULL
    )
    ;
    CREATE TABLE "testapp_star_2013_04" (
        "id" integer NOT NULL PRIMARY KEY,
        "tweet_id" integer NOT NULL REFERENCES "testapp_tweet_2013_04" ("id"),
        "user" text NOT NULL
    )
    ;
    CREATE INDEX "testapp_star_2013_03_36542d72" ON "testapp_star_2013_03" ("tweet_id");
    CREATE INDEX "testapp_star_2013_04_36542d72" ON "testapp_star_2013_04" ("tweet_id");

Note that the foreign key on the dependent `testapp_star_2013_03` and
`testapp_star_2013_04` tables point to the appropriate parent table for that
partition.

If you're not using time-based partitioning (ie. there's no real meaning to
'current' and 'next') then you can just ask it to create a specific, named
partition that makes sense to your application:

    $ python manage.py ensure_partition testapp.models.Tweet baz --sqlall
    CREATE TABLE "testapp_tweet_baz" (
        "id" integer NOT NULL PRIMARY KEY,
        "json" text NOT NULL,
        "created" datetime NOT NULL
    )
    ;
    CREATE TABLE "testapp_star_baz" (
        "id" integer NOT NULL PRIMARY KEY,
        "tweet_id" integer NOT NULL REFERENCES "testapp_tweet_baz" ("id"),
        "user" text NOT NULL
    )
    ;
    CREATE INDEX "testapp_star_baz_36542d72" ON "testapp_star_baz" ("tweet_id");

So - we have our partitions, how do we actually use them? Well, django-parting
helps less here. It simply provides an API to fetch a model representing a
partition for a given partition key. That model is a standard Django model,
and will be tied to the underlying table that represents the partition.

Hence, it's up to your application to determine the correct partition (and
hence model) for some data.

For example:

    import json
    from django.utils.timezone import make_aware, utc

    tweet_data = {
        'created_at': make_aware(datetime.datetime(2012, 12, 6, 14, 23), utc)
        'json': json.dumps({'key': 'value'}),
        'user': 'Jimmy'
    }
    partition_key = _key_for_dt(tweet_data['created_at'])
    partition = Tweet.partitions.get_partition(partition_key)
    tweet = partition(**tweet_data)
    tweet.save()

Likewise, you need to make sure that you know which partition you need to look
in to find your data.


Custom Managers
===============

Often, you will want your generated partition models to have custom managers.
This is supported by adding a `get_managers()` method to your partition
manager subclass, for example:

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

 Now, whenever a Tweet partition is generated, the `objects` attribute will
 be an instance of `CustomManager`.
