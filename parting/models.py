from django.db import models


class PartitionMixin(object):

    def contribute_to_class(self, model, name):
        super(PartitionMixin, self).contribute_to_class(model, name)
        if not model._meta.abstract:

            raise AssertionError(
                u'Partitioned model {} must be abstract.'.format(model._meta))


class PartitionManager(PartitionMixin, models.Manager):
    pass


class PartitionForeignKey(object):

    def __init__(self, *args, **kw):
        pass
