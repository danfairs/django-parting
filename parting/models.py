import imp
import logging
import sys
from django.db.models import get_model
from django.db.models.fields.related import ManyToOneRel
from dfk import DeferredForeignKey, point

logger = logging.getLogger(__file__)
_marker = object()


def is_partition(cls):
    return getattr(cls, '_is_partition', False)


class PartitionRegistry(object):

    def __init__(self):
        # We're not using a defaultdict(list) here to avoid clunky gymnastics
        # in child_models_for polluting the structure. This keeps a mapping
        # of parent model -> list of partitioned foreign keys
        self.partitioned_targets = {}

        # A simple set of partitions. This is just used to figure out whether
        # or not a particular model represents a partition, and avoids storing
        # a nasty custom attribute on _meta, or something like that.
        self.registered_partitions = set()

    def register_foreign_key(self, fk):
        self.partitioned_targets.setdefault(fk.to, []).append(fk)

    def foreign_keys_referencing(self, model):
        return self.partitioned_targets.get(model, [])

_registry = PartitionRegistry()


class PartitionManager(object):
    """
    Manager to provide helpers for partitions. Note that this isn't actually
    a real manager, it just provides a manager-like API.
    """

    def __init__(self, partition_registry=_registry):
        self.registry = partition_registry

    # Methods that subclasses should override
    def partition_key_for(self, ob):
        """ Return the partition key for the given object. The partition key
        controls which partition the object should live in. This should remain
        the same for the object's life - if something happens to change an
        instance's partition key, then the instance should be deleted from its
        old partition before being resaved. This will not happen automatically.
        """
        raise NotImplementedError()

    def current_partition_key(self):
        """ Return the partition key for 'now'. No need to implement this if
        you're not using time-based partitioning.
        """
        raise NotImplementedError()

    def next_partition_key(self):
        """ Return the partition key for 'next'. No need to implement this if
        you're not using time-based partitioning.
        """
        raise NotImplementedError()

    # Utility methods
    def ensure_current_partition(self):
        """ Make sure the current partition's model exists. Note that this
        does not actually create any tables. Returns the model for the
        partition.
        """
        return self.ensure_partition(self.current_partition_key())

    def ensure_next_partition(self):
        """ Make sure the next partition exists
        """
        return self.ensure_partition(self.next_partition_key())

    def get_partition(self, partition_key):
        # Try to grab the required model
        app_label = self.model._meta.app_label
        model_name = self._model_name_for_partition(partition_key)
        return get_model(app_label, model_name)

    def ensure_partition(self, partition_key):
        """ Make sure a partition exists with the given partition key,
        creating it if necessary.
        """
        logger.debug(
            'Ensuring partition for model {} key {}'.format(
                self.model,
                partition_key))
        model = self.get_partition(partition_key)

        # If we didn't get anything back, we need to generate the model.
        if not model:
            logger.debug('Partition not found, generating')
            model_name = self._model_name_for_partition(partition_key)
            imp.acquire_lock()
            try:
                model = create_model(
                    model_name,
                    bases=(self.model,),
                    attrs={'_is_partition': True},
                    module_path=self.model.__module__)

                # Make sure that we don't overwrite an existing name in the
                # module. Raise an AttributeError if we look like we're about
                # to.
                module = sys.modules[self.model.__module__]
                if getattr(module, model_name, _marker) is _marker:
                    setattr(module, model_name, model)
                else:
                    raise AttributeError('{} already exists in {}'.format(
                        model_name, module))

                logger.debug(
                    '{} generated, processing PartitionForeignKeys'.format(
                        model))

                # Find any PartitionForeignKeys that point to our parent model,
                # and generate partitions of those source models
                for pfk in self.registry.foreign_keys_referencing(self.model):
                    if not hasattr(pfk.cls, '_partition_manager'):
                        raise AttributeError(
                            'Source model {} does not have a partition '
                            'manager'.format(pfk.cls))
                    child = pfk.cls._partition_manager.ensure_partition(
                        partition_key)

                    # Replace the placeholder with a real foreign key
                    point(child, pfk.name, model, **pfk.kwargs)

            finally:
                imp.release_lock()
        return model

    # Django integration API
    def contribute_to_class(self, model, name):
        if not model._meta.abstract:
            raise AssertionError(
                u'Partitioned model {} must be abstract.'.format(model._meta))

        self.model = model
        setattr(model, name, self)

        # We also have to keep a reference to ourself in a standard attribute
        # name, so that ensure_partition can find us when processing partition
        # foriengn keys
        model._partition_manager = self

    # Private stuff
    def _model_name_for_partition(self, partition_key):
        return '{}_{}'.format(
            self.model._meta.object_name,
            partition_key)


class PartitionForeignKey(DeferredForeignKey):
    """ This class is really just a placeholder. When the target of the
    foreign key has a partition generated, this will be replaced by a real
    foreign key pointing to the new, generated partition.
    """
    # Options.add_field expects there to be a rel attribute. We can
    # safely just set it to None, as this will eventually be replaced
    # by a proper foreign key anyway.
    rel = None
    primary_key = None

    def __init__(self, to, to_field=None, rel_class=ManyToOneRel,
                 partition_registry=_registry, **kwargs):
        """ Store the args to the fk, and validate that what we are pointing
        to is a partitioned model (TODO)
        """
        super(PartitionForeignKey, self).__init__(
            to_field=to_field,
            rel_class=rel_class,
            **kwargs)
        self.to = to
        self.registry = partition_registry

    def contribute_to_class(self, cls, name):
        # Check that we're not being used on a concrete class that's not
        # a partition. It's OK for us to be on a partition, as we'll shortly
        # be replaced. (It might be nice to figure out a way to do that here,
        # but we'd need to know which partition on of 'to' we should point
        # to.)
        if not cls._meta.abstract and not is_partition(cls):
            raise AssertionError(
                '{} uses a PartitionForeignKey and must therefore '
                'be declared abstract'.format(cls))

        # Check that any other partition foreign keys on this instance
        # point to the same place
        for field in cls._meta.fields:
            if isinstance(field, PartitionForeignKey) and field.to != self.to:
                raise AssertionError(
                    'Multiple PartitionForeignKey instances on {} do not '
                    'point to the same target.'.format(cls))

        self.cls = cls
        self.name = self.attname = name
        self.registry.register_foreign_key(self)
        cls._meta.add_field(self)
        setattr(cls, name, self)


def create_model(name, bases=None, attrs={}, module_path='', meta_attrs={}):
    """ Create a new model class.
    name       - name of the new class to create
    bases      - tuple of base classes. This defaults to (models.Model,).
    attrs      - attributes that should be set on the new instance (will be
                 passed directly to the `type()` call)
    meta_attrs - Extra attributes that should be set on the generated Meta
                 class.

    Based on Pro Django.
    """
    assert module_path
    if bases is None:
        from django.db.models import Model
        bases = (Model,)
    attrs['__module__'] = module_path
    if meta_attrs:
        # Custom meta_attrs - we replace the existing Meta class with a new
        # one, based on the provided attrs.
        class Meta:
            pass
        Meta.__dict__.update(meta_attrs, __module__=module_path)
        attrs['Meta'] = Meta

    return type(name, bases, attrs)
