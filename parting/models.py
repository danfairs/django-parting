import imp
import logging
import sys
from django.db.models import Manager, get_model
from django.db.models.fields.related import ManyToOneRel
from dfk import DeferredForeignKey, point

PARTITION_KEY = '_partition_key'

logger = logging.getLogger(__file__)
_marker = object()


def partition_key(cls, default=_marker):
    if default is not _marker:
        return getattr(cls, PARTITION_KEY, default)
    else:
        return getattr(cls, PARTITION_KEY)


class PartitionRegistry(object):

    def __init__(self):
        # We're not using a defaultdict(list) here to avoid clunky gymnastics
        # in child_models_for polluting the structure. This keeps a mapping
        # of parent model -> list of partitioned foreign keys
        self.partitioned_targets = {}

    def register_foreign_key(self, fk):
        self.partitioned_targets.setdefault(fk.to, []).append(fk)

    def foreign_keys_referencing(self, model):
        return self.partitioned_targets.get(model, [])

_registry = PartitionRegistry()


class PartitionManager(object):
    """
    Manager to provide helpers for partitions. Note that this isn't actually
    a real manager, it just aims to 'feel' like one.
    """

    def __init__(self, partition_registry=_registry):
        self.registry = partition_registry

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

    def get_managers(self, partition):
        """ Return an iterable of tuples of name, manager pairs, which will be
        added to all partitions in the given order. Order is important, as
        Django regards the first manager as the default manager.

        Defaults to a standard models.Manager() instance called 'objects'
        """
        return [
            ('objects', Manager())
        ]

    # Utility methods
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
                    attrs={PARTITION_KEY: partition_key},
                    module_path=self.model.__module__)

                for name, manager in self.get_managers(model):
                    manager.contribute_to_class(model, name)

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
                pfks_to_remove = set()
                for pfk in self.registry.foreign_keys_referencing(self.model):
                    if not hasattr(pfk.cls, '_partition_manager'):
                        raise AttributeError(
                            'Source model {} does not have a partition '
                            'manager'.format(pfk.cls))
                    child = pfk.cls._partition_manager.ensure_partition(
                        partition_key)

                    # Replace the placeholder with a real foreign key
                    point(child, pfk.name, model, **pfk.kwargs)
                    pfks_to_remove.add(pfk)

                # Make sure that there are no pfks hanging around
                for pkf in pfks_to_remove:
                    for lst in (child._meta.fields, child._meta.local_fields):
                        for field in lst:
                            if field.name == pkf.name and isinstance(
                                    field,
                                    PartitionForeignKey):
                                lst.remove(field)
                                break

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
    creation_counter = 0

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
        if not cls._meta.abstract and not partition_key(cls, None):
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
