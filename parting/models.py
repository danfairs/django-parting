import imp
import sys
from django.db.models import get_model

_marker = object()


class PartitionManager(object):
    """
    Manager to provide helpers for partitions. Note that this isn't actually
    a real manager, it just provides a manager-like API.
    """

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
        """ Return the partition key for 'now' """
        raise NotImplementedError()

    def next_partition_key(self):
        raise NotImplementedError()

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

    def ensure_partition(self, partition_key):
        """ Make sure a partition exists with the given partition key,
        creating it if necessary.
        """
        model = self._model_for_partition(partition_key)

        # If we didn't get anything back, we need to generate the model.
        if not model:
            model_name = self._model_name_for_partition(partition_key)
            imp.acquire_lock()
            try:
                model = create_model(
                    model_name,
                    bases=(self.model,),
                    module_path=self.model.__module__)
                module = sys.modules[self.model.__module__]

                # Make sure that we don't overwrite an existing name in the
                # module. Raise an AttributeError if we look like we're about
                # to.
                if getattr(module, model_name, _marker) is _marker:
                    setattr(module, model_name, model)
                else:
                    raise AttributeError('{} already exists in {}'.format(
                        model_name, module))
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

    def _model_name_for_partition(self, partition_key):
        return '{}_{}'.format(
            self.model._meta.object_name,
            partition_key)

    # Private stuff
    def _model_for_partition(self, partition_key):
        # Try to grab the required model
        app_label = self.model._meta.app_label
        model_name = self._model_name_for_partition(partition_key)
        return get_model(app_label, model_name)


class PartitionForeignKey(object):

    def __init__(self, *args, **kw):
        pass


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
