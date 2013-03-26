import importlib
import logging
from cStringIO import StringIO
from django.core.management.commands import sqlall, syncdb
from django.db import models
from optparse import make_option
from django.core.management.base import BaseCommand, CommandError
from django.db import DEFAULT_DB_ALIAS

logger = logging.getLogger(__file__)


class Command(BaseCommand):

    option_list = BaseCommand.option_list + (
        make_option('-d', '--database', dest='database'),
        make_option('-c', '--current-only', dest='current_only',
                    action='store_true'),
        make_option('-n', '--next-only', dest='next_only',
                    action='store_true'),
        make_option('--sqlall', dest='sqlall', action='store_true')
    )

    def handle(self, *args, **options):
        self.args = args
        self.options = options
        model = self.get_model()
        database = self.options.get('database')
        database = database if database else DEFAULT_DB_ALIAS
        only_sqlall = self.options.get('sqlall')

        # First, make sure all the partition models have been generated
        for partition_name in self.get_partition_names(model):
            model._partition_manager.get_partition(partition_name)

        if only_sqlall:
            # We've been asked just to dump the SQL
            sqlall_command = sqlall.Command()
            self._setup_command(sqlall_command)
            app = models.get_app(model._meta.app_label)
            print(sqlall_command.handle_app(
                app,
                database=database
            ))
        else:
            # Invoke syncdb directly. We don't use call_command, as South
            # provides its own implementation which we don't want to use.
            syncdb_command = syncdb.Command()
            self._setup_command(syncdb_command)
            syncdb_command.handle_noargs(
                database=database,
                interactive=False,
                load_initial_data=False,
                show_traceback=True,
                verbosity=0,
            )

    def get_partition_names(self, model):
        current = model._partition_manager.current_partition_key
        next = model._partition_manager.next_partition_key
        current_only = self.options.get('current_only')
        next_only = self.options.get('next_only')

        if current_only and next_only:
            raise CommandError(
                u'You cannot specify current_only and next_only togethers')

        try:
            partition_names = [self.args[1]]
        except IndexError:
            partition_names = None

        if current_only:
            partition_names = [current()]
        elif next_only:
            partition_names = [next()]
        elif not partition_names:
            # No explicit partition names given, use current and next
            partition_names = [current(), next()]

        return partition_names

    def get_model(self):
        try:
            model = self.args[0]
        except IndexError:
            raise CommandError(u'Please supply at least one partitioned model')

        try:
            module_name, model_name = model.rsplit('.', 1)
        except ValueError:
            raise CommandError('Bad model name {}'.format(model))

        # So - we can't use get_model, because this will be an abstract model.
        # Try to grab the model directly from the module.
        module = importlib.import_module(module_name)
        try:
            m = getattr(module, model_name)
        except AttributeError:
            raise CommandError('Unknown model {}'.format(model))
        return m

    def _setup_command(self, c):
        # Plumb some attributes normally set up by a base class directly onto
        # the command
        c.stdout = StringIO()
