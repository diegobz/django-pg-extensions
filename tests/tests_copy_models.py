# -*- coding: utf-8 -*-
from datetime import datetime
from mock import patch

from django import test
from django.conf import settings
from django.core.management import call_command
from django.db.models import loading

from djangopg.copy import copy, _send_csv_to_postgres
from tests.models import Poll


class ModelTestCase(test.TestCase):
    """
    Helper to create tables for models of given apps.
    """
    apps = ()

    def _pre_setup(self):
        # Add the models to the db.
        self._original_installed_apps = list(settings.INSTALLED_APPS)
        for app in self.apps:
            settings.INSTALLED_APPS.append(app)
        loading.cache.loaded = False
        call_command('syncdb', interactive=False, verbosity=0)
        # Call the original method that does the fixtures etc.
        super(ModelTestCase, self)._pre_setup()

    def _post_teardown(self):
        # Call the original method.
        super(ModelTestCase, self)._post_teardown()
        # Restore the settings.
        settings.INSTALLED_APPS = self._original_installed_apps
        loading.cache.loaded = False


@patch('djangopg.copy._send_csv_to_postgres')
class DuplicatedTestCase(ModelTestCase):
    """Tests for checking duplicated entries.

    Entries being added that already exist in the db, should be skipped.

    """
    apps = ('tests',)

    def setUp(self):
        self.entries = [
            Poll(question="Question1", pub_date=datetime.now()),
            Poll(question="Question2", pub_date=datetime.now()),
        ]

    def test_duplicated_entries(self, pmock):
        for entry in self.entries:
            entry.save()
        self.assertEqual(Poll.objects.count(), 2)
        # Try to add already added entries
        copy(Poll, self.entries, keys=['question'])
        fd = pmock.call_args[0][0]
        lines = fd.readlines()
        self.assertEqual(len(lines), 0)
