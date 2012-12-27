# Django PostgreSQL Extensions

This package tries to expose functionality from PostgreSQL to django
applications.


Python 2.6 or greater is required.


## Running the tests

Just run

    django-admin.py test --settings=djangopg.test_settings

from the root directory.

If you are running it within a virtualenv, it might be necessary to 
make your top-level dir of the repo available in your $PYTHONPATH 
with the following command, if you are in the top-level dir:

    add2virtualenv `pwd`

