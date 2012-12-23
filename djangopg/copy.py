# -*- coding: utf-8 -*-

import csv
from itertools import repeat
from cStringIO import StringIO
from django.db import connections, transaction
from django.db.models import AutoField

from djangopg.utils import chunker

def _convert_to_csv_form(data):
    """Convert the data to a form suitable for CSV."""
    # NULL values should be an empty column
    if data is None:
        return ''
    # Empty strings should be different to NULL values
    if data == '':
        return '""'
    # CSV needs to be encoded to UTF8
    if isinstance(data, unicode):
        return data.encode('UTF-8')
    return data


def _send_csv_to_postgres(fd, conn, table_name, columns):
    """
    Send the CSV file to PostgreSQL for inserting the entries.

    Use the COPY command for faster insertion and less WAL generation.

    :param fd: A file-like, CSV-formatted object with the data to send.
    :param conn: The connection object.
    """
    columns = map(conn.ops.quote_name, columns)
    cursor = conn.cursor()
    sql = "COPY %s(%s) FROM STDIN WITH CSV"
    try:
        cursor.copy_expert(sql % (table_name, ','.join(columns)), fd)
    finally:
        cursor.close()


def _prep_values(fields, obj, con, add):
    if hasattr(obj, 'presave') and callable(obj.presave):
        obj.presave()

    values = []
    for f in fields:
        field_type = f.get_internal_type()
        if field_type in ('DateTimeField', 'DateField', 'UUIDField'):
            values.append(f.pre_save(obj, add))
        else:
            values.append(f.get_db_prep_save(f.pre_save(obj, add)))
    return tuple(values)


def copy(model, entries, columns=None, keys=None, using='default'):
    """
    Add the given entries to the database using the COPY command.

    The caller is required to handle the transaction.

    :param model: The model class the entries are for.
    :param entries: An iterable of entries to be inserted.
    :param columns: A list of columns that will have to be populated.
        By default, we use all columns but the primary key.
    :param keys: A list of columns that will be checked to avoid duplicated
        entries in the database. If keys is not passed, try to add all
        entries.
    :param using: The database connection to use.
    """
    table_name = model._meta.db_table
    conn = connections[using]

    if keys:
        key_fields = [f for f in model._meta.fields if f.name in keys]
        assert key_fields, "Empty key fields"

        table = model._meta.db_table
        col_names = ",".join(conn.ops.quote_name(f.column) for f in key_fields)
        insert_entries = []
        cursor = conn.cursor()

        for c_entries in chunker(entries, 500):

            object_keys = [
                (o, _prep_values(key_fields, o, conn, False))
                for o in c_entries
                ]
            parameters = [i for (_, k) in object_keys for i in k]

            # repeat tuple values
            tuple_placeholder = "(%s)" % ",".join(repeat("%s", len(key_fields)))
            placeholders = ",".join(repeat(tuple_placeholder, len(c_entries)))

            sql = "SELECT %s FROM %s WHERE (%s) IN (%s)" % (
                col_names, table, col_names, placeholders)
            cursor.execute(sql, parameters)
            existing = set(cursor.fetchall())

            # Find the objects that need to be inserted.
            insert_entries.extend([o for o, k in object_keys if k not in existing])

        entries = insert_entries
        cursor.close()

    if columns is None:
        fields = [f for f in model._meta.fields if not isinstance(f, AutoField)]
        columns = [f.column for f in fields]
    else:
        fields = [f for f in model._meta.fields if f.name in columns]

    # Construct a StringIO from the entries
    fd = StringIO()
    csvf = csv.writer(fd, quotechar="'")
    for entry in entries:
        row = [_convert_to_csv_form(data)
            for data in _prep_values(fields, entry, conn, True)]
        csvf.writerow(row)
    # Move the fp to the beginning of the string
    fd.seek(0)
    _send_csv_to_postgres(fd, conn, table_name, columns)
