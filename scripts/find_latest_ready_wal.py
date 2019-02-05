#!/usr/bin/python3

import glob
import os
import time


def file_age(filepath):
    return time.time() - os.path.getmtime(filepath)


def main():
    max_seen_age = 0
    ready_files = glob.glob('/var/lib/postgresql/*/*/pg_{xlog,wal}/archive_status/*.ready')
    max_age_filename = '/var/lib/nagios/postgres-wal-e-max-age.txt'

    for ready_file in ready_files:
        this_age = file_age(ready_file)
        if this_age > max_seen_age:
            max_seen_age = this_age

    with open(max_age_filename, 'w') as max_age_file:
        max_age_file.write('{}\n'.format(max_seen_age))


if __name__ == '__main__':
    main()
