#!/usr/bin/python3

import sys


def get_val_from_file(filename):
    with open(filename) as the_file:
        content = the_file.read().strip()
    return content


def make_nice_age(seconds):
    minutes, seconds = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    return "{} days, {} hours, {} minutes and {} seconds".format(days, hours, minutes, seconds)


def main():
    NAGIOS_OK = 0
    NAGIOS_WARN = 1
    NAGIOS_FAIL = 2

    max_age_filename = '/var/lib/nagios/postgres-wal-e-max-age.txt'

    ret_val = NAGIOS_OK

    try:
        warn_threshold = sys.argv[1]
    except IndexError:
        print("Syntax error: not enough arguments given")
        sys.exit(NAGIOS_FAIL)

    try:
        crit_threshold = sys.argv[2]
    except IndexError:
        print("Syntax error: not enough arguments given")
        sys.exit(NAGIOS_FAIL)

    max_age = get_val_from_file(max_age_filename)
    nice_age = make_nice_age(max_age)

    if max_age > crit_threshold:
        print("CRITICAL: Last WAL-E backup was {} ago".format(nice_age))
        ret_val = NAGIOS_FAIL
    elif max_age > warn_threshold:
        print("WARNING: Last WAL-E backup was {} ago".format(nice_age))
        ret_val = NAGIOS_WARN
    else:
        print("OK: No stale WAL-E backups found")
    sys.exit(ret_val)


if __name__ == '__main__':
    main()
