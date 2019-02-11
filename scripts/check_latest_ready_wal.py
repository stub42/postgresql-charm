#!/usr/bin/python3

import sys

MAX_AGE_FILENAME = '/var/lib/nagios/postgres-wal-e-max-age.txt'


def get_val_from_file(filename):
    with open(filename) as the_file:
        content = the_file.read().strip()
    return int(content)


def make_nice_age(seconds):
    minutes, seconds = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    return "{} days, {} hours, {} minutes and {} seconds".format(days, hours, minutes, seconds)


def main(args=sys.argv):
    NAGIOS_OK = 0
    NAGIOS_WARN = 1
    NAGIOS_FAIL = 2

    try:
        warn_threshold = int(args[1])
    except IndexError:
        print("Syntax error: not enough arguments given")
        return NAGIOS_FAIL

    try:
        crit_threshold = int(args[2])
    except IndexError:
        print("Syntax error: not enough arguments given")
        return NAGIOS_FAIL

    max_age = get_val_from_file(MAX_AGE_FILENAME)
    nice_age = make_nice_age(max_age)
    ret_val = NAGIOS_OK

    if max_age > crit_threshold:
        print("CRITICAL: Last WAL-E backup was {} ago".format(nice_age))
        ret_val = NAGIOS_FAIL
    elif max_age > warn_threshold:
        print("WARNING: Last WAL-E backup was {} ago".format(nice_age))
        ret_val = NAGIOS_WARN
    else:
        print("OK: No stale WAL-E backups found")
    return ret_val


if __name__ == '__main__':
    sys.exit(main(sys.argv))
