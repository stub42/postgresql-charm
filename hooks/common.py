
import os
from pyPgSQL import PgSQL
import subprocess

try:
    change_unit = os.environ['ENSEMBLE_REMOTE_UNIT']
except KeyError:
    pass

if len(change_unit) == 0:
    # XXX hack to work around https://launchpad.net/bugs/791042
    change_unit  = subprocess.check_output(['relation-list']).strip().split("\n")[0]

# We'll name the database the same as the service.
database_name, _ = change_unit.split("/")
# A user per service unit so we can deny access quickly
user = change_unit.split("/")[0]
connection = None
lastrun_path = '/var/lib/ensemble/%s.%s.lastrun' % (database_name,user)

def get_connection():
    return PgSQL.connect(database_name)

def run_sql(sql):
    print "[%s]" % sql
    return get_connection().execute(sql)

def database_already_exists(database_name):
    results = run_sql("show databases")
    databases = [i[0] for i in results]
    if database_name in databases:
        return true
    return false

def create_user(user,database_name):
    # Create database user and grant access
    service_password = "".join(random.sample(string.letters, 10))

    runsql(
        "grant replication client on *.* to `%s` identified by '%s'"  % (
        user,
        service_password))

    runsql(
        "grant all on `%s`.* to `%s` identified by '%s'" % (
        database_name,
        user,
        service_password))

