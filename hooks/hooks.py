#!/usr/bin/env python
# vim: et ai ts=4 sw=4:

hooks = hookenv.Hooks()


def volume_get_all_mounted():
    command = ("mount |egrep %s" % external_volume_mount)
    status, output = commands.getstatusoutput(command)
    if status != 0:
        return None
    return output


    write_metrics_cronjob('/usr/local/bin/postgres_to_statsd.py',
                          '/etc/cron.d/postgres_metrics')


@hooks.hook()
def install(run_pre=True, force_restart=True):
    config_data = hookenv.config()
    update_repos_and_packages()
    if 'state' not in local_state:
        log('state not in {}'.format(local_state.keys()), DEBUG)
        # Fresh installation. Because this function is invoked by both
        # the install hook and the upgrade-charm hook, we need to guard
        # any non-idempotent setup. We should probably fix this; it
        # seems rather fragile.
        local_state.setdefault('state', 'standalone')
        log(repr(local_state.keys()), DEBUG)

        # Drop the cluster created when the postgresql package was
        # installed, and rebuild it with the requested locale and encoding.
        version = pg_version()
        for ver, name in lsclusters(slice(2)):
            if version == ver and name == 'main':
                run("pg_dropcluster --stop {} main".format(version))
        listen_port = config_data.get('listen_port', None)
        if listen_port:
            port_opt = "--port={}".format(config_data['listen_port'])
        else:
            port_opt = ''
        createcluster()
        assert (
            not port_opt
            or get_service_port() == config_data['listen_port']), (
            'allocated port {!r} != {!r}'.format(
                get_service_port(), config_data['listen_port']))
        local_state['port'] = get_service_port()
        log('publishing state', DEBUG)
        local_state.publish()

    postgresql_backups_dir = (
        config_data['backup_dir'].strip() or
        os.path.join(postgresql_data_dir, 'backups'))

    host.mkdir(postgresql_backups_dir, owner="postgres", perms=0o755)
    host.mkdir(postgresql_scripts_dir, owner="postgres", perms=0o755)
    host.mkdir(postgresql_logs_dir, owner="postgres", perms=0o755)
    paths = {
        'base_dir': postgresql_data_dir,
        'backup_dir': postgresql_backups_dir,
        'scripts_dir': postgresql_scripts_dir,
        'logs_dir': postgresql_logs_dir,
    }
    charm_dir = hookenv.charm_dir()
    template_file = "{}/templates/pg_backup_job.tmpl".format(charm_dir)
    backup_job = Template(open(template_file).read()).render(paths)
    host.write_file(
        os.path.join(postgresql_scripts_dir, 'dump-pg-db'),
        open('scripts/pgbackup.py', 'r').read(), perms=0o755)
    host.write_file(
        os.path.join(postgresql_scripts_dir, 'pg_backup_job'),
        backup_job, perms=0755)
    install_postgresql_crontab(postgresql_crontab)

    # Create this empty log file on installation to avoid triggering
    # spurious monitoring system alerts, per Bug #1329816.
    if not os.path.exists(backup_log):
        host.write_file(backup_log, '', 'postgres', 'postgres', 0664)

    hookenv.open_port(get_service_port())

    # Ensure at least minimal access granted for hooks to run.
    # Reload because we are using the default cluster setup and started
    # when we installed the PostgreSQL packages.
    config_changed(force_restart=force_restart)

    snapshot_relations()


@hooks.hook()
def upgrade_charm():
    """Handle saving state during an upgrade-charm hook.

    When upgrading from an installation using volume-map, we migrate
    that installation to use the storage subordinate charm by remounting
    a mountpath that the storage subordinate maintains. We exit(1) only to
    raise visibility to manual procedure that we log in juju logs below for the
    juju admin to finish the migration by relating postgresql to the storage
    and block-storage-broker services. These steps are generalised in the
    README as well.
    """
    install(run_pre=False, force_restart=False)
    snapshot_relations()
    version = pg_version()
    cluster_name = hookenv.config('cluster_name')
    data_directory_path = os.path.join(
        postgresql_data_dir, version, cluster_name)
    if (os.path.islink(data_directory_path)):
        link_target = os.readlink(data_directory_path)
        if "/srv/juju" in link_target:
            # Then we just upgraded from an installation that was using
            # charm config volume_map definitions. We need to stop postgresql
            # and remount the device where the storage subordinate expects to
            # control the mount in the future if relations/units change
            volume_id = link_target.split("/")[3]
            unit_name = hookenv.local_unit()
            new_mount_root = external_volume_mount
            new_pg_version_cluster_dir = os.path.join(
                new_mount_root, "postgresql", version, cluster_name)
            if not os.exists(new_mount_root):
                os.mkdir(new_mount_root)
            log("\n"
                "WARNING: %s unit has external volume id %s mounted via the\n"
                "deprecated volume-map and volume-ephemeral-storage\n"
                "configuration parameters.\n"
                "These parameters are no longer available in the postgresql\n"
                "charm in favor of using the volume_map parameter in the\n"
                "storage subordinate charm.\n"
                "We are migrating the attached volume to a mount path which\n"
                "can be managed by the storage subordinate charm. To\n"
                "continue using this volume_id with the storage subordinate\n"
                "follow this procedure.\n-----------------------------------\n"
                "1. cat > storage.cfg <<EOF\nstorage:\n"
                "  provider: block-storage-broker\n"
                "  root: %s\n"
                "  volume_map: \"{%s: %s}\"\nEOF\n2. juju deploy "
                "--config storage.cfg storage\n"
                "3. juju deploy block-storage-broker\n4. juju add-relation "
                "block-storage-broker storage\n5. juju resolved --retry "
                "%s\n6. juju add-relation postgresql storage\n"
                "-----------------------------------\n" %
                (unit_name, volume_id, new_mount_root, unit_name, volume_id,
                 unit_name), WARNING)
            postgresql_stop()
            os.unlink(data_directory_path)
            log("Unmounting external storage due to charm upgrade: %s" %
                link_target)
            try:
                subprocess.check_output(
                    "umount /srv/juju/%s" % volume_id, shell=True)
                # Since e2label truncates labels to 16 characters use only the
                # first 16 characters of the volume_id as that's what was
                # set by old versions of postgresql charm
                subprocess.check_call(
                    "mount -t ext4 LABEL=%s %s" %
                    (volume_id[:16], new_mount_root), shell=True)
            except subprocess.CalledProcessError, e:
                log("upgrade-charm mount migration failed. %s" % str(e), ERROR)
                sys.exit(1)

            log("NOTICE: symlinking {} -> {}".format(
                new_pg_version_cluster_dir, data_directory_path))
            os.symlink(new_pg_version_cluster_dir, data_directory_path)
            run("chown -h postgres:postgres {}".format(data_directory_path))
            postgresql_start()  # Will exit(1) if issues
            log("Remount and restart success for this external volume.\n"
                "This current running installation will break upon\n"
                "add/remove postgresql units or relations if you do not\n"
                "follow the above procedure to ensure your external\n"
                "volumes are preserved by the storage subordinate charm.",
                WARNING)
            # So juju admins can see the hook fail and note the steps to fix
            # per our WARNINGs above
            sys.exit(1)


def slave_count():
    num_slaves = 0
    for relid in hookenv.relation_ids('replication'):
        num_slaves += len(hookenv.related_units(relid))
    for relid in hookenv.relation_ids('master'):
        num_slaves += len(hookenv.related_units(relid))
    return num_slaves


@hooks.hook('nrpe-external-master-relation-changed')
def update_nrpe_checks():
    config_data = hookenv.config()
    try:
        nagios_uid = getpwnam('nagios').pw_uid
        nagios_gid = getgrnam('nagios').gr_gid
    except Exception:
        hookenv.log("Nagios user not set up.", hookenv.DEBUG)
        return

    try:
        nagios_password = create_user('nagios')
        pg_pass_entry = '*:*:*:nagios:%s' % (nagios_password)
        with open('/var/lib/nagios/.pgpass', 'w') as target:
            os.fchown(target.fileno(), nagios_uid, nagios_gid)
            os.fchmod(target.fileno(), 0400)
            target.write(pg_pass_entry)
    except psycopg2.InternalError:
        if config_data['manual_replication']:
            log("update_nrpe_checks(): manual_replication: "
                "ignoring psycopg2.InternalError caught creating 'nagios' "
                "postgres role; assuming we're already replicating")
        else:
            raise

    relids = hookenv.relation_ids('nrpe-external-master')
    relations = []
    for relid in relids:
        for unit in hookenv.related_units(relid):
            relations.append(hookenv.relation_get(unit=unit, rid=relid))

    if len(relations) == 1 and 'nagios_hostname' in relations[0]:
        nagios_hostname = relations[0]['nagios_hostname']
        log("update_nrpe_checks: Obtained nagios_hostname ({}) "
            "from nrpe-external-master relation.".format(nagios_hostname))
    else:
        unit = hookenv.local_unit()
        unit_name = unit.replace('/', '-')
        nagios_hostname = "%s-%s" % (config_data['nagios_context'], unit_name)
        log("update_nrpe_checks: Deduced nagios_hostname ({}) from charm "
            "config (nagios_hostname not found in nrpe-external-master "
            "relation, or wrong number of relations "
            "found)".format(nagios_hostname))

    nrpe_service_file = \
        '/var/lib/nagios/export/service__{}_check_pgsql.cfg'.format(
            nagios_hostname)
    nagios_logdir = '/var/log/nagios'
    if not os.path.exists(nagios_logdir):
        os.mkdir(nagios_logdir)
        os.chown(nagios_logdir, nagios_uid, nagios_gid)
    for f in os.listdir('/var/lib/nagios/export/'):
        if re.search('.*check_pgsql.cfg', f):
            os.remove(os.path.join('/var/lib/nagios/export/', f))

    # --- exported service configuration file
    servicegroups = [config_data['nagios_context']]
    additional_servicegroups = config_data['nagios_additional_servicegroups']
    if additional_servicegroups != '':
        servicegroups.extend(
            servicegroup.strip() for servicegroup
            in additional_servicegroups.split(',')
        )
    templ_vars = {
        'nagios_hostname': nagios_hostname,
        'nagios_servicegroup': ', '.join(servicegroups),
    }
    template = render_template('nrpe_service.tmpl', templ_vars)
    with open(nrpe_service_file, 'w') as nrpe_service_config:
        nrpe_service_config.write(str(template))

    # --- nrpe configuration
    # pgsql service
    nrpe_check_file = '/etc/nagios/nrpe.d/check_pgsql.cfg'
    with open(nrpe_check_file, 'w') as nrpe_check_config:
        nrpe_check_config.write("# check pgsql\n")
        nrpe_check_config.write(
            "command[check_pgsql]=/usr/lib/nagios/plugins/check_pgsql -P {}"
            .format(get_service_port()))
    # pgsql backups
    nrpe_check_file = '/etc/nagios/nrpe.d/check_pgsql_backups.cfg'
    # XXX: these values _should_ be calculated from the backup schedule
    #      perhaps warn = backup_frequency * 1.5, crit = backup_frequency * 2
    warn_age = 172800
    crit_age = 194400
    with open(nrpe_check_file, 'w') as nrpe_check_config:
        nrpe_check_config.write("# check pgsql backups\n")
        nrpe_check_config.write(
            "command[check_pgsql_backups]=/usr/lib/nagios/plugins/\
check_file_age -w {} -c {} -f {}".format(warn_age, crit_age, backup_log))

    if os.path.isfile('/etc/init.d/nagios-nrpe-server'):
        host.service_reload('nagios-nrpe-server')


@hooks.hook('data-relation-changed')
def data_relation_changed():
    """Listen for configured mountpoint from storage subordinate relation"""
    if not hookenv.relation_get("mountpoint"):
        hookenv.log("Waiting for mountpoint from the relation: %s"
                    % external_volume_mount, hookenv.DEBUG)
    else:
        hookenv.log("Storage ready and mounted", hookenv.DEBUG)
        config_changed(mount_point=external_volume_mount)


@hooks.hook('data-relation-joined')
def data_relation_joined():
    """Request mountpoint from storage subordinate by setting mountpoint"""
    hookenv.log("Setting mount point in the relation: %s"
                % external_volume_mount, hookenv.DEBUG)
    hookenv.relation_set(mountpoint=external_volume_mount)


@hooks.hook('data-relation-departed')
def stop_postgres_on_data_relation_departed():
    hookenv.log("Data relation departing. Stopping PostgreSQL",
                hookenv.DEBUG)
    postgresql_stop()


@hooks.hook('master-relation-joined', 'master-relation-changed')
def master_relation_joined_changed():
    local_relation = hookenv.relation_get(unit=hookenv.local_unit())

    # Relation settings both master and standbys can set now.
    allowed_units = sorted(hookenv.related_units())  # Bug #1458754
    hookenv.relation_set(
        relation_settings={'allowed-units': ' '.join(allowed_units),
                           'host': hookenv.unit_private_ip(),
                           'port': get_service_port(),
                           'state': local_state['state'],
                           'version': pg_version()})

    if local_state['state'] == 'hot standby':
        # Hot standbys cannot create credentials. Publish them from the
        # master if they are available, or defer until a peer-relation-changed
        # hook when they are.
        publish_hot_standby_credentials()
        config_changed()
        return

    user = local_relation.get('user') or user_name(hookenv.relation_id(),
                                                   hookenv.remote_unit())
    password = local_relation.get('password') or create_user(user,
                                                             admin=True,
                                                             replication=True)
    hookenv.relation_set(user=user, password=password)

    # For logical replication, the standby service may request an explicit
    # database.
    database = hookenv.relation_get('database')
    if database:
        ensure_database(user, user, database)
        hookenv.relation_set(database=database)  # Signal database is ready

    # We may need to bump the number of replication connections and
    # restart, and we will certainly need to regenerate pg_hba.conf
    # and reload.
    config_changed()  # Must be called after db & user are created.


@hooks.hook()
def master_relation_departed():
    config_changed()
    allowed_units = hookenv.relation_get('allowed-units',
                                         hookenv.local_unit()).split()
    if hookenv.remote_unit() in allowed_units:
        allowed_units.remove(hookenv.remote_unit())
    hookenv.relation_set(relation_settings={
        'allowed-units': ' '.join(allowed_units)})


def _get_postgresql_config_dir(config_data=None):
    """ Return the directory path of the postgresql configuration files. """
    if config_data is None:
        config_data = hookenv.config()
    version = pg_version()
    cluster_name = config_data['cluster_name']
    return os.path.join("/etc/postgresql", version, cluster_name)


###############################################################################
# Global variables
###############################################################################
postgresql_data_dir = "/var/lib/postgresql"
postgresql_scripts_dir = os.path.join(postgresql_data_dir, 'scripts')
postgresql_logs_dir = os.path.join(postgresql_data_dir, 'logs')

postgresql_sysctl = "/etc/sysctl.d/50-postgresql.conf"
postgresql_crontab = "/etc/cron.d/postgresql"
postgresql_service_config_dir = "/var/run/postgresql"
local_state = State('local_state.pickle')
hook_name = os.path.basename(sys.argv[0])
juju_log_dir = "/var/log/juju"
external_volume_mount = "/srv/data"

backup_log = os.path.join(postgresql_logs_dir, "backups.log")

