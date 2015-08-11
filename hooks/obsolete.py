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



postgresql_data_dir = "/var/lib/postgresql"
external_volume_mount = "/srv/data"
