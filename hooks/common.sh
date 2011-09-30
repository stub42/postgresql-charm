

database=${JUJU_REMOTE_UNIT///*}
user=${JUJU_REMOTE_UNIT///*}
host=`hostname -f`

admin() {
  [[ $(basename $0) =~ admin ]]
}

get_database_name() {
  if admin; then
    echo "all"
  else
    echo "${database}"
  fi
}


