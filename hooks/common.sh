

database=${JUJU_REMOTE_UNIT///*}
database=${database/-/_}
user=${JUJU_REMOTE_UNIT///*}
user=${user/-/_}
host=`unit-get private-address`

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


