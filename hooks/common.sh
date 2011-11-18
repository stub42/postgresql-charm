

database=${JUJU_REMOTE_UNIT///*}
user=${JUJU_REMOTE_UNIT///*}
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


