
ip=$(unit-get private-address)
database=${JUJU_REMOTE_UNIT///*}
database=${database//-/_}
user=${JUJU_REMOTE_UNIT///*}
user=${user//-/_}
host=`gethostip $ip | awk '{print $2}'`

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


