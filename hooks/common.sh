

database=${ENSEMBLE_REMOTE_UNIT///*}
user=${ENSEMBLE_REMOTE_UNIT///*}
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


