

database=${ENSEMBLE_REMOTE_UNIT///*}
user=${ENSEMBLE_REMOTE_UNIT///*}
host=`hostname -f`

admin() {
  [[ $(basename $0) =~ /admin/ ]]
}

database_name() {
  if admin; then
    "all"
  else
    ${database}
  fi
}


