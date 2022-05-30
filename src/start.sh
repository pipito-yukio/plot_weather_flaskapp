#!/bin/bash

# ./start.sh                    -> development
# ./start.sh prod | production  ->production
env_mode="development"
if [ $# -eq 0 ]; then
    :
else
   if [[ "$1" = "prod" || "$1" = "production" ]]; then 
        env_mode="production"
   fi
fi

host_name="$(/bin/cat /etc/hostname)"
export IP_HOST="${host_name}.local"
export ENV=$env_mode
echo "$IP_HOST with $ENV"

# EXEC_PATH=
# if [ -n "$PATH_WEBAPP" ]; then
#    EXEC_PATH=$PATH_WEBAPP
# else
#    EXEC_PATH=$HOME/webapp/
# fi

. $HOME/py_venv/py37_flask/bin/activate
python run.py

deactivate
