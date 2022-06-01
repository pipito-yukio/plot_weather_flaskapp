#!/bin/bash

# ./start.sh                    -> development
# ./start.sh prod | production  ->production

export PATH_WEATHER_DB=$HOME/Examples/python/Github/plot_weather_flaskapp/db/weather.db

env_mode="development"
if [ $# -eq 0 ]; then
    :
else
   if [[ "$1" = "prod" || "$1" = "production" ]]; then 
        env_mode="production"
   fi
fi

host_name="$(/bin/cat /etc/hostname)"
IP_HOST_ORG="${host_name}.local"   # ADD host suffix ".local"
export IP_HOST="${IP_HOST_ORG,,}"  # to lowercase
export FLASK_ENV=$env_mode
echo "$IP_HOST with $FLASK_ENV"

. $HOME/py_venv/py_flask/bin/activate
python run.py

deactivate
