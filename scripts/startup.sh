$app_path = /~/money-monitor/

echo 'Installing dependencies ...'
sudo apt-get install golang python-pip python-dev build-essential git tmux

pip install $app_path/requirements.txt
go get -u -f https://github.com/DarthSim/overmind
echo 'Loading enviroment ...'

echo 'Starting server ...'
overmind start -f $app_path/Procfile
