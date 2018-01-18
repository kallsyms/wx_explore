#!/bin/bash

sudo apt update
sudo apt upgrade -y
sudo apt install -y git curl wget vim build-essential \
    libbz2-dev libssl-dev libreadline-dev libsqlite3-dev tk-dev libpng-dev libfreetype6-dev \
    software-properties-common

sudo add-apt-repository -y ppa:ubuntugis/ppa
sudo apt update
sudo apt install -y gdal-bin libgdal-dev \
    libgrib-api-dev

curl -L https://raw.githubusercontent.com/yyuu/pyenv-installer/master/bin/pyenv-installer | bash

cat << EOF >> ~/.bashrc

export PATH="$HOME/.pyenv/bin:\$PATH"
eval "\$(pyenv init -)"
eval "\$(pyenv virtualenv-init -)"
EOF

export PATH="$HOME/.pyenv/bin:$PATH"
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"

pyenv install 3.6.4
pyenv virtualenv 3.6.4 wx_explore
pyenv global wx_explore

pip install --global-option=build_ext --global-option="-I/usr/include/gdal" GDAL==2.1.0
pip install numpy pyproj
pip install -r requirements.txt

echo "Be sure to setup a config in wx_explore/web/config.py"
