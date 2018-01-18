#!/bin/bash

sudo add-apt-repository ppa:ubuntugis/ppa
sudo apt update
sudo apt upgrade -y
sudo apt install -y git curl wget vim build-essential libbz2-dev libssl-dev libreadline-dev libsqlite3-dev tk-dev libpng-dev libfreetype6-dev \
    software-properties-common \
    gdal-bin libgdal-dev \
    libgrib-api-dev

curl -L https://raw.githubusercontent.com/yyuu/pyenv-installer/master/bin/pyenv-installer | bash
pyenv install 3.6.4
pyenv virtualenv 3.6.4 wx_explore
pyenv global wx_explore

cat << EOF >> ~/.bash_profile
export PATH="$HOME/.pyenv/bin:\$PATH"
eval "\$(pyenv init -)"
eval "\$(pyenv virtualenv-init -)"
EOF

source ~/.bash_profile

pip install --global-option=build_ext --global-option="-I/usr/include/gdal" GDAL==2.1.0
pip install numpy pyproj
pip install -r requirements.txt
