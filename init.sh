#bin/bash

sudo wget https://bootstrap.pypa.io/pip/2.7/get-pip.py
sudo python get-pip.py
# 由于linux rpm包依赖太过复杂，离线安装工作量巨大，这一步最好采用在线安装。
sudo yum install -y python-devel cyrus-sasl-plain  cyrus-sasl-devel  cyrus-sasl-gssapi cyrus-sasl cyrus-sasl-lib gcc-c++ gcc unzip
sudo pip install -r requirements.txt