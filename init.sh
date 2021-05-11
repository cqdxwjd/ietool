#bin/bash

sudo wget https://bootstrap.pypa.io/pip/2.7/get-pip.py
sudo python get-pip.py
sudo yum install -y python-devel cyrus-sasl-plain  cyrus-sasl-devel  cyrus-sasl-gssapi cyrus-sasl cyrus-sasl-lib gcc-c++ gcc
sudo pip install -r requirements.txt