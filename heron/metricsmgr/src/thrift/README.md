## install thrift 0.9.3 

```
sudo apt-get install automake libssl-dev byacc bison flex libevent-dev -y
git clone https://github.com/apache/thrift.git 
cd thrift && git checkout 0.9.3
./bootstrap.sh 
./configure --with-boost=/bin --libdir=/usr/lib --without-java --without-python
make
sudo make insall
```
