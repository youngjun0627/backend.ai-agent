#! /bin/bash
set -e

arch=$(uname -m)
distros=("ubuntu16.04" "centos7.6" "alpine3.8")

static_libs_dockerfile_part=$(cat <<'EOF'
ENV ZLIB_VER=1.2.11 \
    SSL_VER=1.1.1d

RUN wget https://www.zlib.net/zlib-${ZLIB_VER}.tar.gz -O /root/zlib-${ZLIB_VER}.tar.gz && \
    wget https://www.openssl.org/source/openssl-${SSL_VER}.tar.gz -O /root/openssl-${SSL_VER}.tar.gz

RUN cd /root && \
    tar xzvf zlib-${ZLIB_VER}.tar.gz && \
    tar xzvf openssl-${SSL_VER}.tar.gz

RUN echo "BUILD: zlib" && \
    cd /root/zlib-${ZLIB_VER} && \
    ./configure --prefix=/usr/local --static && \
    make && \
    make install

RUN echo "BUILD: OpenSSL" && \
    cd /root/openssl-${SSL_VER} && \
    ./config --prefix=/usr/local no-shared --openssldir=/usr/local/openssl && \
    make && \
    make install
EOF
)

ubuntu_builder_dockerfile=$(cat <<'EOF'
FROM ubuntu:16.04
RUN apt-get update
RUN apt-get install -y make gcc
RUN apt-get install -y autoconf
RUN apt-get install -y wget
# below required for sys/mman.h
RUN apt-get install -y libc6-dev
EOF
)

centos_builder_dockerfile=$(cat <<'EOF'
FROM centos:7.6.1810
RUN yum install -y make gcc
RUN yum install -y autoconf
RUN yum install -y wget
EOF
)

alpine_builder_dockerfile=$(cat <<'EOF'
FROM alpine:3.8
RUN apk add --no-cache make gcc musl-dev
RUN apk add --no-cache autoconf
RUN apk add --no-cache wget
# below required for sys/mman.h
RUN apk add --no-cache linux-headers
EOF
)

build_script=$(cat <<'EOF'
#! /bin/sh
echo "BUILD: OpenSSH"
cd /workspace/openssh-portable
autoreconf
export LDFLAGS="-L/root/zlib-${ZLIB_VER} -L/root/openssl-${SSL_VER} -pthread"
export LIBS="-ldl"
sed -i "s/-lcrypto/-l:libcrypto.a/" ./configure
sed -i "s/-lz/-l:libz.a/" ./configure
./configure --prefix=/usr/local
make sftp-server scp
cp sftp-server ../sftp-server.$X_DISTRO.$X_ARCH.bin
cp scp ../scp.$X_DISTRO.$X_ARCH.bin
make clean
EOF
)

SCRIPT_DIR=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
temp_dir=$(mktemp -d -t sftpserver-build.XXXXX)
echo "Using temp directory: $temp_dir"
echo "$build_script" > "$temp_dir/build.sh"
chmod +x $temp_dir/*.sh
echo -e "$ubuntu_builder_dockerfile\n$static_libs_dockerfile_part" > "$SCRIPT_DIR/sftpserver-builder.ubuntu16.04.dockerfile"
echo -e "$centos_builder_dockerfile\n$static_libs_dockerfile_part" > "$SCRIPT_DIR/sftpserver-builder.centos7.6.dockerfile"
echo -e "$alpine_builder_dockerfile\n$static_libs_dockerfile_part" > "$SCRIPT_DIR/sftpserver-builder.alpine3.8.dockerfile"

for distro in "${distros[@]}"; do
  docker build -t sftpserver-builder:$distro \
    -f $SCRIPT_DIR/sftpserver-builder.$distro.dockerfile $SCRIPT_DIR
done

cd "$temp_dir"
git clone -c advice.detachedHead=false --branch "V_8_1_P1" https://github.com/openssh/openssh-portable openssh-portable

for distro in "${distros[@]}"; do
  docker run --rm -it \
    -e X_DISTRO=$distro \
    -e X_ARCH=$arch \
    -u $(id -u):$(id -g) \
    -w /workspace \
    -v $temp_dir:/workspace \
    sftpserver-builder:$distro \
    /workspace/build.sh
done

ls -l .
cp sftp-server.*.bin $SCRIPT_DIR/../src/ai/backend/runner
cp scp.*.bin $SCRIPT_DIR/../src/ai/backend/runner

cd $SCRIPT_DIR/..
rm -rf "$temp_dir"
