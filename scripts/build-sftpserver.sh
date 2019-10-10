#! /bin/bash
set -e

arch=$(uname -m)
distros=("ubuntu16.04" "centos7.6" "alpine3.8")

ubuntu_builder_dockerfile=$(cat <<'EOF'
FROM ubuntu:16.04
RUN apt-get update
RUN apt-get install -y make gcc
RUN apt-get install -y libssl-dev autoconf
EOF
)

centos_builder_dockerfile=$(cat <<'EOF'
FROM centos:7.6.1810
RUN yum install -y make gcc
RUN yum install -y openssl-devel autoconf
EOF
)

alpine_builder_dockerfile=$(cat <<'EOF'
FROM alpine:3.8
RUN apk add --no-cache make gcc musl-dev
RUN apk add --no-cache openssl-dev autoconf
EOF
)

build_script=$(cat <<'EOF'
#! /bin/sh
set -e
cd openssh-portable
autoreconf
./configure
make sftp-server
cp sftp-server ../sftp-server.$X_DISTRO.$X_ARCH.bin
make clean
EOF
)

SCRIPT_DIR=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
temp_dir=$(mktemp -d -t sftpserver-build.XXXXX)
echo "Using temp directory: $temp_dir"
echo "$build_script" > "$temp_dir/build.sh"
chmod +x $temp_dir/*.sh
echo "$ubuntu_builder_dockerfile" > "$SCRIPT_DIR/sftpserver-builder.ubuntu16.04.dockerfile"
echo "$centos_builder_dockerfile" > "$SCRIPT_DIR/sftpserver-builder.centos7.6.dockerfile"
echo "$alpine_builder_dockerfile" > "$SCRIPT_DIR/sftpserver-builder.alpine3.8.dockerfile"

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

cd $SCRIPT_DIR/..
rm -rf "$temp_dir"
