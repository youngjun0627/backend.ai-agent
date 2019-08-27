#!/bin/sh
set -e

ubuntu_init_script=$(cat <<'EOF'
#! /bin/sh
set -e
apt-get update
apt-get install -y make gcc
EOF
)

centos_init_script=$(cat <<'EOF'
#! /bin/sh
set -e
yum install -y make gcc
EOF
)

alpine_init_script=$(cat <<'EOF'
#! /bin/sh
set -e
apk add --no-cache make gcc musl-dev
EOF
)


build_script=$(cat <<'EOF'
#! /bin/sh
set -e
./${X_DISTRO}-init.sh
cd su-exec
make
chown -R $X_UID:$X_GID .
cp ./su-exec /root/su-exec
chown $X_UID:$X_GID /root/su-exec
make clean
EOF
)

temp_dir=$(mktemp -d -t suexec-build.XXXXX)
echo "Using temp directory: $temp_dir"
echo "$build_script" > "$temp_dir/build.sh"
echo "$centos_init_script" > "$temp_dir/centos-init.sh"
echo "$ubuntu_init_script" > "$temp_dir/ubuntu-init.sh"
echo "$alpine_init_script" > "$temp_dir/alpine-init.sh"
chmod +x $temp_dir/*.sh

git clone https://github.com/ncopa/su-exec $temp_dir/su-exec

docker run --rm -it \
  -e X_DISTRO=ubuntu \
  -e X_UID=$(id -u) \
  -e X_GID=$(id -g) \
  -w /workspace \
  -v $(pwd):/root \
  -v $temp_dir:/workspace \
  ubuntu:16.04 \
  /workspace/build.sh
mv su-exec "su-exec.ubuntu16.04.bin"

docker run --rm -it \
  -e X_DISTRO=centos \
  -e X_UID=$(id -u) \
  -e X_GID=$(id -g) \
  -w /workspace \
  -v $(pwd):/root \
  -v $temp_dir:/workspace \
  centos:7.6.1810 \
  /workspace/build.sh
mv su-exec "su-exec.centos7.6.bin"

docker run --rm -it \
  -e X_DISTRO=alpine \
  -e X_UID=$(id -u) \
  -e X_GID=$(id -g) \
  -w /workspace \
  -v $(pwd):/root \
  -v $temp_dir:/workspace \
  alpine:3.8 \
  /workspace/build.sh
mv su-exec "su-exec.alpine3.8.bin"

rm -rf "$temp_dir"
