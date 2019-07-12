#!/bin/sh

echo "/opt/backend.ai/lib" > /etc/ld.so.conf.d/backendai.conf
ldconfig

USER_ID=${LOCAL_USER_ID:-9001}
USER_NAME=work
echo "Starting with user $USER_NAME ($USER_ID)"
if [ -f /bin/ash ]; then  # for alpine
  addgroup -g $USER_ID -S $USER_NAME
  adduser -s /bin/ash -h "/home/$USER_NAME" -H -S -u $USER_ID -G $USER_NAME -g "User" $USER_NAME
else
  useradd -s /bin/bash -d "/home/$USER_NAME" -M -r -u $USER_ID -U -o -c "User" $USER_NAME
fi
export HOME="/home/$USER_NAME"

# Make the agent communication socket accessible by the user
chown work /opt/backend.ai/agent.sock

exec /opt/backend.ai/bin/su-exec $USER_NAME "$@"
