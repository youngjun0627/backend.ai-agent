#!/bin/sh

echo "/opt/backend.ai/lib" > /etc/ld.so.conf.d/backendai.conf
ldconfig

USER_ID=${LOCAL_USER_ID:-9001}
GROUP_ID=${LOCAL_GROUP_ID:-9001}
USER_NAME=work
echo "Setting up user/group: $USER_NAME ($USER_ID:$GROUP_ID)"
if [ -f /bin/ash ]; then  # for alpine
  addgroup -g $GROUP_ID -S $USER_NAME
  adduser -s /bin/ash -h "/home/$USER_NAME" -H -S -u $USER_ID -G $USER_NAME -g "User" $USER_NAME
else
  useradd -s /bin/bash -d "/home/$USER_NAME" -M -r -u $USER_ID -U -o -c "User" $USER_NAME
fi
export HOME="/home/$USER_NAME"

# Update ownership of the agent communication socket
chown work:work /opt/kernel/agent.sock

echo "Done, executing the main program..."
exec /opt/kernel/su-exec $USER_NAME:$USER_NAME "$@"
