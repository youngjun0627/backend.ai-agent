#!/bin/sh

USER_ID=${LOCAL_USER_ID:-9001}
GROUP_ID=${LOCAL_GROUP_ID:-9001}
USER_NAME=work
echo "Setting up user/group: $USER_NAME ($USER_ID:$GROUP_ID)"
if [ -f /bin/ash ]; then  # for alpine
  export LD_LIBRARY_PATH="/opt/backend.ai/lib:$LD_LIBRARY_PATH"
  addgroup -g $GROUP_ID $USER_NAME
  adduser -s /bin/ash -h "/home/$USER_NAME" -H -D -u $USER_ID -G $USER_NAME -g "User" $USER_NAME
  export SHELL=/bin/ash
else
  export LD_LIBRARY_PATH="/opt/backend.ai/lib:$LD_LIBRARY_PATH"
  groupadd -g $GROUP_ID $USER_NAME
  useradd -s /bin/bash -d "/home/$USER_NAME" -M -r -u $USER_ID -g $USER_NAME -o -c "User" $USER_NAME
  export SHELL=/bin/bash
fi
export HOME="/home/$USER_NAME"

# Invoke image-specific bootstrap hook.
if [ -x "/opt/container/bootstrap.sh" ]; then
  /opt/container/bootstrap.sh
fi

# Correct the ownership of agent socket.
chown work:work /opt/kernel/agent.sock

echo "Done, executing the main program..."
exec /opt/kernel/su-exec $USER_NAME:$USER_NAME "$@"
