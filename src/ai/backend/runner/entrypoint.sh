#! /bin/sh

USER_ID=${LOCAL_USER_ID:-9001}
GROUP_ID=${LOCAL_GROUP_ID:-9001}

if [ $USER_ID -eq 0 ]; then

  echo "WARNING: Running the user codes as root is not recommended."
  if [ -f /bin/ash ]; then  # for alpine
    export SHELL=/bin/ash
  else
    export SHELL=/bin/bash
  fi
  export LD_LIBRARY_PATH="/opt/backend.ai/lib:$LD_LIBRARY_PATH"
  export HOME="/home/work"

  # Invoke image-specific bootstrap hook.
  if [ -x "/opt/container/bootstrap.sh" ]; then
    /opt/container/bootstrap.sh
  fi
  echo "Executing the main program..."
  exec "$@"

else

  USER_NAME=work
  echo "Setting up user/group: $USER_NAME ($USER_ID:$GROUP_ID)"
  GROUP_NAME=$(getent group $GROUP_ID | cut -d: -f1)
  if [ -f /bin/ash ]; then  # for alpine (busybox)
    if [ -z "$GROUP_NAME" ]; then
      GROUP_NAME=work
      addgroup -g $GROUP_ID $GROUP_NAME
    fi
    adduser -s /bin/ash -h "/home/$USER_NAME" -H -D -u $USER_ID -G $GROUP_NAME -g "User" $USER_NAME
    export SHELL=/bin/ash
  else
    if [ -z "$GROUP_NAME" ]; then
      GROUP_NAME=work
      groupadd -g $GROUP_ID $GROUP_NAME
    fi
    useradd -s /bin/bash -d "/home/$USER_NAME" -M -r -u $USER_ID -g $GROUP_NAME -o -c "User" $USER_NAME
    export SHELL=/bin/bash
  fi
  export LD_LIBRARY_PATH="/opt/backend.ai/lib:$LD_LIBRARY_PATH"
  export HOME="/home/$USER_NAME"

  # Invoke image-specific bootstrap hook.
  if [ -x "/opt/container/bootstrap.sh" ]; then
    /opt/container/bootstrap.sh
  fi

  # Correct the ownership of agent socket.
  chown $USER_ID:$GROUP_ID /opt/kernel/agent.sock

  echo "Executing the main program..."
  exec /opt/kernel/su-exec $USER_ID:$GROUP_ID "$@"

fi
