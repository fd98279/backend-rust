NSQD_ADMIN_HOST_NAME=${NSQD_ADMIN_HOST_NAME}
NSQD_ADMIN_AUTH_TOKEN=${NSQD_ADMIN_AUTH_TOKEN}
TOPIC_NAME=${TOPIC_NAME}
if [ -n "$NSQD_ADMIN_HOST_NAME" ]; then
curl "http://${NSQD_ADMIN_HOST_NAME}/api/topics/${TOPIC_NAME}/${TOPIC_NAME}" -k \
    -X 'GET' -H 'Accept: application/vnd.nsq; version=1.0' \
    -H "Authorization: Basic ${NSQD_ADMIN_AUTH_TOKEN}" \
    --silent | grep `hostname` > /dev/null;
    if [ 0 != $? ]; then echo "Health check failed!!!" && exit 1; else echo "Health check passed!!!!"; fi ;
else
  echo "Health check not performed!"
fi
