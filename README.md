### NSQ
```bash
# Run the service
cargo run
# Publish message
curl -d "@tests/message.json" http://nsqd-1:4151/pub?topic=vagrant_backend-rust # Leveraged funds
curl -d "@tests/message_3.json" http://nsqd-1:4151/pub?topic=vagrant_backend-rust # Earning
# Get Messagse
# In any nsq container, run nsq_tail
nsq_tail --lookupd-http-address=nsqlookupd-1:4161 --topic=training-node
```