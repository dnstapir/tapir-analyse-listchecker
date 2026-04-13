# Configuration example
```toml
debug = true
interval = 120
list_url = "https://example.com/list.txt"
observation = "<OBSERVATION>" # Observation to be set when a domain is found on the list 

[nats]
debug = true
url = "nats://nats:4222"
event_subject = "internal.events.new_qname"
observation_subject_prefix = "internal.observations"

[[nats.observation_buckets]]
name = "<OBSERVATION>"
observation = "example_observation_bucket"
ttl = 10
create = false # A "false" setting requires bucket to be pre-provisioned
```
