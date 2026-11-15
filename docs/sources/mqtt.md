# MQTT

`kind = "mqtt"` subscribes to topics on an MQTT broker and writes the JSON messages that arrive,
routed to tables by topic filter.

Each route maps an MQTT topic filter to a table. A message whose topic matches becomes one point:

- its fields are the payload's numeric top-level values (a JSON object is expected; text,
  booleans, nulls and nested values are left out), written as floats unless the route sets
  `numbers = "keep"`, or only the keys a route lists in `fields`;
- its tags are the route's, where `{N}` stands for the topic's N-th segment (from 0);
- its time is when the hub received it, at the source's `precision`, unless the route names a
  `timestamp_field` holding Unix seconds.

The session is persistent, under the source's name unless `client_id` is set, so the broker keeps
QoS 1 and 2 messages for the hub while it reconnects. A dropped connection restarts the source
after its back-off. MQTT sources write no `ingest_progress`: a pushed stream has no position to
report.


See the [README](../../README.md#every-source) for what every source shares: credentials, state,
heartbeats and the status page.
