# Setup

Preparing Pulsar objects:

```bash
echo "=####### TENANT #######="
./pulsar-admin tenants create rtk

echo "=###### NAMESPACES #######="
./pulsar-admin namespaces create rtk/test001
./pulsar-admin namespaces set-is-allow-auto-update-schema --disable rtk/test001
./pulsar-admin namespaces set-schema-compatibility-strategy --compatibility ALWAYS_COMPATIBLE rtk/test001
./pulsar-admin namespaces set-schema-validation-enforce --enable rtk/test001

./pulsar-admin topics create-partitioned-topic --partitions 1 persistent://rtk/test001/input
./pulsar-admin topics create-subscription persistent://rtk/test001/input -s input-sub

./pulsar-admin topics create-partitioned-topic --partitions 1 persistent://rtk/test001/output

./pulsar-admin schemas upload \
    --filename /pulsar/schemas/Event.json \
    persistent://rtk/test001/input

./pulsar-admin schemas upload \
    --filename /pulsar/schemas/Event.json \
    persistent://rtk/test001/output
```

Schema content:

```json
{"type":"AVRO","schema":"{\"type\":\"record\",\"name\":\"Event\",\"namespace\":\"pl.trojczak.flinkpulsar.model\",\"fields\":[{\"name\":\"id\",\"type\":[\"null\",\"long\"]},{\"name\":\"key\",\"type\":[\"null\",\"string\"]},{\"name\":\"content\",\"type\":[\"null\",\"string\"]},{\"name\":\"action\",\"type\":[\"null\",\"string\"]}]}","properties":{"__alwaysAllowNull":"true","__jsr310ConversionEnabled":"false"}}
```

