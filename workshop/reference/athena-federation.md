# Athena Federated Query: one SQL engine, every store

The workshop lands the **same** pump-rate / node-telemetry metric in several
places on purpose — a live push tier, a continuous-aggregate tier, a lakehouse
tier — so you can compare freshness and cost side by side. The downside of that
spread is that answering "what does store X actually hold right now?" normally
means learning X's own query dialect and connecting to it directly.

**Amazon Athena Federated Query** collapses that: it runs ordinary SQL in Athena
against data that lives *outside* S3 by invoking a per-source **data source
connector** (an AWS Lambda function) that translates between Athena and the
store. One `SELECT` can `JOIN` the Iceberg lakehouse table against live rows in
TimescaleDB and raw messages still on a Kafka topic.

## What is federated here

| Catalog (in Athena) | Store | Connector | How it's reachable |
|---|---|---|---|
| `awsdatacatalog` → `workshop_telemetry.telemetry` | Iceberg on S3 | *native* (no connector) | S3 — always |
| `timescaledb_cloud` | Cloud TimescaleDB (`sensor_readings`) | Amazon Athena **PostgreSQL** connector | internal NLB → `timescaledb.workshop.internal` |
| `msk` | Shared MSK topics `raw.telemetry`, `sensors.raw.sim` | Amazon Athena **Kafka** connector | MSK SASL/SCRAM bootstrap brokers |

Two stores are **deliberately out of scope**: cloud **RisingWave** (the
PostgreSQL JDBC connector's `pg_catalog`/`information_schema` introspection is
unproven against RisingWave's partial system-catalog support) and **Timestream
for InfluxDB** (no prebuilt Athena connector exists — it would need a custom
Query Federation SDK connector). Both carry the same data that is already
federated through the cloud TimescaleDB and MSK tiers.

All three catalogs are **account-shared** (one connector set for the whole
workshop); per-slot isolation stays, as everywhere else, through the
`deployment_id` column/field.

## How to query it

Everything runs through the shared `workshop-shared` Athena workgroup, so the
same `scripts/athena-query.sh` wrapper and console you already use for the
Iceberg table work unchanged — you just prefix a table with its catalog.

```sql
-- Live rows from cloud TimescaleDB for one slot
SELECT sensor, value, unit, partition_time
FROM "timescaledb_cloud"."edge"."sensor_readings"
WHERE deployment_id = 'ws-slot00'
ORDER BY partition_time DESC
LIMIT 20;
```

```sql
-- Raw node telemetry still on the Kafka topic (no database hop)
SELECT thing_name, cpu_pct, mem_used_pct, ingest_ts
FROM "msk"."athena_msk"."raw_telemetry"
WHERE deployment_id = 'ws-slot00'
LIMIT 20;
```

```sql
-- Cross-store JOIN: lakehouse history ⋈ live hot tier, in one engine
SELECT t.thing_name,
       t.cpu_pct      AS live_cpu,
       ice.cpu_pct    AS archived_cpu
FROM "msk"."athena_msk"."raw_telemetry" t
JOIN "awsdatacatalog"."workshop_telemetry"."telemetry" ice
  ON ice.thing_name = t.thing_name
WHERE t.deployment_id = 'ws-slot00'
  AND ice.deployment_id = 'ws-slot00'
LIMIT 20;
```

!!! tip "Push filters down"
    Both connectors push predicates and `LIMIT` into the source, so a
    `WHERE deployment_id = 'ws-slot00'` keeps the Lambda from scanning (and
    spilling) the whole table/topic. Always scope by slot.

## How it's wired (infrastructure)

The connectors, their Glue Schema Registry (which maps each Kafka topic to a
table), the Athena data catalogs, the private hosted zone, and the Lambda
security group are all defined once in the shared platform stack. The TimescaleDB
reachability (internal NLB + the `timescaledb.workshop.internal` record) is
completed by the cloud-analytics deploy script, because the NLB's DNS name is
only known after Kubernetes provisions it.

??? example "View source — the Athena federation block (CDK)"
    [:simple-github: Open in GitHub](https://github.com/aws-samples/sample-edge-to-cloud-digital-ops-workshop/blob/main/amplify/custom/platform-stack.ts){ .md-button target=_blank }

    ```typescript
    --8<-- "amplify/custom/platform-stack.ts:athena-federation"
    ```

### Networking, in one paragraph

MSK already admits the cloud VPC CIDR, so the Kafka connector Lambda reaches the
brokers directly. TimescaleDB runs as a CloudNativePG **ClusterIP** on the EKS
pod network, which a VPC-attached Lambda cannot route to — so the cloud-analytics
chart exposes the primary through an **internal NLB** (provisioned by the in-tree
EKS load balancer, no AWS Load Balancer Controller required), and the connector
dials the stable `timescaledb.workshop.internal` Route53 name bound to that NLB.
The EKS cluster security group is opened to the cloud VPC CIDR on the NLB
node-port range so both query traffic and NLB health checks land.

## Operational notes

- **Deploy-role IAM.** Deploying the connectors reads them from the AWS
  Serverless Application Repository, so the deploying principal needs
  `serverlessrepo:CreateCloudFormationChangeSet` / `GetApplication` and
  permission to the published connector apps, in addition to the usual
  CloudFormation/Lambda/IAM permissions.
- **First-query cold start.** Each connector is a Lambda; the first federated
  query after an idle period pays a cold start (and, for TimescaleDB, a JDBC
  connect). Subsequent queries are warm.
- **Credentials.** The PostgreSQL connector reads a Secrets Manager secret
  (`workshop/athena-federation/timescaledb`) populated post-deploy from the CNPG
  app secret; the Kafka connector reuses the shared `AmazonMSK_workshop-shared`
  SASL/SCRAM secret already associated with the cluster.
- **Spill.** Over-sized result pages spill to
  `s3://workshop-platform-000000000000/athena-federation-spill/` (expired after 7
  days); participant roles can read that prefix so Athena can return spilled
  results.
