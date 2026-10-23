# MSK metadata-only execution

Production Git SHA: `3d772bdea261de4275f5888599d661b7f888fb73`.
User mission permitted short existing EC2 and temporary minimum IAM with cleanup.
Fixture count **0**. No install, new cloud resource, topic change, publish, consume,
Databricks Compute, offset/checkpoint change occurred.

`metadata-and-cleanup.json` contains the exact temporary policy, timestamps,
read-only client shell, selected non-secret broker metadata/config output, original
IAM policy snapshot and cleanup comparison. No credential file contents were read
or copied. Existing EC2 role/profile dependencies were checked before mutation.

Both outputs exist: partitions3/RF3/min ISR2/delete/retention604800000ms/bytes=-1.
All partitions had leaders and3 ISR entries. This proves metadata/authentication
from the EC2 IAM client, not Job2 end-to-end data delivery or output consumer access.

Start request to stopped confirmation:59.12 seconds. EC2 final stopped; temporary
policy NoSuchEntity; original policies preserved; cleanup_errors empty. Planning
budget $0.05/15min is not an AWS billing hard cap. Simple prior-price VM+IPv4
estimate ~$0.0003 excludes minimum billing, credits, taxes and other traffic/costs.
Existing MSK/NAT/EIP/EBS baseline cost continues; no deletion was authorized.

For a portfolio screenshot, display only metadata_lines and final_ec2_state,
temporary_policy_final_state/original_policies_preserved/cleanup_errors. Never show
client.properties, environment credential values or raw credential diagnostics.
