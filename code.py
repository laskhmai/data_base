Hi Nate,

Just to explain everything from the beginning so it’s clear:

We started with the Redis changes in a feature branch (feature/phmddsme-redis-main).

A PR was opened to merge into main.

The QA and UAT Terraform plan workflows were triggered from that PR.

Initially, both QA and UAT workflows had:

branch: [QA, UAT]

so both environments were running from the same matrix configuration.

We suspected that might be causing workspace confusion, so the workflows were separated so QA and UAT run independently.

After separating them, the pipelines still show:
“No changes. Your infrastructure matches the configuration.”

DEV previously worked correctly.

At this point, QA and UAT are running, but Terraform is not detecting any changes even though Redis changes were added.

Could you help review whether:

The workspace mapping per environment is correct, or

The workflows are referencing the correct Terraform Cloud workspaces/state?