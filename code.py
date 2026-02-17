✅ Where the mistake happened

You have two separate workflows (QA workflow + UAT workflow), but inside each workflow the matrix is still:
branch: [QA, UAT]

So:

The QA workflow is running for QA and also for UAT

The UAT workflow is running for QA and also for UAT

That creates confusion and often leads to:

running the wrong environment with the wrong TF_WORKSPACE_NAME

“No changes” because it’s pointing to the wrong workspace/state

deployments not matching what you expect

That’s exactly what Charles was pointing out: each workflow should target only its own env.

✅ What it should be

Option A (recommended): Keep 2 workflows

QA workflow → branch: [QA]

UAT workflow → branch: [UAT]

Option B: Keep 1 workflow

Single workflow → branch: [DEV, QA, UAT, PREPROD] and handle all in one file