Hi Tracy,

Just wanted to share a quick update on this.

Initially, we updated the module version and the pipeline ran successfully. After that, we introduced the scaling change (capacity 1 → 2), but Terraform is not detecting any changes — it still shows “No changes”.

I verified the setup end-to-end:

* Pipeline variables are updated correctly
* Token replacement step is working
* All `.tf` files are present and the working directory is correct

Since it was still not clear, I reached out to Nate. Based on his suggestions, we enabled detailed logging and validated the working directory and files as well. Everything looks correct, but even after that, the plan is still not detecting the change.

At this point, we’re unable to identify why the updated value is not being picked up by Terraform.

Could you please suggest what we should try next or if there’s anything else we should validate?
