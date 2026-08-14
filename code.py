It didn’t come in the morning for some reason, but maybe it will come tomorrow or the day after.

“Okay, okay.”

So, I mean, you said you were a bit free. We need to validate these GCP anomalies. I actually don’t have time to validate the report.

“Okay, okay, yeah.”

So basically, this is a report.

“Okay.”

One minute. We should have it in the DB also. I’ll give you the script.

“Okay.”

Actually, it will come into the table. There will also be another Silver table where you have all the resources. For Azure, I already created it.

It is called Silver Anomaly Results.

Basically, that one is for Azure. Similarly, I will create one for GCP in the Silver table.

“Okay.”

So, in the Silver table, you will have results for all the resources.

“Okay.”

But when it comes to the Gold table, if we have a delta of more than $50, then it will come into this table.

“Okay.”

But what I want you to do is: we have the aggregation table, right? From that, identify whether whatever anomalies we are getting are actually anomalies or not.

“Okay, okay. Understood.”

Yeah. Some might be missed and some might not be missed. Check whether the ones that are coming are coming properly. Do it for multiple days, maybe take around five days.

“Okay.”

And once this is done, I think I’m going to schedule the Gold table today. I’ll give you the script. You can directly insert the data into the table and directly verify it.

“Okay.”

It will also generate an email. I’ll add your ID to that email. It should be somewhere near the end of the script.

“Okay.”

And one more thing: if we create/load the Silver table, there will be a lot of data. If you need the Silver table, tell me. I’ll create a separate process for it.

Because if you have too many resources, loading them directly from Python takes a lot of time.

“Okay, okay.”

For Azure, what I do is put it into storage and then move it to the DB.

“Okay.”

For GCP, I think we have less than 200K resources. But it will still take a lot of time to insert all of them right now.

“Okay, understood.”

Let me make sure I understand. In this Gold Anomaly Results table, we get the anomalies—resources with unusual activity. So my work is basically to check the cost in the aggregation table, compare the cost here, and identify whether what was detected is actually an anomaly. Is that correct?

And another thing: you can reduce the delta if you want. There is a delta configured here; at the top it is set to $50.

“Yes.”

“Okay, then I understood correctly.”

So basically, the aggregation table has the daily cost, right?

“Yes, daily cost.”

So we run this anomaly model using that table.

“Yeah.”

When this anomaly model runs, it runs for every resource, right?

The result for every resource should actually be present in this [Silver] table.

“Yes.”

But I haven't created that for GCP yet.

“Okay, okay.”

But for the Gold table, if the delta is more than $50, we insert that data. So that insertion process is already there.

“Okay.”

So when you run this Python script, the data will be inserted directly into the DB.

“Okay.”

What I want you to do is run that script for different days.

“Okay, okay.”

I think the latest date I saw was the 26th. It hasn't been scheduled, right?

If you want, you can run it from your side and backfill the remaining days.

“Okay.”

If it runs tomorrow, I know it will take the latest date, right?

“Yes.”

So we won't get the remaining dates automatically. If you want, backfill them, or check it for the previous days.

“Okay, okay. Understood.”

And another thing: in anomaly detection, if you have only 14 days of cost data, the model may not work for that resource because you need at least 30 days of data to know whether something is an anomaly condition.

“Okay.”

So that doesn't make it an anomaly [for the model]. That part won't work. I mean, I’m saying that a specific resource may not come as an anomaly.

“Oh, okay, yeah.”

Because it is a newly created resource. Since it has less historical data, it wasn't included/detected.

So basically, to summarize your work: from the aggregation table, look at the costs. Or you can do it through reverse engineering—start from the anomaly result and check whether that specific resource is really an anomaly.

“Okay, okay. Yeah, I understand the overall concept.”

After validating those, then come from the aggregation side.

“Okay.”

From aggregation, identify what we have not detected as anomalies but which are actually anomalies.

“Okay, okay.”

You can write a simple query for that. For example, look at the last 30 days and identify where today's cost suddenly increased significantly.

Just put a delta of maybe more than $50 or $25 compared with yesterday's cost.

A simple/default query will give you that.

“Yeah, yeah.”

Then you will automatically get a list of resources that could potentially be anomalies.

“Okay.”

And from there, you can check why a resource was not detected.

“Okay, understood.”

I’ll send you the script.

One thing: it will send the complete email. Just comment out the Send Mail function.

“Okay.”

I think that should be fine. Everything else can remain.

Basically, whatever is going into the Gold table will be inserted using the insert function.

“Okay.”

Your minimum overspend is $50. Only when the anomaly/overspend is more than $50 will it be detected/inserted there.

“Okay, $50.”

For example, suppose today's actual cost is much higher than the expected cost. If the difference is more than $50, that's the overspend.

“Yes.”

Those records will be in this table. Anything less than $50 will not be there.

“Okay, okay, yeah.”

You can give whatever billing date you want.

“Okay.”

Once it is scheduled, basically it will work/process the last three days.

“Yeah.”