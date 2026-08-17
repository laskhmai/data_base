You: Yes. Yesterday when I checked the data you gave me, I noticed that some data was missing for GCP. When I originally loaded it, some dates were missing. For some dates, only partial data was available.

So I backfilled that data, ran it again, and checked everything normally. Now we have around 45 days of complete data. The data you gave me yesterday was from around July 8, right? From around July 8/10 onward, we have the complete data after the backfill.

Lead: Okay. Then finally, after doing the remaining backfill, one thing I want you to do is check whether seasonality can be added.

If it cannot be added, let's see what percentage of the results are at least correct, because when I checked, most of them were okay.

You: Yeah. There is just one issue. One particular type is not getting detected. Some new resources were added last month.

What is happening is that their daily amount/cost is gradually increasing every day. So the model is considering that as expected behavior, basically normal behavior.

That's why that particular case isn't getting detected. Apart from that, everything else in the data seems to be correct.

Lead: Okay. Then finally, check the seasonality.

Another thing I want you to do is put your email address in the script and send an email to yourself.

You: Okay.

Lead: And in that email, what I want you to do is make the necessary changes to support GCP.

Because, as far as I know, the current email has Azure-related content. I think I changed most of it, but after that I didn't completely review the entire email because we wanted to validate everything first.

You: Okay, sure. I'll check it.

Lead: And one thing I observed is that the resource names are very different. In some cases, we're just getting numbers.

We have to identify whether those are the actual resource names, or whether we're getting only those values from Cloudability.

You: Yeah, those are coming from Cloudability.

Lead: Right. We should also check once in the GCP assets. If the same thing is there, then that's fine.

Because with just a number, we can't really identify the resource. That's my only concern.

You: Yeah. Actually, when I checked the aggregation table, I checked that too.

Even with the Cloudability account/resource name, I'm seeing some different kinds of names rather than normal readable names.

I checked the asset table, and there is another main table as well. I checked there too.

I saw the same name there. There was no difference at all.

Lead: Okay, okay.

You: Even when I was preparing the data, I checked it against two tables and also checked the cloud data.

I wanted to make sure that I hadn't made some manual mistake, but I'm getting the same result/data everywhere.

So I'm almost 100% sure that's what we're receiving.

Lead: Okay. But once, run the email and see what changes we have to make.

Check whether there are any issues with spacing, formatting, or anything like that.

You: Yeah, sure. I'll check.

Lead: Okay.

And you need to create a story for yourself.

You: Okay, sure.

Lead: Because I forgot to tell you earlier.

Maybe put something like:

“Verifying/validating seasonality of resources.”

Put something along those lines as the story and assign it to yourself.

You: Okay, sure.

You: So right now we have the full June and July data. For August, I'll do the complete backfill from the required date, then check and validate everything once, and then we'll add the email.

The date is the 17th, right?

I'll backfill from around the 15th and make sure we have around two months / 60 days of data.

Lead: Okay.

You: Once the daily run happens, the remaining data should get backfilled.

After that, I'll run this script again and check whether any anomalies are coming, validate them, and load the results.

You scheduled the anomaly process daily already, right?

Lead: No, the anomaly process hasn't been scheduled yet.

Once we validate it, on Wednesday or Thursday, we just need to put it on the schedule.

You: Okay, sure.

Lead: So that shouldn't be much of a problem.

And I was thinking about one more thing.

Check all the outputs. I think you have to save around four outputs/files, if I'm not wrong.

You: Yeah.

Lead: The Excel should be saved, the HTML should be saved, the mail, and the Gold table / Silver table data should also be stored.

You: Right now I think only around three outputs are getting saved.

Lead: Yeah. Check once whether the Silver table data is also being stored.

Actually, for Azure, because there are a lot of resources, I created a pipeline.

We store the data in Blob Storage, and then move it from Blob into the DB.

The Gold table is directly inserted from here.

You: Okay, sure. I'll check.

Lead: For the Silver one, if you're putting it into Blob, one thing you can do is, after the complete validation—or even before that, because it should work anyway—save it once locally.

Check the table structure, create that table in your DB, and insert the data into it.

You: In the DB? Like Gold?

Lead: Yeah, in the DB.

Actually, one minute. Let's keep the order clear.

First thing: check seasonality.

Next: check whether the email is working properly.

You: Okay.

Lead: For the email, check the entire email content.

The HTML will be added to it, and the Excel will also be attached/added to your email.

You: Okay, yeah.

Lead: Just click/open everything and see whether it all looks good.

If something doesn't look good, note down what changes we need to make.

You: Okay, sure.

Lead: After that, check the storage.

In the actual code, the output is directly saved to storage.

Check whether the Silver table output is also getting stored there.

You: Okay, sure.

Lead: For Azure, we already have the Silver table. For GCP, we need to create it.

You: Yeah, we need to create it for GCP. You mentioned that.

Lead: Yeah.

This script directly inserts into the Gold table.

But actually, we should also keep/store the Silver results.

You: Okay.

Lead: So if we need any proof or historical results later, we'll have that.

I think once that is done, that should basically be final.

It should just be a direct upload/process after that. There shouldn't be much confusion. That's the major part.

You: Okay, sure.

You: I have one small doubt.

Actually, the last time I ran it, duplicate records were inserted.

After that I changed the code and ran it again. I'll check it one more time.

Lead: Yeah, yeah. Check that once.

You: That's my only concern.

I had run it multiple times while testing because there was an issue.

During one run, there were something like 99 records, and another time around 18 records were inserted.

So I truncated the table and inserted the data again.

I'll check the code completely once more and make sure everything is correct.

That's my only concern.

Lead: Okay.

You: Yeah, sure.

You: Okay, thanks.