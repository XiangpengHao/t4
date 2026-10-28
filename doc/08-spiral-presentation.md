This is the draft for the presentation at spiral on Oct 30

Meta guide:
1. at the bottom of each slide, show navigation (content structure), so that readers are not lost.
2. 

=======

Title: t4 serves Vortex faster from disk than memory
Author: Xiangpeng Hao
The title slide may have a benchmark comparing datafusion-vortex reading from t4-disk vs t4-memory vs t4-s3 vs s3. 
We show end to end latency, repeat 10 times, mark min/max latencies.
We run clickbench q20-q30.
(maybe we also want to benchmark on my own desktop, to show this is not a crazy setup.)


Slide 1: show a history of how disk and memory envovle.
Memory price vs SSD price over the last 20 years.
Memory bandwidth vs SSD bandwidth over the last 20 years. Maybe we should also show % of SSD bandwidth over memory bandwidth.

This slide is to set the background and expectation of what storage devices we're talking about.
Maybe we should include S3 bandwidth.


Slide 2: the standard way of doing io.
A diagram with animation. showing data first copy from SSD to kernel buffer, then kernel buffer to user buffer.
Then we show how long is a copy of 4KB.
Then we show how much overhead is a system call.
We say this is the pread, i.e., the tokio way of doing io.

We can say this: if a server's memory bandwidth is 100GB/s, and SSD bandwidth is 50GB/s. then just scanning the data occupies all of the work, this is extremely wasteful.

Slide 3: gental intro to io_uring 

Don't need to be super technical.
Say io_uring is reletively "new".
tokio's file read can't do io_uring. 

Slide 4: 100+ ways to do io_uring.

Show a list of papers discussing how to do io_uring in modern data systems.

Then say: mostly of them are either impractical, or pain wrong.

One representative wrong claim: you need a thread pool to manage io and satruate bandwidth.

In t4, we need the following constraints:
1. it needs to be runtime agnostic. you can't simply say tokio is not supported.
2. you can't invent your own encryption and replace tls.

(no need to write down, but say it, most papers are written by junior PhDs with paper pressures, so take their conclusions with a grain of salt) 

Slide 5: t4's architecture

we need an animated diagram.

First we show a single thread can manage all IOs in the system.
The thread submit tasks to io uring, then poll the compeletion queue.
there's a io_uring submission/compeletion queue thing going on, it should feel faimilar with the diagram in slide 3.

Then we show each read request is send to that thread through a message passing channel.

Then we show the t4 object store is the interface.

internally it has a bbtree instance, mapping each file and their 1MB block to a destination.
Then we show there're two destinations: disk, memory, or remote.

Slide 6: concrete example

We show the life cycle: the disk and memory are both empty intially.
As we run the queries, ranges are consolidated into 1MB blocks, and sending to S3.
Once data is fetched, there's a policy, to decide where to put it, memory, disk, or discard.

Once memory/disk is full. We run over a policy to decide which block to remove/evict to disk.

We also show that a query may request a long range, a part of them is in memory, some in disk, some in s3.
We will automatically split them and read from S3. 

Slide 7: show the title's benchmark again

we first show the full benchmark figure, small texts, mouthful of information, dense.
Then in a overlay, we delete it, and say "Pro tip: don't trust Claude's performance numbers"
"Pro tip: performance optimization is only scientific when you have a cost model"

Slide 8: cost model
then we show some calculations:
1. how much bytes we read to run a query? e.g., 15GB.
2. how long does the query run? e.g., 1s. Then the storage bandwidth is 15GB/s
3. this means if our SSD is 15GB/s, and if we do no stupid things, our performance should match when data is cached in memory. 

then let's look at our setup:
Clickbench partitioned, 100 files.
at query 23, it reads 15GB.
our ssd can do 30GB/s. more than enough to meet the requirements.

How about latency, you may ask? we will get to that later. but there's a high level idea: for queries that are bottlenecked by disk, they are pretty much bandwidth intensive. If we interleave the compute and io well engough, latency diff can be effectively hide. 

Slide 9: show the title benchmark again

...

Slide 10: t4 does much more than that.

we show a benchmark of latency variations, repeat 10 times, show their end to end query latency.
With the following series: aws read from S3. aws read from gcs.

How is it possible that t4 read from s3 is more predictable than naive read from s3? 

Slide 11: automatic merging, throuttling, and racing.

Reading data from network is extremely nosiy.
Show a figure of the same request, from lowest latency to highest latency.

Query performance is limited by the slowest query.
Show a figure showing parallel request, query ends after the slowest request finishes.

Then show a diagram of t4 putting request to a queue, mergeing neighboring requests, racing read if a request has exceeded p90 latency.

Slide 12: not all request are the same

Some requests are critical: metadata read, if a metadata hasn't arrived, no query work can start.
But network doesn't care your critical request.

t4 do.

t4 detects metadata request, very simple: last 2MB of the file, and sending 2 requests simoutanously and takes whichever arrives first.
doing so, the pmax of metadata request reduces by 10x.

Slide 13: multi-SSD support

Cloud instances have multiple local SSDs.
You typically would need to do a raid to use them.
t4 automatically support multiple devices.

(optionally) mention racing read in ssd as well.


Slide 14: t4 is running in vortex and spiral now.

