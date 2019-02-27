# Cuttle

An embedded job scheduler/executor for your Scala projects.

# Concepts

Embedded means that cuttle is not an hosted service where you submit jobs to schedule/execute. Instead it is
a Scala library that you embed into your own project to schedule and execute a DAG of jobs. The DAG and the jobs
definitions are all written using the cuttle Scala API. The scheduling mechanism can be customized.

## Jobs

A cuttle project is composed of many [Jobs](https://criteo.github.io/cuttle/api/com/criteo/cuttle/Job.html) to execute.

Each job is defined by a set of metadata (_such as the job identifier, name, etc._) and most importantly by a side effect function. This function handles the actual job execution, and its Scala signature is something like `Context => Future[Completed]` (_which can be read as “execute the job for this input parameter and signal me the completion or failure with the returned Future value”_).

The side effect function is opaque for cuttle, so it can't exactly know what will happen there (_it can be any Scala code_), but it assumes that the function:

- Is asynchronous and non-blocking. It will immediately return a [Future](https://www.scala-lang.org/api/current/scala/concurrent/Future.html) value that will be resolved upon execution success or failure.
- Produces a side effect, so calling it actually will do some job and mutate some state somewhere.
- Is idempotent, so calling it twice for the same input (_context_) won't be a problem.

Being idempotent is important because cuttle is an __at least once__ executor. It will ensure that the job has been successfully executed at least once for a given input. In case of failure or crash it may have to execute it again and so it may happen that the side effect function will succeed more that once. It would be very brittle otherwise.

## Scheduler

Executions of these jobs are planned by a [Scheduler](https://criteo.github.io/cuttle/api/com/criteo/cuttle/Scheduler.html). Actually a job is always configured for a specific [Scheduling](https://criteo.github.io/cuttle/api/com/criteo/cuttle/Scheduling.html) and this is the type `S` you usually see in the Scala API. This scheduling information allows to provide more information to the scheduler about how the jobs must be triggered.

The scheduler gets the list of job (a scheduling specific [Workload](https://criteo.github.io/cuttle/api/com/criteo/cuttle/Workload.html)) as input and starts producing [Executions](https://criteo.github.io/cuttle/api/com/criteo/cuttle/Execution.html). A basic scheduler can for example run a single execution for each job.

But of course more sophisticated schedulers can exist. Cuttle comes with a [TimeSeries](https://criteo.github.io/cuttle/api/com/criteo/cuttle/timeseries/TimeSeries.html) scheduler that executes a whole job workflow (a Directed Acyclic Graph of jobs) across time partitions. For example it can execute the graph hourly or daily. And it can even execute it across different time partitions such as a daily job depending on several executions of an hourly job.

The input context given to the side effect function depends of the scheduling. For example the input for a time series job is [TimeSeriesContext](https://criteo.github.io/cuttle/api/com/criteo/cuttle/timeseries/TimeSeriesContext.html) and contains basically the start and end time for the partition for which the job is being executed.

## Executor

The cuttle [Executor](https://criteo.github.io/cuttle/api/com/criteo/cuttle/Executor.html) handles the job executions triggered by the scheduler. When it has to execute a job for a given [SchedulingContext](https://criteo.github.io/cuttle/api/com/criteo/cuttle/SchedulingContext.html) it creates and execution, and then invoke the job's side effect function for it.

As soon as the execution starts, it is in the __Started__ state. Started executions are displayed in the UI with a special status indicating if they are __Running__ or __Waiting__. This actually indicates if the Scala code being currently executed is waiting for some external resources (_the permit to fork an external process for example_). But as soon as the execution is __Started__ it means that the Scala lambda behind is running!

An execution can also be in the __Stuck__ state. It happens when a given execution keeps failing: Let's say the scheduler wants to execute the job _a_ for the _X_ context. So it asks the executor which eventually executes the job side effect. If the function fails, the returned [Future](https://www.scala-lang.org/api/current/scala/concurrent/Future.html) fails and the scheduler is notified of that failure. Because the scheduler really wants that job to be executed for the _X_ context, it will submit it again. When the executor sees this new execution coming back after a failure it will apply a [RetryStrategy](https://criteo.github.io/cuttle/api/com/criteo/cuttle/RetryStrategy.html). The default strategy is to use an exponential backoff to delay the retry of these failing executions. While being in this state __Stuck__ executions are displayed in a special tab of the UI and it means that it is something you should take care of.

An execution can also be in __Paused__ state. It happens when the job itself has been paused. Note that this is a temporary state; eventually the job has to be unpaused and so the executions will be triggered, otherwise more and more paused executions will stack forever.

Finally executions can be __Finished__ either with a __Success__ or __Failed__ state. You can retrieve these old executions in the log for finished executions.

## Execution Platforms

The way to manage external resources in cuttle is via [ExecutionPlatform](https://criteo.github.io/cuttle/api/com/criteo/cuttle/ExecutionPlatform.html). An execution platforms defines the contract about how to use the resources. They are configured at project bootstrap and usually set limits on how resources will be used (_for example to only allow 10 external processes to be forked at the same time_).

This is necessary because potentially thousands of concurrent executions can happen in cuttle. These executions will fight for shared resources via these execution platforms. Usually a platform will use a priority queue to prioritize access to these shared resources, and the priority is based on the [SchedulingContext](https://criteo.github.io/cuttle/api/com/criteo/cuttle/SchedulingContext.html) of each execution (_so the executions with highest priority get access to the shared resources first_). For example the [TimeSeriesContext](https://criteo.github.io/cuttle/api/com/criteo/cuttle/timeseries/TimeSeriesContext.html) defines its [Ordering](https://www.scala-lang.org/api/current/scala/math/Ordering.html) in such way that oldest partitions take priority.

## Time series scheduling

The built-in [TimeSeriesScheduler](https://criteo.github.io/cuttle/api/com/criteo/cuttle/timeseries/TimeSeriesScheduler.html) executes a workflow of jobs for the time partitions defined in a calendar. Each job defines how it maps to the calendar (_for example Hourly or Daily CEST_), and the scheduler ensures that at least one execution is created and successfully run for each defined (Job, Period) pair.

In case of failure the time series scheduler will submit the execution again and again until the partition is successfully completed (_depending of the retry strategy you have configured the delay between retries will vary_).

It is also possible to [Backfill](https://criteo.github.io/cuttle/api/com/criteo/cuttle/timeseries/Backfill.html) successfully completed past partitions, meaning that we want to recompute them anyway. The whole graph or only a part of the graph can be backfilled depending of what you need. A priority can be given to the backfill so the executions triggered by this backfill can be assigned more or less priority than the day to day workload.

# Documentation

The [API documentation](https://criteo.github.io/cuttle/api/index.html) is the main reference for Scala programmers.

For a project example, you can also follow these hands-on introductions:
- [A basic project using the built-in timeseries scheduler](https://criteo.github.io/cuttle/examples/examples0/HelloTimeSeries.scala.html).
- [A minimal custom scheduling](https://criteo.github.io/cuttle/examples/examples0/HelloCustomScheduling.scala.html)

To run the example application, checkout the repository, launch the [sbt](http://www.scala-sbt.org/) console in the project (_you will need [yarn](https://yarnpkg.com/en/) as well to compile the UI part_), and run the `example HelloWorld` command.

# Usage

The library is cross-built for __Scala 2.11__ and __Scala 2.12__.

The core module to use is `"com.criteo.cuttle" %% "cuttle" % "0.9.1"`.

You also need to fetch one __Scheduler__ implementation:
- __TimeSeries__: `"com.criteo.cuttle" %% "timeseries" % "0.9.1""`.
- __Cron__: `"com.criteo.cuttle" %% "cron" % "0.9.1""`.

# License

This project is licensed under the Apache 2.0 license.

# Copyright

Copyright © Criteo, 2019.
