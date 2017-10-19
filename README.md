# Analyst

This is the server component of Conveyal's analysis platform.

**Please note:** At this time Conveyal does not provide support for third-party deployments of Analysis. We provide paid subscriptions to a hosted deployment of this system, as well as transportation planning consulting for subscribers.

For now, the project is open source primarily to prevent vendor lock-in for our clients and to ensure transparency in planning and decision making processes. It is likely that over time the system will become easier to deploy by third parties, but we do not plan to provide technical support for such deployments.

## Setup

In production we use Auth0 for authentication and S3 for storage; set the following variables, either in application.conf
in the working directory, or as environment variables.

- `AUTH0_CLIENT_ID`: your Auth0 client ID
- `AUTH0_CLIENT_SECRET`: your Auth0 client secret
- `MONGOLAB_URI`: URI to your Mongo cluster
- `BUNDLE_BUCKET`: S3 bucket to store bundles in
- `GRID_BUCKET`: S3 bucket to store grids in
- `RESULTS_BUCKET`: S3 bucket to store results in
- `RESULTS_QUEUE`: SQS queue to use to return results

Alternatively, omit all of that and set `OFFLINE=true`. For now, in offline mode you still need an S3 grid bucket and SQS results queue, those parts do not yet work offline. This means that regional jobs will write and read from the specified queue - be careful not to configure any development or staging instance with the same queue names used in production! For that matter, never configure any two instances to use the same queue.

You will need to have S3 credentials set up in your environment or in `~/.aws` for an identity that is allowed to access all the buckets in use, including the seamless census data bucket. If you have multiple profiles, you can use the `AWS_PROFILE` variable in the environment or in application.conf to choose which AWS credentials profile will be used.

By default it will use the `scenario-editor` database in your Mongo instance. You can set `DATABASE_NAME` to change that.

Once you have configured your environment or `application.conf`, build the application with `mvn package` and start it with
`java -Xmx2g -jar target/analyst.jar`

You can then start the [frontend](https://github.com/conveyal/analysis-ui) with `yarn start`

## Creating a development environment

In order to do development on the frontend, backend, or on [R5](https://github.com/conveyal/r5), which we use for
performing the analyses, you'll want a local development environment. We use [IntelliJ IDEA](https://www.jetbrains.com/idea/)
(free/community version is fine). First, clone the project with `git`, and add it as a project to IntelliJ. Do the same with
R5; add it as another module _in the same IntelliJ project_. Then, enter the project settings of `analysis-backend` and remove 
the existing dependency on R5 (which will pull down a built JAR from Maven Central) and replace it with a module dependency 
on your local R5. This way, any changes you make to R5 will also be reflected when you run it.

You can then create a run configuration for `com.conveyal.taui.AnalysisServer`, which is the main class. You will need to
configure the options mentioned above; I recommend using environment variables in the run configuration rather than messing
with config files for local development. If you set `OFFLINE=true`, you won't need to run the R5 `BrokerMain` and
`AnalystWorker` classses separately. You will need to configure an `GRID_BUCKET`, `RESULTS_QUEUE` and `RESULTS_BUCKET`,
and AWS credentials to access them, as these do not yet have offline equivalents.

You can then follow the instructions to get the [frontend](https://github.com/conveyal/analysis-ui) started up.

You'll also need a MongoDB instance running locally.
