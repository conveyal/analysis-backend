# Analysis

This is the server component of the [Conveyal Analysis](http://conveyal.com/analysis) platform.

**Please note:** At this time Conveyal does not provide support for third-party deployments of Analysis. We provide paid subscriptions to a hosted deployment of this system, as well as transportation planning consulting for subscribers.

For now, the project is open source primarily to prevent vendor lock-in for our clients and to ensure transparency in planning and decision making processes. It is likely that over time the system will become easier to deploy by third parties, but we do not plan to provide technical support for such deployments.

## Configuration

Conveyal Analysis can be run locally (e.g. on your laptop) or on the cloud (e.g. on AWS), depending on the configuration set in analysis.properties.  To get started, copy the template configuration (`analysis.properties.tmp`) to `analysis.properties`.

To run locally, use the default values in `analysis.properties`. `offline = true` will create a local instance that does not need cloud-based storage, database, or authentication services.  By default, analysis-backend will use the `scenario-editor` database in a local MongoDB instance, so you'll need to install and start a MongoDB instance.

To run on the cloud, we use Auth0 for authentication and S3 for storage; configure these services as needed, then set the corresponding variables including:

- `auth0-client-id`: your Auth0 client ID
- `auth0-secret`: your Auth0 client secret
- `database-uri`: URI to your Mongo cluster
- `bundle_bucket`: S3 bucket for storing GTFS bundles and built transport networks
- `grid_bucket`: S3 bucket for storing opportunity dataset grids
- `results_bucket`: S3 bucket for storing regional analysis results

You will need S3 credentials set up in your environment or in `~/.aws` for an identity that is allowed to access all the buckets in use, including the seamless census data bucket. If you have multiple profiles, you can use the `AWS_PROFILE` variable in the environment to choose which AWS credentials profile will be used.

## Building and running

Once you have configured `analysis.properties` and started mongo locally, build the application with `mvn package` and start it with
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
`AnalystWorker` classses separately. You will need to configure an `GRID_BUCKET` and `RESULTS_BUCKET`,
and AWS credentials to access them, as these do not yet have offline equivalents.

You can then follow the instructions to get the [frontend](https://github.com/conveyal/analysis-ui) started up.
