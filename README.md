# Conveyal Analysis

This is the server component of [Conveyal Analysis](http://conveyal.com/analysis), which allows users to create public 
transport scenarios and evaluate them in terms of accessibility.

**Please note:** At this time Conveyal does not provide support for third-party deployments of Analysis. We provide paid 
subscriptions to a hosted deployment of this system, as well as transportation planning consulting for subscribers.

For now, the project is open source primarily to prevent vendor lock-in for our clients and to ensure transparency in 
planning and decision making processes. It is likely that over time the system will become easier to deploy by third 
parties, but we do not plan to provide technical support for such deployments.

## Configuration

Conveyal Analysis can be run locally (e.g. on your laptop) or on Amazon Web Services EC2 instances, depending on the 
configuration set in analysis.properties.  With extensions requiring software development skills, it could be modified 
to run in other cloud-computing environments.
                                           
To get started, copy the template configuration (`analysis.properties.tmp`) to `analysis.properties`.  

To run locally, use the default values in the template configuration file. `offline = true` will create a local instance 
that avoids cloud-based storage, database, or authentication services.  To run regional accessibility analyses (as 
opposed to single-point isochrone analyses), you will need to set up an AWS S3 bucket and set the value of `results_bucket`.
By default, analysis-backend will use the `analysis` database in a local MongoDB instance, so you'll also need to 
install and start a MongoDB instance.

To run on the cloud, we use Auth0 for authentication and S3 for storage; configure these services as needed, then set 
the corresponding variables including:

- `auth0-client-id`: your Auth0 client ID
- `auth0-secret`: your Auth0 client secret
- `database-uri`: URI to your Mongo cluster
- `database-name`: name of project database in your Mongo cluster
- `frontend-url`: URL of the analysis-ui frontend (see below)
- `bundle_bucket`: S3 bucket for storing GTFS bundles and built transport networks
- `grid_bucket`: S3 bucket for storing opportunity dataset grids
- `results_bucket`: S3 bucket for storing regional analysis results

You will need S3 credentials set up in your environment or in `~/.aws` for an identity that is allowed to access all the 
buckets above. If you have multiple profiles, you can use the `AWS_PROFILE` variable in the environment to choose which 
AWS credentials profile will be used.

## Building and running

Once you have configured `analysis.properties` and started mongo locally, build the application with `mvn package` and 
start it with `java -Xmx2g -jar target/analysis.jar`

You can then follow the instructions to get the [analysis-ui frontend](https://github.com/conveyal/analysis-ui) started 
with `yarn start`. If you want to avoid starting the frontend yourself, you the default value of `frontend-url` will use 
a prebuilt copy of the frontend provided by Conveyal, but there are no guarantees this version of the frontend will be 
compatible with the version of analysis-backend you are using. 

## Creating a development environment

In order to do development on the frontend, backend, or on [R5](https://github.com/conveyal/r5), which we use for
performing the analyses, you'll want a local development environment. We use [IntelliJ IDEA](https://www.jetbrains.com/idea/)
(free/community version is fine). First, clone the project with `git`, and add it as a project to IntelliJ. Do the same with
R5; add it as another module _in the same IntelliJ project_. Using the Maven panel in INtelliJ, you can then substitute 
your cloned copy of R5 for the version downloaded as a dependency via the analysis-backend `pom.xml`.  

You can then create a run configuration for `com.conveyal.taui.AnalysisServer`, which is the main class. You will need to
configure the options mentioned above.
