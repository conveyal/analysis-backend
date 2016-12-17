# Analyst

This is the server component of Transport Analyst version 2.

## Setup

In production we use Auth0 for authentication and S3 for storage; set the following variables, either in application.conf
in the working directory, or as environment variables.

`AUTH0_CLIENT_ID`: your Auth0 client ID
`AUTH0_CLIENT_SECRET`: your Auth0 client secret
`MONGOLAB_URI`: URI to your Mongo cluster
`BUNDLE_BUCKET`: S3 bucket to store bundles in
`GRID_BUCKET`: S3 bucket to store grids in
`RESULTS_BUCKET`: S3 bucket to store results in
`RESULTS_QUEUE`: SQS queue to use to return results

Alternatively, omit all of that and set `OFFLINE=true` (for the time being you still need a grid bucket, that does not yet work offline).

Currently we depend on the `scenario-editor` branch of R5. Check that out and `mvn install` it.

By default it will use the `scenario-editor` database in your Mongo instance. You can set `DATABASE_NAME` to change that.

Once you have configured your environment or `application.conf`, build the application with `mvn package` and start it with
`java -Xmx2g -jar target/analyst.jar`

You can then start the frontend with `npm start`
