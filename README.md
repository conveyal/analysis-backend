# Conveyal Analysis

This is the server component of [Conveyal Analysis](http://conveyal.com/analysis), which allows users to create public transport scenarios and evaluate them in terms of cumulative opportunities accessibility indicators. 

**Please note** that Conveyal does not provide technical support for third-party deployments of Analysis. We provide paid subscriptions to a cloud-based deployment of this system which performs these complex calculations hundreds of times faster using a compute cluster. This project is open source primarily to ensure transparency and reproducibility in public planning and decision making processes, and in hopes that it may help researchers, students, and potential collaborators to understand and build upon our methodology.

## Configuration

It is possible to run a Conveyal Analysis UI and backend locally (e.g. on your laptop), which should produce results identical to those from our hosted platform. However, the computations for more complex analyses may take quite a long time. Extension points in the source code allow the system to be tailored to cloud computing environments to enable faster parallel computation.

### Running Locally

To get started, copy the template configuration (`analysis.properties.tmp`) to `analysis.properties`.  

To run locally, use the default values in the template configuration file. `offline=true` will create a local instance 
that avoids cloud-based storage, database, or authentication services. By default, analysis-backend will use the `analysis` database in a local MongoDB instance, so you'll also need to install and start a MongoDB instance.

Database configuration variables include:

- `database-uri`: URI to your Mongo cluster
- `database-name`: name of the database to use in your Mongo cluster

## Building and running

Once you have configured `analysis.properties` and started mongo locally, build the application with `mvn package` and 
start it with `java -Xmx2g -jar target/shaded/vX.Y.Z.jar`

Next, follow the instructions to start the [analysis-ui frontend](https://github.com/conveyal/analysis-ui). Once that 
is running, you should be able to log in without authentication (using the frontend URL, e.g. http://localhost:3000). 

## Creating a development environment

In order to do development on the frontend or backend, you'll need to set up a local development environment. We use [IntelliJ IDEA](https://www.jetbrains.com/idea/). The free/community edition is sufficient for working on Conveyal Analysis. Add analysis-backend to IntelliJ as a new project from existing sources. You can then create a run configuration for `com.conveyal.analysis.BackendMain`, which is the main class. You will need to configure the JVM options and properties file mentioned above.

## Structured Commit Messages

We use structured commit messages to allow automated tools to determine release version numbers and generate changelogs.

The first line of these messages is in the following format: `<type>(<scope>): <summary>` 

The `(<scope>)` is optional and is often a class name. The `<summary>` should be in the present tense. The type should be one of the following:

- feat: A new feature from the user point of view, not a new feature for the build.
- fix: A bug fix from the user point of view, not a fix to the build.
- docs: Changes to the user documentation, or to code comments.
- style: Formatting, semicolons, brackets, indentation, line breaks. No change to program logic.
- refactor: Changes to code which do not change behavior, e.g. renaming a variable.
- test: Adding tests, refactoring tests. No changes to user code.
- build: Updating build process, scripts, etc. No changes to user code.
- devops: Changes to code that only affect deployment, logging, etc. No changes to user code.
- chore: Any other changes causing no changes to user code.

The body of the commit message (if any) should begin after one blank line. If the commit meets the definition of a major version change according to semantic versioning (e.g. a change in API visible to an external module), the commit message body should begin with `BREAKING CHANGE: <description>`.

Presence of a `fix` commit in a release should increment the number in the third (PATCH) position.
Presence of a `feat` commit in a release should increment the number in the second (MINOR) position.
Presence of a `BREAKING CHANGE` commit in a release should increment the number in the first (MAJOR) position.

This is based on https://www.conventionalcommits.org.
