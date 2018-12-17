package com.conveyal.taui.analysis.broker;

import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.ec2.model.LaunchSpecification;
import com.amazonaws.services.ec2.model.RequestSpotInstancesRequest;
import com.amazonaws.services.ec2.model.ResourceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.ShutdownBehavior;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TagSpecification;
import com.conveyal.r5.analyst.WorkerCategory;
import com.conveyal.taui.AnalysisServerConfig;
import com.google.common.io.ByteStreams;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;
import java.util.Base64;
import java.util.Properties;

public class EC2RequestConfiguration {
    private WorkerCategory category;
    private String group;
    private String user;
    private String userDataScript;
    private Properties workerConfig;

    @Override
    public String toString(){
        return (String.format("%s for %s (%s)", workerConfig, user, group));
    }

    EC2RequestConfiguration(WorkerCategory category, String group, String user) {
        this.category = category;
        this.group = group;
        this.user = user;

        if (!AnalysisServerConfig.offline) {
            workerConfig = new Properties();
            // TODO rename to worker-log-group in worker code
            workerConfig.setProperty("log-group", AnalysisServerConfig.workerLogGroup);
            workerConfig.setProperty("broker-address", AnalysisServerConfig.serverAddress);
            // TODO rename all config fields in config class, worker and backend to be consistent.
            // TODO Maybe just send the entire analyst config to the worker!
            workerConfig.setProperty("broker-port", Integer.toString(AnalysisServerConfig.serverPort));
            workerConfig.setProperty("worker-port", Integer.toString(AnalysisServerConfig.workerPort));
            workerConfig.setProperty("graphs-bucket", AnalysisServerConfig.bundleBucket);
            workerConfig.setProperty("pointsets-bucket", AnalysisServerConfig.gridBucket);
            workerConfig.setProperty("aws-region", AnalysisServerConfig.awsRegion);
            // Tell the workers to shut themselves down automatically when no longer busy.
            workerConfig.setProperty("auto-shutdown", "true");
            workerConfig.setProperty("initial-graph-id", category.graphId);
        }

        // Substitute details into the startup script
        // We used to just pass the config to custom AMI, but by constructing a startup script that initializes a stock
        // Amazon Linux AMI, we don't have to worry about maintaining and keeping our AMI up to date. Amazon Linux applies
        // important security updates on startup automatically.
        // Tell the worker where to get its R5 JAR. This is a Conveyal S3 bucket with HTTP access turned on.
        String workerDownloadUrl = String.format("https://r5-builds.s3.amazonaws.com/%s.jar", category.workerVersion);
        ByteArrayOutputStream cfg = new ByteArrayOutputStream();

        try {
            workerConfig.store(cfg, "Worker config");
            cfg.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            String workerConfigString = cfg.toString();
            InputStream scriptIs = Broker.class.getClassLoader().getResourceAsStream("worker.sh");
            ByteArrayOutputStream scriptBaos = new ByteArrayOutputStream();
            ByteStreams.copy(scriptIs, scriptBaos);
            scriptIs.close();
            scriptBaos.close();
            String scriptTemplate = scriptBaos.toString();
            String logGroup = AnalysisServerConfig.workerLogGroup;
            // Substitute values so that the worker can tag itself (see the bracketed numbers in R5 worker.sh).
            // Tags are useful in the EC2 console and for billing.
            String script = MessageFormat.format(scriptTemplate, workerDownloadUrl, logGroup, workerConfigString,
                    group, user, category.graphId, category.workerVersion);
            // Send the config to the new workers as EC2 "user data"
            userDataScript = new String(Base64.getEncoder().encode(script.getBytes()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    RequestSpotInstancesRequest prepareSpotRequest(){
        LaunchSpecification launchSpecification = new LaunchSpecification()
                .withImageId(AnalysisServerConfig.workerAmiId)
                .withInstanceType(AnalysisServerConfig.workerInstanceType)
                .withSubnetId(AnalysisServerConfig.workerSubnetId)
                .withIamInstanceProfile(new IamInstanceProfileSpecification().withArn(AnalysisServerConfig.workerIamRole))
                .withUserData(userDataScript)
                .withEbsOptimized(true);

        return new RequestSpotInstancesRequest()
                .withLaunchSpecification(launchSpecification);
    }

    RunInstancesRequest prepareOnDemandRequest(){
        // Set tags so we can identify the instances immediately in the EC2 console. The worker user data makes
        // workers tag themselves, but with a lag.
        TagSpecification instanceTags = new TagSpecification().withResourceType(ResourceType.Instance)
                .withTags(
                        new Tag("Name", "Analysis Worker"),
                        new Tag("Project", "Analysis"),
                        new Tag("networkId", category.graphId),
                        new Tag("workerVersion", category.workerVersion),
                        new Tag("group", group),
                        new Tag("user", user)
                );

        return new RunInstancesRequest()
            .withImageId(AnalysisServerConfig.workerAmiId)
            .withInstanceType(AnalysisServerConfig.workerInstanceType)
            .withSubnetId(AnalysisServerConfig.workerSubnetId)
            .withIamInstanceProfile(new IamInstanceProfileSpecification().withArn(AnalysisServerConfig.workerIamRole))
            .withInstanceInitiatedShutdownBehavior(ShutdownBehavior.Terminate)
            .withTagSpecifications(instanceTags)
            .withUserData(userDataScript)
            .withEbsOptimized(true);
    }
}
