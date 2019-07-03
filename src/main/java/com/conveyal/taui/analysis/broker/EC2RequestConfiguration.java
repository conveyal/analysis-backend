package com.conveyal.taui.analysis.broker;

import com.conveyal.r5.analyst.WorkerCategory;
import com.conveyal.taui.AnalysisServerConfig;
import com.google.common.io.ByteStreams;
import software.amazon.awssdk.services.ec2.model.IamInstanceProfileSpecification;
import software.amazon.awssdk.services.ec2.model.RequestSpotInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RequestSpotLaunchSpecification;
import software.amazon.awssdk.services.ec2.model.ResourceType;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.ShutdownBehavior;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.TagSpecification;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;
import java.util.Base64;
import java.util.Properties;
import java.util.stream.Collectors;

public class EC2RequestConfiguration {

    private final WorkerCategory category;
    private final String group;
    private final String user;
    private final String projectId;
    private final String regionId;
    private final String userDataScript;
    private final TagSpecification instanceTags;
    private Properties workerConfig; // FIXME this can't be made final because it's only initialized when offline=false

    @Override
    public String toString() {
        return String.format("%s for %s (%s)", category, user, group);
    }

    // The final four parameters should serve only as tags to identify how the worker started up.
    // Note that a worker could migrate to a new project after finishing a job on a different project.
    // The region ID is redundant (implied by the category.bundleId) but useful for categorizing costs in EC2.
    EC2RequestConfiguration(WorkerCategory category, String group, String user, String projectId, String regionId) {
        this.category = category;
        this.group = group;
        this.user = user;
        this.projectId = projectId;
        this.regionId = regionId;

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

        // Set tags so we can identify the instances immediately in the EC2 console. The worker user data makes
        // workers tag themselves, but with a lag.
        instanceTags = TagSpecification.builder().resourceType(ResourceType.INSTANCE).tags(
                Tag.builder().key("Name").value("AnalysisWorker").build(),
                Tag.builder().key("networkId").value(category.graphId).build(),
                Tag.builder().key("workerVersion").value(category.workerVersion).build(),
                Tag.builder().key("group").value(group).build(),
                Tag.builder().key("user").value(user).build(),
                Tag.builder().key("project").value(projectId).build(),
                Tag.builder().key("region").value(regionId).build()
        ).build();

        // Convert the above tags to a command line parameter for the AWS CLI, to allow spot instances to tag themselves.
        // This ensures we use exactly the same set of tags, set in the same way, for both spot and on-demand instances.
        // Note the tagging part of the script will be run even on instances already tagged when created. This is
        // redundant but simpler to maintain and should be harmless.
        String tagString = instanceTags.tags().stream()
                .map(tag -> String.format("Key=%s,Value=%s", tag.key(), tag.value()))
                .collect(Collectors.joining(" "));

        try {
            String workerConfigString = cfg.toString();
            InputStream scriptIs = Broker.class.getClassLoader().getResourceAsStream("worker.sh");
            ByteArrayOutputStream scriptBaos = new ByteArrayOutputStream();
            ByteStreams.copy(scriptIs, scriptBaos);
            scriptIs.close();
            scriptBaos.close();
            String scriptTemplate = scriptBaos.toString();
            String logGroup = AnalysisServerConfig.workerLogGroup;
            // Substitute values so that the worker can tag itself (see the bracketed numbers in worker.sh).
            // Tags are useful in the EC2 console and for billing.
            String script = MessageFormat.format(scriptTemplate, workerDownloadUrl, logGroup, workerConfigString, tagString);
            // Send the config to the new workers as EC2 "user data"
            userDataScript = new String(Base64.getEncoder().encode(script.getBytes()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    RequestSpotInstancesRequest prepareSpotRequest (int nSpot, String clientToken) {
        RequestSpotLaunchSpecification launchSpecification = RequestSpotLaunchSpecification.builder()
                .imageId(AnalysisServerConfig.workerAmiId)
                .instanceType(AnalysisServerConfig.workerInstanceType)
                .subnetId(AnalysisServerConfig.workerSubnetId)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().arn(AnalysisServerConfig.workerIamRole).build())
                .userData(userDataScript)
                .ebsOptimized(true)
                .build();

        return RequestSpotInstancesRequest.builder()
                .launchSpecification(launchSpecification)
                .instanceCount(nSpot)
                .clientToken(clientToken)
                .build();
    }

    RunInstancesRequest prepareOnDemandRequest (int minInstances, int maxInstances, String clientToken) {
        return RunInstancesRequest.builder()
            .imageId(AnalysisServerConfig.workerAmiId)
            .instanceType(AnalysisServerConfig.workerInstanceType)
            .subnetId(AnalysisServerConfig.workerSubnetId)
            .iamInstanceProfile(IamInstanceProfileSpecification.builder().arn(AnalysisServerConfig.workerIamRole).build())
            .instanceInitiatedShutdownBehavior(ShutdownBehavior.TERMINATE)
            .tagSpecifications(instanceTags)
            .userData(userDataScript)
            .ebsOptimized(true)
            .minCount(minInstances)
            .maxCount(maxInstances)
            .clientToken(clientToken)
            .build();
    }

}
