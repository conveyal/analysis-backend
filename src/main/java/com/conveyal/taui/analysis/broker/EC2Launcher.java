package com.conveyal.taui.analysis.broker;
import com.conveyal.taui.AnalysisServerConfig;
import com.conveyal.taui.ExecutorServices;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.RequestSpotInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RequestSpotInstancesResponse;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;

/**
 * AWS SDK client to launch EC2 fleets.  Should be initialized once (in Broker)
 */

public class EC2Launcher {
    private static final Logger LOG = LoggerFactory.getLogger(EC2Launcher.class);

    /** Amazon AWS SDK client. */
    private Ec2Client ec2;

    EC2Launcher() {
        // When running on an EC2 instance, default to the AWS region of that instance
        // "If you don't explicitly set a region using the region method, the SDK consults the default region provider
        // chain to try and determine the region to use."
        if (AnalysisServerConfig.offline) {
            ec2 = Ec2Client.builder().region(Region.of(AnalysisServerConfig.awsRegion)).build();
        } else {
            ec2 = Ec2Client.create();
        }
    }

    public void launch (EC2RequestConfiguration requestConfig, int nOnDemand, int nSpot) {

        if (nOnDemand > 0){

            LOG.info("Requesting {} on-demand workers on {}", nOnDemand, requestConfig);

            String clientToken = UUID.randomUUID().toString().replaceAll("-", "");

            RunInstancesRequest onDemandRequest = requestConfig.prepareOnDemandRequest(1, nOnDemand, clientToken);

            ExecutorServices.light.execute(() -> {
                RunInstancesResponse response = ec2.runInstances(onDemandRequest);
                // TODO check and log result of request.
            });
        }

        if (nSpot > 0){

            LOG.info("Requesting {} spot workers on {}", nSpot, requestConfig);

            String clientToken = UUID.randomUUID().toString().replaceAll("-", "");

            RequestSpotInstancesRequest spotRequest = requestConfig.prepareSpotRequest(nSpot, clientToken);

            ExecutorServices.light.execute(() -> {
                RequestSpotInstancesResponse response = ec2.requestSpotInstances(spotRequest);
                // TODO check and log result of request.
            });
        }
    }
}
