package com.conveyal.taui.analysis.broker;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.RequestSpotInstancesRequest;
import com.amazonaws.services.ec2.model.RequestSpotInstancesResult;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.conveyal.taui.AnalysisServerConfig;
import com.conveyal.taui.ExecutorServices;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AWS SDK client to launch EC2 fleets.  Should be initialized once (in Broker)
 */

public class EC2Launcher {
    private static final Logger LOG = LoggerFactory.getLogger(EC2Launcher.class);

    /** Amazon AWS SDK client. */
    private AmazonEC2 ec2;

    EC2Launcher(){

        AmazonEC2ClientBuilder ec2Builder = AmazonEC2ClientBuilder.standard();

        // When running on an EC2 instance, default to the AWS region of that instance
        Region region = null;
        if (!AnalysisServerConfig.offline) {
            region = Regions.getCurrentRegion();
        }
        if (region != null) {
            ec2Builder.setRegion(region.getName());
        } else {
            ec2Builder.withRegion(AnalysisServerConfig.awsRegion);
        }
        ec2 = ec2Builder.build();

    }

    public void launch (EC2RequestConfiguration requestConfig, int nOnDemand, int nSpot) {

        if (nOnDemand > 0){

            LOG.info("Requesting {} on-demand workers on {}", nOnDemand, requestConfig);

            String clientToken = UUID.randomUUID().toString().replaceAll("-", "");

            RunInstancesRequest onDemandRequest = requestConfig.prepareOnDemandRequest()
                    .withMinCount(1)
                    .withMaxCount(nOnDemand)
                    .withClientToken(clientToken);

            ExecutorServices.light.execute(() -> {
                RunInstancesResult res = ec2.runInstances(onDemandRequest);
                // TODO check and log result of request.
            });
        }

        if (nSpot > 0){

            LOG.info("Requesting {} spot workers on {}", nSpot, requestConfig);

            String clientToken = UUID.randomUUID().toString().replaceAll("-", "");

            RequestSpotInstancesRequest spotRequest = requestConfig.prepareSpotRequest()
                    .withInstanceCount(nSpot)
                    .withClientToken(clientToken);

            ExecutorServices.light.execute(() -> {
                RequestSpotInstancesResult res = ec2.requestSpotInstances(spotRequest);
                // TODO check and log result of request.
            });
        }
    }
}
