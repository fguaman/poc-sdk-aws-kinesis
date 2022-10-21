package cl.guaman.pocsdkawskinesis.common;

import cl.guaman.pocsdkawskinesis.infrastructure.JOB;
import cl.guaman.pocsdkawskinesis.infrastructure.KinesisProducer;
import cl.guaman.pocsdkawskinesis.infrastructure.KinesisTwoProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.util.Date;

/**
 * Config Kinesis
 *
 * @author fguaman
 */
@Configuration
public class KinesisConfig {

    @Value("${aws.accessKeyId}")
    private String awsAccessKeyId;

    @Value("${aws.secretAccessKey}")
    private String awsSecretAccessKey;

    @Value("${aws.region}")
    private String awsRegion;

    @Value("${aws.kinesis.streamName}")
    private String awsKinesisStreamName;

    @Value("${aws.kinesis.shard}")
    private int awsKinesisShard;

    @Value("${aws.rolArn}")
    private String awsRolArn;

    @Value("${aws.accessKeyId}")
    private String awsAccessKeyIdTwo;

    @Value("${aws.secretAccessKey}")
    private String awsSecretAccessKeyTwo;

    @Bean
    public AwsCredentialsProvider awsCredentialsProvider() {
        return () -> AwsBasicCredentials.create(awsAccessKeyId, awsSecretAccessKey);
    }

    @Bean
    public AwsCredentialsProvider awsCredentialsProviderTwo() {
        return () -> AwsBasicCredentials.create(awsAccessKeyIdTwo, awsSecretAccessKeyTwo);
    }

    @Bean
    public KinesisClient awsKinesisClient(AwsCredentialsProvider awsCredentialsProvider) {
        return KinesisClient.builder()
                .region(Region.of(awsRegion))
                .credentialsProvider(awsCredentialsProvider)
                .build();
    }

    @Bean
    public KinesisAsyncClient awsKinesisAsyncClient(AwsCredentialsProvider awsCredentialsProvider) {
        return KinesisAsyncClient.builder()
                .region(Region.of(awsRegion))
                .credentialsProvider(awsCredentialsProvider)
                .build();
    }


    @Bean
    public KinesisProducer kinesisProducer(KinesisClient kinesisClient, KinesisAsyncClient kinesisAsyncClient) {
        return new KinesisProducer(kinesisClient, kinesisAsyncClient, awsKinesisStreamName, awsKinesisShard);
    }

    @Bean
    public KinesisTwoProducer kinesisTwoProducer(KinesisTwoConfig kinesisTwoConfigt) {
        return new KinesisTwoProducer(kinesisTwoConfigt, awsKinesisStreamName);
    }

    @Bean
    public JOB job(KinesisProducer kinesisProducer, KinesisTwoProducer kinesisTwoProducer) {
        return new JOB(kinesisProducer, kinesisTwoProducer);
    }


    @Bean
    public StsClient stsClient(AwsCredentialsProvider awsCredentialsProviderTwo) {
        return StsClient.builder()
                .region(Region.of(awsRegion))
                .credentialsProvider(awsCredentialsProviderTwo)
                .build();
    }

    @Bean
    public AssumeRoleRequest assumeRoleRequest() {
        return AssumeRoleRequest.builder()
                .roleArn(awsRolArn)
                .roleSessionName("rol-kinesis-" + new Date().getTime())
                .durationSeconds(3600)
                .build();
    }

    @Bean
    public KinesisTwoConfig kinesisTwoConfig(StsClient stsClient, AssumeRoleRequest assumeRoleRequest) {
        return new KinesisTwoConfig(stsClient, assumeRoleRequest, awsRegion);
    }
}
