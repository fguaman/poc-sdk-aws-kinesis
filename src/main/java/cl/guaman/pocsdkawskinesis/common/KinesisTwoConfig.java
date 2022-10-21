package cl.guaman.pocsdkawskinesis.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

/**
 * Config Kinesis two
 *
 * @author fguaman
 */
public class KinesisTwoConfig {
    private static final Logger log = LoggerFactory.getLogger(KinesisTwoConfig.class);

    private final StsClient stsClient;
    private final AssumeRoleRequest assumeRoleRequest;
    private Credentials credentials;

    private KinesisClient awsKinesisClient;

    private KinesisAsyncClient awsKinesisAsyncClient;

    private final String awsRegion;

    public KinesisTwoConfig(StsClient stsClient, AssumeRoleRequest assumeRoleRequest, String awsRegion) {
        this.stsClient = stsClient;
        this.assumeRoleRequest = assumeRoleRequest;
        this.awsRegion = awsRegion;
    }

    /**
     * Get sync client. Check if credentials expired
     *
     * @return KinesisClient
     */
    public KinesisClient getAwsKinesisClient() {
        Instant now = Instant.now().plus(30, ChronoUnit.MINUTES);
        if (Objects.isNull(this.credentials) || this.credentials.expiration().isBefore(now)) {
            this.awsKinesisClient = KinesisClient.builder().region(Region.of(awsRegion)).credentialsProvider(StaticCredentialsProvider.create(getSessionCredentials(now))).build();
        }
        return awsKinesisClient;
    }

    /**
     * Get async client. Check if credentials expired
     *
     * @return KinesisAsyncClient
     */
    public KinesisAsyncClient getAwsKinesisClientAsync() {
        Instant now = Instant.now().plus(30, ChronoUnit.MINUTES);
        if (Objects.isNull(this.credentials) || this.credentials.expiration().isBefore(now)) {
            this.awsKinesisAsyncClient = KinesisAsyncClient.builder().region(Region.of(awsRegion)).credentialsProvider(StaticCredentialsProvider.create(getSessionCredentials(now))).build();
        }
        return awsKinesisAsyncClient;
    }

    /**
     * Get session credentials
     *
     * @param now
     * @return AwsSessionCredentials
     */
    private AwsSessionCredentials getSessionCredentials(Instant now) {
        this.credentials = this.stsClient.assumeRole(this.assumeRoleRequest).credentials();
        log.info("accessKeyId={}", credentials.accessKeyId());
        log.info("expiration={}", credentials.expiration());
        log.info("now={}", now);
        return AwsSessionCredentials.create(this.credentials.accessKeyId(), this.credentials.secretAccessKey(), this.credentials.sessionToken());
    }
}
