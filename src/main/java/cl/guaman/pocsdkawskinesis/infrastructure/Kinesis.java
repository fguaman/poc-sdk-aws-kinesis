package cl.guaman.pocsdkawskinesis.infrastructure;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse;

/**
 * Adapter AWS Kinesis
 *
 * @author fguaman
 */
public class Kinesis implements CommandLineRunner {
    private static final Logger log = LoggerFactory.getLogger(Kinesis.class);

    protected KinesisClient kinesisClient;
    protected KinesisAsyncClient kinesisAsyncClient;

    protected String awsKinesisStreamName;

    protected int awsKinesisShard;

    public Kinesis() {
        // do nothing
    }

    @Override
    public void run(String... args) throws Exception {
        createIfNotExist();
    }

    /**
     * Create stream if not exist
     */
    private void createIfNotExist() {
        try {
            CreateStreamRequest streamReq = CreateStreamRequest.builder()
                    .streamName(awsKinesisStreamName)
                    .shardCount(awsKinesisShard)
                    .build();
            ListStreamsResponse list = kinesisClient.listStreams();
            boolean exist = list.streamNames().stream()
                    .anyMatch(item -> item.equals(awsKinesisStreamName));
            log.info("awsKinesisStreamName={}, exist={}", awsKinesisStreamName, exist);
            if (!exist)
                this.kinesisClient.createStream(streamReq);
        } catch (Exception e) {
            log.error("error in kinesis creating stream={}, error={}", awsKinesisStreamName, e.getMessage());
        }
    }
}
