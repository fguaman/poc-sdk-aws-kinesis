package cl.guaman.pocsdkawskinesis.infrastructure;

import cl.guaman.pocsdkawskinesis.common.JSON;
import cl.guaman.pocsdkawskinesis.common.KinesisTwoConfig;
import cl.guaman.pocsdkawskinesis.domain.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;

import java.util.UUID;

/**
 * Adapter AWS Kinesis(Producer two)
 *
 * @author fguaman
 */
public class KinesisTwoProducer extends KinesisTwo implements Message.Commands {
    private static final Logger log = LoggerFactory.getLogger(KinesisTwoProducer.class);

    private final JSON<Message> messageJSON;

    public KinesisTwoProducer(KinesisTwoConfig kinesisConfig, String awsKinesisStreamName) {
        super.kinesisConfig = kinesisConfig;
        super.awsKinesisStreamName = awsKinesisStreamName;
        this.messageJSON = new JSON<>(Message.class);
    }

    /**
     * Implementation Kinesis to send message sync two
     *
     * @param message
     */
    @Override
    public void send(Message message) {
        try {
            String json = messageJSON.toJson(message);
            PutRecordResponse response = kinesisConfig.getAwsKinesisClient().putRecord(PutRecordRequest.builder().streamName(super.awsKinesisStreamName).data(SdkBytes.fromByteArray(json.getBytes())).partitionKey(message.getId()).build());
            log.info("sequenceNumber={}", response.sequenceNumber());
        } catch (Exception e) {
            log.error("error in kinesis two sending sync message={}, error={}", message, e.getMessage());
        }
    }

    /**
     * Implementation Kinesis to send message async two
     *
     * @param message
     */
    @Override
    public void sendAsync(Message message) {
        try {
            String json = messageJSON.toJson(message);
            super.kinesisConfig.getAwsKinesisClientAsync().putRecord(PutRecordRequest.builder().streamName(super.awsKinesisStreamName).data(SdkBytes.fromByteArray(json.getBytes())).partitionKey(UUID.randomUUID().toString()).build());
        } catch (Exception e) {
            log.error("error in kinesis two sending async message={}, error={}", message, e.getMessage());
        }
    }
}
