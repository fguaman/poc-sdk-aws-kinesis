package cl.guaman.pocsdkawskinesis.infrastructure;

import cl.guaman.pocsdkawskinesis.common.JSON;
import cl.guaman.pocsdkawskinesis.domain.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;

import java.util.UUID;

/**
 * Adapter AWS Kinesis(Producer)
 *
 * @author fguaman
 */
public class KinesisProducer extends Kinesis implements Message.Commands {
    private static final Logger log = LoggerFactory.getLogger(KinesisProducer.class);
    private final JSON<Message> messageJSON;

    public KinesisProducer(KinesisClient kinesisClient, KinesisAsyncClient kinesisAsyncClient, String awsKinesisStreamName, int awsKinesisShard) {
        super.kinesisClient = kinesisClient;
        super.kinesisAsyncClient = kinesisAsyncClient;
        super.awsKinesisStreamName = awsKinesisStreamName;
        super.awsKinesisShard = awsKinesisShard;
        this.messageJSON = new JSON<>(Message.class);
    }

    /**
     * Implementation Kinesis to send message sync
     *
     * @param message
     */
    @Override
    public void send(Message message) {
        try {
            String json = messageJSON.toJson(message);
            PutRecordResponse response = super.kinesisClient.putRecord(PutRecordRequest.builder().streamName(super.awsKinesisStreamName).data(SdkBytes.fromByteArray(json.getBytes())).partitionKey(message.getId()).build());
            log.info("sequenceNumber={}", response.sequenceNumber());
        } catch (Exception e) {
            log.error("error in kinesis sending sync message={}, error={}", message, e.getMessage());
        }
    }
    /**
     * Implementation Kinesis to send message async
     *
     * @param message
     */
    @Override
    public void sendAsync(Message message) {
        try {
            String json = messageJSON.toJson(message);
            super.kinesisAsyncClient.putRecord(PutRecordRequest.builder().streamName(super.awsKinesisStreamName).data(SdkBytes.fromByteArray(json.getBytes())).partitionKey(UUID.randomUUID().toString()).build());
        } catch (Exception e) {
            log.error("error in kinesis sending async message={}, error={}", message, e.getMessage());
        }
    }
}
