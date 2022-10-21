package cl.guaman.pocsdkawskinesis.infrastructure;

import cl.guaman.pocsdkawskinesis.domain.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.UUID;

/**
 * Adapter JOB
 *
 * @author fguaman
 */
public class JOB {
    private static final Logger log = LoggerFactory.getLogger(JOB.class);
    private final Message.Commands command;
    private final Message.Commands commandTwo;

    public JOB(KinesisProducer kinesisProducer, KinesisTwoProducer kinesisTwoProducer) {
        this.command = kinesisProducer;
        this.commandTwo = kinesisTwoProducer;
    }

    /**
     * Send messages to Kinesis sync
     */
    //@Scheduled(cron = "0/10 * * * * ?")
    void initOne() {
        this.command.send(new Message(UUID.randomUUID().toString(), "Hello from JOB one!!!"));
    }

    /**
     * Send messages to Kinesis sync and async every 10sec
     */
    @Scheduled(cron = "0/10 * * * * ?")
    void initTwo() {
        this.commandTwo.send(new Message(UUID.randomUUID().toString(), "Hello from JOB two!!!"));
    }
}
