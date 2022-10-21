package cl.guaman.pocsdkawskinesis.infrastructure;

import cl.guaman.pocsdkawskinesis.domain.Message;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

/**
 * Adapter WEB
 *
 * @author fguaman
 */
@RestController
public class WEB {

    private final Message.Commands command;
    private final Message.Commands commandTwo;


    public WEB(KinesisProducer kinesisProducer, KinesisTwoProducer kinesisTwoProducer) {
        this.command = kinesisProducer;
        this.commandTwo = kinesisTwoProducer;
    }

    /**
     * Generate new message to AWS Kinesis one
     *
     * @return
     */
    @GetMapping(path = "/producer-one/message", produces = "text/plain")
    public ResponseEntity<String> sendMessage() {
        command.send(new Message(UUID.randomUUID().toString(), "Hello from WEB one!!!"));
        return ResponseEntity.ok("generated message in producer one");
    }

    /**
     * Generate new message to AWS Kinesis two
     *
     * @return
     */
    @GetMapping(path = "/producer-two/message", produces = "text/plain")
    public ResponseEntity<String> sendMessageTwo() {
        commandTwo.send(new Message(UUID.randomUUID().toString(), "Hello from WEB two!!!"));
        return ResponseEntity.ok("generated message in producer two");
    }
}