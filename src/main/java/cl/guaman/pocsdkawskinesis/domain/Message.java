package cl.guaman.pocsdkawskinesis.domain;

/**
 * Domain Message to Kinesis
 *
 * @author fguaman
 */
public class Message {

    private String id;
    private String text;

    public Message() {
    }

    public Message(String id, String text) {
        this.id = id;
        this.text = text;
    }

    /**
     * Design pattern commands
     */
    public interface Commands {

        /**
         * Method to send message sync
         *
         * @param message
         */
        void send(Message message);

        /**
         * Method to send message async
         *
         * @param message
         */
        void sendAsync(Message message);
    }

    /**
     * id
     *
     * @return
     */
    public String getId() {
        return id;
    }

    /**
     * text
     *
     * @return
     */
    public String getText() {
        return text;
    }

    @Override
    public String toString() {
        return "class Message(id={id}, text={text})"
                .replace("{id}", this.id).replace("{id}", this.id)
                .replace("{text}", this.text).replace("{text}", this.text);

    }
}
