package cl.guaman.pocsdkawskinesis.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility JSON
 *
 * @param <T>
 * @author fguaman
 */
public class JSON<T> {

    private final Class<T> typeParameterClass;
    private final Logger log = LoggerFactory.getLogger(JSON.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    public JSON(Class<T> typeParameterClass) {
        this.typeParameterClass = typeParameterClass;
    }

    public String toJson(T t) {
        try {
            return objectMapper.writeValueAsString(t);
        } catch (Exception e) {
            log.error("message={}", e.getMessage());
            return null;
        }
    }


    public T toDomain(String json) {
        try {
            return objectMapper.readValue(json, typeParameterClass);
        } catch (Exception e) {
            log.error("message={}", e.getMessage());
            return null;
        }
    }
}
