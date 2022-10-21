package cl.guaman.pocsdkawskinesis;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Run app
 *
 * @author fguaman
 */
@EnableScheduling
@SpringBootApplication
public class PocSdkAwsKinesisApplication {

    public static void main(String[] args) {
        SpringApplication.run(PocSdkAwsKinesisApplication.class, args);
    }
}
