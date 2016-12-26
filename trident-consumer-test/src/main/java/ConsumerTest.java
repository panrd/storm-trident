import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.me.da.kafka.KafkaConsumerBuilder;

/**
 * Created by Pavel Popov on 26.12.2016.
 */
public class ConsumerTest {

    private static Logger logger = LoggerFactory.getLogger(ConsumerTest.class);

    public static void main(String[] args) {

        KafkaConsumerBuilder kcb = new KafkaConsumerBuilder("hbasehost:2185", "alert-topic");

        kcb.open(v -> logger.info(new String(v)));

    }

}
