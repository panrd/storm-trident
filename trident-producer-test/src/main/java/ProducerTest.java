import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.me.da.kafka.KafkaProducerBuilder;
import ru.me.da.util.DataGenerator;

import java.util.Random;

/**
 * Created by Pavel Popov on 26.12.2016.
 */
public class ProducerTest {

    private static Logger logger = LoggerFactory.getLogger(ProducerTest.class);

    private static String LOG_TOPIC;
    private static Random rnd;

    static {
        LOG_TOPIC = "log-topic";
        rnd = new Random();
    }

    public static void main(String[] args) {

        KafkaProducerBuilder kpb = new KafkaProducerBuilder("hbasehost:9092");
        Runtime.getRuntime().addShutdownHook(new Thread(kpb::close));
        try {
            for (; ; ) {
                int capacity = 10000;//rnd.nextInt(1000);
                if (capacity == 0) {
                    capacity = 1;
                }
                DataGenerator dg = new DataGenerator();
                String msg2Topic = dg.getJson(capacity);
                if (msg2Topic != null) {
                    kpb.send(LOG_TOPIC, msg2Topic);
                    Thread.sleep(1000);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
