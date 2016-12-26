import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.me.da.kafka.KafkaProducerBuilder;
import ru.me.da.util.DataGenerator;

/**
 * Created by Pavel Popov on 26.12.2016.
 */
public class ProducerTest {

    private static Logger logger = LoggerFactory.getLogger(ProducerTest.class);

    private static String LOG_TOPIC;

    static {
        LOG_TOPIC = "log-topic";
    }

    public static void main(String[] args) {

        KafkaProducerBuilder kpb = new KafkaProducerBuilder("hbasehost:9092");
        Runtime.getRuntime().addShutdownHook(new Thread(kpb::close));
        try {
            for (; ; ) {
                int capacity = 300;//rnd.nextInt(100);
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
