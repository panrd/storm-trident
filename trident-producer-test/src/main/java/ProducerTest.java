import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.me.da.kafka.KafkaProducerBuilder;
import ru.me.da.util.DataGenerator;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Pavel Popov on 26.12.2016.
 */
public class ProducerTest {

    private static Logger logger = LoggerFactory.getLogger(ProducerTest.class);

    private static final String LOG_TOPIC;
    private static final int poolSize;
    private static final int maxBatchSize;
    private static final Random rnd;

    static {
        rnd = new Random();
        LOG_TOPIC = "log-topic";
        poolSize = 10;
        maxBatchSize = 2000;
    }

    public static void main(String[] args) {

        KafkaProducerBuilder kpb = new KafkaProducerBuilder("hbasehost:9092");
        Runtime.getRuntime().addShutdownHook(new Thread(kpb::close));

        ExecutorService executorPool = Executors.newFixedThreadPool(poolSize);

        for (int i = 0; i < poolSize; i++) {

            executorPool.execute(() -> {
                try {
                    for (; ; ) {
                        int capacity = maxBatchSize;//rnd.nextInt(maxBatchSize);
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
                    logger.error(ex.getLocalizedMessage(), ex);
                }
            });

        }
    }
}
