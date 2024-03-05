package dharmycode.kafkaproducerwikimedia.producer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import dharmycode.kafkaproducerwikimedia.WikimediaChangeHandler;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

@Service
public class WikimediaChangesProducer {

    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(WikimediaChangesProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public WikimediaChangesProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    // Create a method
    public void sendMessage() throws InterruptedException {

        String topic = "wikimedia_recentchange";

    //    to read real time stream data from wikimedia, we use event source

        EventHandler eventHandler = new WikimediaChangeHandler(kafkaTemplate, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();
        eventSource.start();

        TimeUnit.MINUTES.sleep(10);
    }

}
