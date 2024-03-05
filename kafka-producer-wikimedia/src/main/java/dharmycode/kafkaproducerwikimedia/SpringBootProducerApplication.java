package dharmycode.kafkaproducerwikimedia;

import dharmycode.kafkaproducerwikimedia.producer.WikimediaChangesProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringBootProducerApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(SpringBootProducerApplication.class, args);

	}

	private WikimediaChangesProducer wikimediaChangesProducer;

	@Autowired
	public SpringBootProducerApplication(WikimediaChangesProducer wikimediaPublishMessageProducer) {
		this.wikimediaChangesProducer = wikimediaPublishMessageProducer;
	}

	@Override
	public void run(String... args) throws Exception {
		wikimediaChangesProducer.sendMessage();
	}


}