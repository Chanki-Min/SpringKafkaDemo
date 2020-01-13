package kr.ac.hongik.apl.demo;

import kr.ac.hongik.apl.demo.Listener.KafkaListenerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringKafkaDemoApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(SpringKafkaDemoApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		KafkaListenerService service = new KafkaListenerService();
		service.run();
	}
}
