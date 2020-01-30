package kr.ac.hongik.apl.demo.Configuration;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.HashMap;
import java.util.Map;

@Configuration
@PropertySource("classpath:kafka.properties")
@EnableKafka
public class KafkaProducerConfiguration {
	@Autowired
	private Environment env;

	public Map<String, Object> producerConfigs() {
		Map<String, Object> props = new HashMap<>();

		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, env.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
		props.put(ProducerConfig.RETRIES_CONFIG, env.getProperty(ProducerConfig.RETRIES_CONFIG));
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, env.getProperty(ProducerConfig.BATCH_SIZE_CONFIG));
		props.put(ProducerConfig.LINGER_MS_CONFIG, env.getProperty(ProducerConfig.LINGER_MS_CONFIG));
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, env.getProperty(ProducerConfig.BUFFER_MEMORY_CONFIG));
		props.put(ProducerConfig.ACKS_CONFIG, env.getProperty(ProducerConfig.ACKS_CONFIG));
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		return props;
	}


	public ProducerFactory<String, Object> producerFactory() {
		return new DefaultKafkaProducerFactory<>(producerConfigs());
	}

	@Bean
	public KafkaTemplate<String, Object> kafkaTemplate() {
		// Bean을 통하여 의존성 주입
		return new KafkaTemplate<>(producerFactory());
	}
}
