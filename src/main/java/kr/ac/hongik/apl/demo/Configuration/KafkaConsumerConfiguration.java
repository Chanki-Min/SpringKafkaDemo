package kr.ac.hongik.apl.demo.Configuration;


import kr.ac.hongik.apl.demo.Service.KafkaListenerService;
import kr.ac.hongik.apl.demo.sensordata.Sensor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
@PropertySource("classpath:kafka.properties")
public class KafkaConsumerConfiguration {
	@Autowired
	Environment env;

	@Bean(name = "consumerConfigs")
	public Map<String, Object> consumerConfigs() {
		Map<String, Object> props = new HashMap<>();

		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, env.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "Lee");
		props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		props.put(JsonDeserializer.TRUSTED_PACKAGES,"com.scheduledproducer.scheduledproducer.sensordata");
		return props;
	}

	@Bean
	public ConsumerFactory<String, Sensor> consumerFactory() {


		return new DefaultKafkaConsumerFactory<>(consumerConfigs(), new StringDeserializer(),
				new JsonDeserializer<>(Sensor.class));
	}
//	new JsonDeserializer<>(Sensor.class)
	@Bean
	public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Sensor>> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, Sensor> kafkaListenerContainerFactory = new ConcurrentKafkaListenerContainerFactory<>();
		kafkaListenerContainerFactory.setConsumerFactory(consumerFactory());
		return kafkaListenerContainerFactory;
	}

	@Bean(name = "listenerServiceConfigs")
	public Map<String, Object> listenerServiceConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(KafkaListenerService.TOPICS, Arrays.asList(env.getProperty(KafkaListenerService.TOPICS)));
		props.put(KafkaListenerService.MIN_BATCH_SIZE, env.getProperty(KafkaListenerService.MIN_BATCH_SIZE));
		return props;
	}
}
