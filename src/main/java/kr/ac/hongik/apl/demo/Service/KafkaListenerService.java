package kr.ac.hongik.apl.demo.Service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.Duration;
import java.util.*;

@Slf4j
@Service
public class KafkaListenerService implements InitializingBean, DisposableBean {
	public static final String TOPICS = "kafka.listener.service.topic";
	public static final String MIN_BATCH_SIZE = "kafka.listener.service.minBatchSize";

	@Resource(name = "consumerConfigs")
	private Map<String, Object> consumerConfigs;
	@Resource(name = "listenerServiceConfigs")
	private Map<String, Object> listerServiceConfigs;

	//Async 노테이션의 메소드는 this가 invoke()할 수 없기 때문에 비동기 실행만 시키는 서비스를 주입한다
	@Autowired
	private AsyncExecutionService asyncExecutionService;

	private KafkaConsumer<String, String> consumer = null;

	@Async("threadPoolTaskExecutor")
	public void startConsumer() {
		try {
			log.info("Start listener service");
			consumer = new KafkaConsumer<>(consumerConfigs);
			consumer.subscribe((Collection<String>) listerServiceConfigs.get(TOPICS));

			List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
			while (true) {

			/*ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> record: records) {
				buffer.add(record);
				log.info(String.format("records size = %d", records.count()));
				log.info(String.format("buffer size = %d", buffer.size()));
			}

			int check=0;
			long lastOffset=0;
			if (buffer.size() >= Integer.parseInt((String) listerServiceConfigs.get(MIN_BATCH_SIZE))) {
				for(ConsumerRecord<String,String> R : buffer){
					System.out.println(R.offset() + " : " + R.value());
					++check;
					if(check == Integer.parseInt((String)listerServiceConfigs.get(MIN_BATCH_SIZE))){
						lastOffset = R.offset();
						break;
					}
				}
				consumer.commitSync();
				buffer.clear();
			}*/

				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
				for(TopicPartition partition : records.partitions()){
					List<ConsumerRecord<String,String>> partitionRecords = records.records(partition);
					long lastOffset=0;
					for( ConsumerRecord<String,String> record : partitionRecords){
						lastOffset = record.offset(); // 제한 시간안에 안올 때를 대비해서 매번 갱신
						buffer.add(record);
						log.info(String.format("buffer size = %d , offset : %d", buffer.size(),record.offset()));
						System.out.println(record.value());
						if(buffer.size() == Integer.parseInt((String)listerServiceConfigs.get(MIN_BATCH_SIZE))){
							//전송
							buffer.clear();
							System.out.println("consumed 1 batch");
							consumer.commitSync(Collections.singletonMap(partition,new OffsetAndMetadata(lastOffset+1)));
						}
					}
				}
			}
		} finally {
			consumer.close();
		}
	}

	public void shutdown() {
		if(consumer != null) {
			consumer.wakeup();
		}
	}

	@Override
	public void destroy() throws Exception {
		this.shutdown();
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		//asyncExecutionService.run(this::startConsumer);
	}
}
