package kr.ac.hongik.apl.demo.Service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Service
public class KafkaListenerService implements InitializingBean, DisposableBean {
	public static final String TOPICS = "kafka.listener.service.topic";
	public static final String MIN_BATCH_SIZE = "kafka.listener.service.minBatchSize";
	private final AtomicBoolean closed = new AtomicBoolean(false);

	@Resource(name = "consumerConfigs")
	private Map<String, Object> consumerConfigs;
	@Resource(name = "listenerServiceConfigs")
	private Map<String, Object> listerServiceConfigs;

	//Async 노테이션의 메소드는 this가 invoke()할 수 없기 때문에 비동기 실행만 시키는 서비스를 주입한다
	@Autowired
	private AsyncExecutionService asyncExecutionService;

	private KafkaConsumer<String, Object> consumer = null;

	@Async("threadPoolTaskExecutor")
	public void startConsumer() {
		try {
			log.info("Start listener service");
			consumer = new KafkaConsumer<>(consumerConfigs);
			consumer.subscribe((Collection<String>) listerServiceConfigs.get(TOPICS));
			long timeOut,start,end,Interval;
			long lastOffset=0;
			TopicPartition p = null;
			timeOut = 0;
			List<ConsumerRecord<String, Object>> buffer = new ArrayList<>(); // 로컬 저장
			while (true) {
				start = System.currentTimeMillis();
				ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(1000));
				end = System.currentTimeMillis();
				Interval = (end - start)/1000;
				timeOut += Interval;
				if(timeOut > 5.0){
					timeOut = 0;
					if(buffer.size() > 0){
						buffer.clear();
						log.info("consumed uncompleted batch");
						consumer.commitSync(Collections.singletonMap(p,new OffsetAndMetadata(lastOffset+1))); // 오프셋 커밋
					}
					else{
						log.info("no data");
					}
				}

				for(TopicPartition partition : records.partitions()){
					timeOut = 0;
					p = partition;
					List<ConsumerRecord<String,Object>> partitionRecords = records.records(partition);
					for( ConsumerRecord<String, Object> record : partitionRecords){
						lastOffset = record.offset();
						// 최신 오프셋을 버퍼가 기준치를 충족 했을 때만 if문 안에서 업데이트 해줄 수 있지만,
						// 타임 아웃에도 읽은 만큼을 커밋해줘야하기 때문에 항상 기준치 크기만큼 커밋되는 것이 아니라
						// 매번 최신 오프셋을 업뎃하는게 좋아 보여요

						buffer.add(record); // buffer에 담기
						log.info(String.format("buffer size = %d , offset : %d , value : %s", buffer.size(),record.offset(),record.value()));
						if(buffer.size() == Integer.parseInt((String)listerServiceConfigs.get(MIN_BATCH_SIZE))){ // 버퍼가 기준치만큼 차면
							buffer.clear(); // 버퍼 초기화
							log.info("consumed 1 batch");
							consumer.commitSync(Collections.singletonMap(partition,new OffsetAndMetadata(lastOffset+1))); // 오프셋 커밋
						}
					}
				}
			}
		} catch (WakeupException e) {
			// 정상적으로 아토믹 불리언이 false이라면 예외를 무시하고 종료한다
			if (!closed.get()) throw e;
		} finally {

			consumer.close();
			System.out.println("끝났지렁");
		}
	}

	public void shutdown() {
		if(consumer != null) {
			closed.set(true);
			consumer.wakeup();
		}
	}

	@Override
	public void destroy() throws Exception {
		this.shutdown();
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		asyncExecutionService.run(this::startConsumer);
		// 2번 문제 실험시 위 실행 명령어를 주석처리
	}
}
