package kr.ac.hongik.apl.demo.Controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import kr.ac.hongik.apl.demo.Service.KafkaListenerService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.types.Field;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Map;

@Controller
@Slf4j
public class KafkaProducingController {
	@Autowired
	KafkaTemplate<String, Object> kafkaTemplate;

	ObjectMapper objectMapper = new ObjectMapper();


	@Autowired
	KafkaListenerService kafkaListenerService;


	@RequestMapping(value="/push")
	@ResponseBody
	public String getData(@RequestParam(value = "message", required = true, defaultValue = "") String message ) throws JsonProcessingException {
		String jsonProcess = "{\"message\":\""+message+"\"}";
		Map objectProcess = objectMapper.readValue(jsonProcess, new TypeReference<Map<String,String>>() {});
		kafkaTemplate.send("Lee2", objectProcess);
		return String.format("message published to kafka, msg : %s", jsonProcess);
	}


	@RequestMapping(value = "/start")
	@ResponseBody
	public String start() {
		kafkaListenerService.startConsumer();
		return "started";
	}

	@RequestMapping(value = "/shutdown")
	@ResponseBody
	public String close() {
		kafkaListenerService.shutdown();
		return "shut down";
	}
}
