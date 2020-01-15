package kr.ac.hongik.apl.demo.Controller;

import kr.ac.hongik.apl.demo.Service.KafkaListenerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@Slf4j
public class KafkaProducingController {
	@Autowired
	KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	KafkaListenerService kafkaListenerService;

	@RequestMapping(value="/push")
	@ResponseBody
	public String getData(@RequestParam(value = "message", required = true, defaultValue = "") String message ){
		kafkaTemplate.send("Lee", message);
		return String.format("message published to kafka, msg : %s", message);
	}

	@RequestMapping(value = "/start")
	@ResponseBody
	public String start() {
		kafkaListenerService.startConsumer();
		return "started";
	}
}
