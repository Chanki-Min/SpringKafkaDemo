package kr.ac.hongik.apl.demo;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.time.LocalDateTime;

@Controller
@Slf4j
public class KafkaProducingController {
	@Autowired
	KafkaTemplate<String, String> kafkaTemplate;


	@RequestMapping(value="/push")
	public String getData(@RequestParam(value = "message", required = true, defaultValue = "") String message ){
		kafkaTemplate.send("demoTopic", message);
		return String.format("message published to kafka, msg : %s", message);
	}
}
