package kr.ac.hongik.apl.demo.Service;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class Ack {
    @RequestMapping()
    public void SendAck(Object lastRecord){

    }
}
