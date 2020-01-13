package kr.ac.hongik.apl.demo.Service;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
public class AsyncExecutionService {
	@Async
	public void run(Runnable runnable) {
		runnable.run();
	}
}
