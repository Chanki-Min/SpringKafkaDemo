package kr.ac.hongik.apl.demo.Service;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

public interface ConsumingPbftClient extends InitializingBean, DisposableBean {
    /**
     * 컨슈머가 어떤 타입을 key-value로 가져올지는 도메인 마다 다르므로 객체 생성시 정의한다
     */

    /**
     * 컨슈머가 돌아가지 않고 있다면, 컨슈머 객체를 생성하고, loop를 돌면서 poll() 하고 일정 조건 만족시 execute()를 호출합니다
     */
    public abstract void startConsumer();

    /**
     * 조건 만족시 실제 실행 로직은 여기에 작성한다
     */
    public abstract void execute();

    /**
     * AtomicBoolean과 Consumer.wakeup()을 통하여 thread-safe 하게 컨슈머를 종료합니다
     */
    public abstract void shutdownConsumer();

    /**
     * 생성자 호출 이후에 바로 실행할 로직을 작성합니다
     */
    @Override
    public abstract void afterPropertiesSet() throws Exception;

    /**
     * 객체 소멸 직전에 실행할 로직을 작성합니다
     */
    @Override
    public abstract void destroy() throws Exception;
}
