package io.github.virtualscheduler.demo.realtime;

import io.github.virtualscheduler.scheduler.DynamicAbstractSchedulerSE;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

/**
 * 실시간 알림 발송 스케줄러 데모.
 *
 * <p>결제 승인, 입금 알림 등 실시간성이 중요한 발송 채널.
 * 모든 노드에서 {@code startExecutor()}로 기동하여 신규 요청을 즉시 처리한다.
 *
 * <h2>AS-IS 문제</h2>
 * <pre>
 *   재전송(reSend) 500건이 new Thread().start() 중일 때
 *   실시간 10건이 같은 JVM에 투입되면 플랫폼 스레드 경쟁 발생.
 *   DB 커넥션 풀을 재전송이 선점하면 실시간 응답이 수백ms~수초 지연.
 * </pre>
 *
 * <h2>TO-BE 해결</h2>
 * <pre>
 *   독립 ExecutorService → 재전송 executor와 완전히 분리.
 *   실시간 10건은 자체 Semaphore(permits=20)로 처리.
 *   재전송이 아무리 바빠도 실시간 응답 무영향.
 * </pre>
 */
@Slf4j
@Service
public class RealtimeNotificationScheduler extends DynamicAbstractSchedulerSE {

    public RealtimeNotificationScheduler() {
        setSemaphorePermits(20);
    }

    /**
     * 실시간 발송 요청 처리.
     *
     * <p>HTTP 요청 수신 시 호출. submit()이 즉시 반환되므로
     * Tomcat 스레드는 블로킹 없이 다음 요청을 받을 수 있다.
     */
    public void sendRealtime(List<NotificationTask> tasks) {
        tasks.forEach(task ->
            submit(task::execute, task.getTaskId())
        );
    }

    @Override
    protected void runner() {
        // 실시간 채널은 스케줄 없음 — startExecutor()로만 기동
    }
}
