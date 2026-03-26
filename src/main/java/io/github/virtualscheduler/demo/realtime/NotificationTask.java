package io.github.virtualscheduler.demo.realtime;

import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * 알림 발송 작업 단위.
 *
 * <p>실제 구현에서는 HTTP 발송 후 DB에 결과를 INSERT/UPDATE한다.
 * 가상 스레드는 HTTP I/O 대기 중 carrier 스레드를 반납하여 OS 스레드를 절약한다.
 */
@Slf4j
@Getter
@Builder
public class NotificationTask {

    private final String taskId;
    private final String targetUrl;
    private final String message;
    private final int    retryCount;

    /**
     * 알림 발송 실행.
     *
     * <p>실제 구현:
     * <ol>
     *   <li>HTTP POST 발송 (가상 스레드 I/O 대기 중 carrier 반납)</li>
     *   <li>발송 결과 DB INSERT</li>
     *   <li>실패 시 재전송 테이블(RE_TABLE)에 INSERT</li>
     * </ol>
     */
    public void execute() {
        log.info("[NotificationTask] 발송 시작 taskId=[{}] url=[{}]", taskId, targetUrl);
        try {
            // HTTP 발송 시뮬레이션 (실제: HttpClient 또는 RestTemplate)
            Thread.sleep(100);
            log.info("[NotificationTask] 발송 완료 taskId=[{}]", taskId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("[NotificationTask] 인터럽트 taskId=[{}]", taskId);
        }
    }

    public static NotificationTask sample(int index) {
        return NotificationTask.builder()
                .taskId(java.util.UUID.randomUUID().toString())
                .targetUrl("https://merchant-" + index + ".example.com/notify")
                .message("{\"type\":\"PAYMENT\",\"amount\":10000}")
                .retryCount(0)
                .build();
    }
}
