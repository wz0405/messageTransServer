package io.github.virtualscheduler.demo.resend;

import io.github.virtualscheduler.demo.realtime.NotificationTask;
import io.github.virtualscheduler.scheduler.DynamicAbstractSchedulerSE;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.IntStream;

/**
 * 실패 알림 재전송 스케줄러 데모.
 *
 * <p>DB의 재전송 대기 테이블(RE_TABLE)을 주기적으로 조회하여
 * 실패한 알림을 재처리한다.
 * 클러스터 선점 노드에서만 {@code startScheduler()}로 기동한다.
 *
 * <h2>AS-IS vs TO-BE</h2>
 * <pre>
 *   AS-IS: runner()에서 new Thread().start() 500번 호출
 *          → 플랫폼 스레드 500개 = ~500MB OS 스택
 *          → DB 커넥션 100개에 500개 경쟁 → 타임아웃 발생
 *          → 실시간 채널도 같은 DB 풀 사용 → 실시간 응답 지연
 *
 *   TO-BE: runner()에서 submit() 500번 호출
 *          → 가상 스레드 500개 = ~수MB
 *          → Semaphore(200)가 동시 실행 200개로 제한 → DB 풀 안전
 *          → 독립 executor → 실시간 채널에 영향 없음
 * </pre>
 */
@Slf4j
@Service
public class ResendNotificationScheduler extends DynamicAbstractSchedulerSE {

    public ResendNotificationScheduler() {
        setInitialDelay(5);
        setFixedDelay(60);
        setSemaphorePermits(200);
    }

    /**
     * 60초 주기로 재전송 대상 조회 및 처리.
     *
     * <p>실제 구현에서는 DB에서 재전송 대기 레코드를 조회하고
     * 각 건을 submit()으로 가상 스레드에 위임한다.
     * RE_TABLE에 레코드가 이미 존재하므로 kill -15 시에도 다음 기동 때 재처리 가능.
     */
    @Override
    protected void runner() {
        if (isShuttingDown()) return;

        List<NotificationTask> pendingTasks = loadPendingTasks();
        if (pendingTasks.isEmpty()) {
            log.debug("[ResendNotificationScheduler] 재전송 대기 없음");
            return;
        }

        log.info("[ResendNotificationScheduler] 재전송 시작 — {}건", pendingTasks.size());
        pendingTasks.forEach(task ->
            submit(task::execute, task.getTaskId())
        );
    }

    /**
     * 재전송 대기 목록 조회 (데모용 스텁).
     *
     * <p>실제 구현: {@code SELECT * FROM RE_TABLE WHERE STATUS = 'PENDING' LIMIT 500}
     */
    private List<NotificationTask> loadPendingTasks() {
        // 데모용: 5건 반환 (실제: DB 조회)
        return IntStream.range(0, 5)
                .mapToObj(NotificationTask::sample)
                .toList();
    }
}
