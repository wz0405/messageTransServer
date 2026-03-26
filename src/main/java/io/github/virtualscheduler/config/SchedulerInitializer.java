package io.github.virtualscheduler.config;

import com.zaxxer.hikari.HikariDataSource;
import io.github.virtualscheduler.demo.realtime.RealtimeNotificationScheduler;
import io.github.virtualscheduler.demo.resend.ResendNotificationScheduler;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

/**
 * 스케줄러 초기화 설정.
 *
 * <h2>semaphorePermits 자동 계산</h2>
 * <pre>
 *   DB 커넥션 풀 크기(Hikari maxPoolSize) 기준으로 세마포어를 동적 설정.
 *   타임아웃 발생 임계값: T = poolSize × (dbTimeout / latency + 1)
 *
 *   실시간: Math.max(20,  poolSize * 2)
 *   재전송: Math.max(100, poolSize * 4)
 * </pre>
 *
 * <h2>기동 순서</h2>
 * <ol>
 *   <li>{@code @PostConstruct}: semaphorePermits 계산 및 설정</li>
 *   <li>{@code ApplicationReadyEvent}: 실시간 executor 기동 (모든 노드)</li>
 *   <li>클러스터 선점 후: 재전송 스케줄러 기동 (선점 노드만)</li>
 * </ol>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SchedulerInitializer {

    private final DataSource                     dataSource;
    private final RealtimeNotificationScheduler  realtimeScheduler;
    private final ResendNotificationScheduler    resendScheduler;

    @PostConstruct
    public void init() {
        int poolSize = getHikariMaxPoolSize();

        int realtimePermits = Math.max(20,  poolSize * 2);
        int resendPermits   = Math.max(100, poolSize * 4);

        realtimeScheduler.setSemaphorePermits(realtimePermits);
        resendScheduler.setSemaphorePermits(resendPermits);

        log.info("[SchedulerInitializer] Hikari poolSize={} → realtime={}, resend={}",
                poolSize, realtimePermits, resendPermits);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        // 모든 노드: 실시간 발송 executor 기동
        realtimeScheduler.startExecutor();
        log.info("[SchedulerInitializer] 실시간 executor 기동 완료");

        // 재전송 스케줄러는 클러스터 선점 노드에서만 기동
        // 실제 구현: ClusterScheduler.register() 후 resendScheduler.startScheduler() 호출
    }

    private int getHikariMaxPoolSize() {
        if (dataSource instanceof HikariDataSource hds) {
            return hds.getMaximumPoolSize();
        }
        return 10;
    }
}
