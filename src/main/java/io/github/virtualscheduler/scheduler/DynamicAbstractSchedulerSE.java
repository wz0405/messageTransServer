package io.github.virtualscheduler.scheduler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.PeriodicTrigger;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Virtual Thread + Semaphore 기반 동적 스케줄러 추상 클래스.
 *
 * <h2>설계 배경</h2>
 * <p>재전송(reSend)과 실시간 발송(realtime)이 동시에 처리될 때,
 * 기존 {@code new Thread().start()} 방식은 JVM의 플랫폼 스레드를 무제한 생성한다.
 * 이 경우 두 가지 문제가 발생한다:
 * <ol>
 *   <li><b>메모리 폭탄</b>: 플랫폼 스레드 1개 = OS 스택 ~1MB → 1000건 동시 = ~1GB 소비</li>
 *   <li><b>DB 커넥션 고갈</b>: 무제한 스레드가 커넥션 풀에 동시 접근 → 타임아웃 폭발</li>
 * </ol>
 *
 * <h2>해결 전략</h2>
 * <p>Netty나 WebFlux(리액티브 스택) 도입을 검토했으나,
 * Java 21의 Virtual Thread가 동일한 문제를 더 직관적인 코드로 해결함을 확인하여 채택했다.
 *
 * <ul>
 *   <li><b>Virtual Thread</b>: 가상 스레드 수천 개 = 수십MB → 메모리 문제 없음.
 *       I/O 대기 중 carrier 스레드 반납 → OS 스레드 재사용.</li>
 *   <li><b>Semaphore(fair=true)</b>: 동시 실행 수를 DB 풀 크기 기준으로 제한.
 *       fair=true로 FIFO 순서 보장 → 요청 접수 순서대로 처리.</li>
 *   <li><b>독립 ExecutorService</b>: 재전송/실시간 채널이 각자의 executor를 가짐.
 *       재전송이 아무리 바빠도 실시간 채널에 영향 없음.</li>
 *   <li><b>Graceful shutdown</b>: {@code @PreDestroy} → {@code awaitTermination(60s)}
 *       → kill -15 시 처리 중인 작업 전부 완료 후 JVM 종료.</li>
 * </ul>
 *
 * <h2>타임아웃 발생 임계값</h2>
 * <pre>
 *   T = DB_POOL_SIZE × (DB_TIMEOUT_MS / LATENCY_MS + 1)
 *   예) poolSize=100, timeout=500ms, latency=100ms → T = 600건
 *   동시 요청 600건 초과 시 AS-IS는 타임아웃 폭발, TO-BE는 세마포어가 흡수
 * </pre>
 */
@Slf4j
public abstract class DynamicAbstractSchedulerSE {

    private static final int DEFAULT_INITIAL_DELAY_SEC = 5;
    private static final int DEFAULT_FIXED_DELAY_SEC   = 60;

    protected int initialDelay    = DEFAULT_INITIAL_DELAY_SEC;
    protected int fixedDelay      = DEFAULT_FIXED_DELAY_SEC;
    protected int semaphorePermits = 20;

    private final AtomicBoolean                      shutdownFlag  = new AtomicBoolean(false);
    private final AtomicReference<ScheduledFuture<?>>futureRef     = new AtomicReference<>();
    private final AtomicReference<ThreadPoolTaskScheduler> schedulerRef = new AtomicReference<>();

    private volatile ExecutorService executorService;
    private volatile Semaphore        semaphore;

    // ─── 기동/중단 ──────────────────────────────────────────────────────────

    /**
     * 신규 발송 전용 executor 기동.
     *
     * <p>스케줄 없이 executor + semaphore만 초기화한다.
     * 모든 노드(비선점 포함)가 실시간 발송을 처리할 수 있도록
     * 애플리케이션 기동 시 호출한다.
     */
    public void startExecutor() {
        shutdownFlag.set(false);
        if (executorService == null || executorService.isShutdown()) {
            semaphore       = new Semaphore(semaphorePermits, true);
            executorService = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());
            log.info("[{}] executor 초기화 — semaphorePermits={}",
                    getClass().getSimpleName(), semaphorePermits);
        }
    }

    /**
     * 재전송 스케줄러 기동.
     *
     * <p>executor + semaphore를 재생성하고 주기 스케줄을 등록한다.
     * 클러스터 선점 노드에서만 호출한다.
     */
    public void startScheduler() {
        stopScheduler();
        shutdownFlag.set(false);
        semaphore       = new Semaphore(semaphorePermits, true);
        executorService = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());

        ThreadPoolTaskScheduler newScheduler = new ThreadPoolTaskScheduler();
        newScheduler.setPoolSize(1);
        newScheduler.setThreadNamePrefix(getClass().getSimpleName() + "-scheduler-");
        newScheduler.initialize();

        PeriodicTrigger trigger = new PeriodicTrigger(Duration.ofSeconds(fixedDelay));
        trigger.setFixedRate(false);
        trigger.setInitialDelay(Duration.ofSeconds(initialDelay));

        ScheduledFuture<?> future = newScheduler.schedule(this::safeRunner, trigger);
        schedulerRef.set(newScheduler);
        futureRef.set(future);

        log.info("[{}] 스케줄러 기동 — delay={}s, period={}s, semaphore={}",
                getClass().getSimpleName(), initialDelay, fixedDelay, semaphorePermits);
    }

    /**
     * 스케줄러 및 executor 중단.
     *
     * <p>drainSemaphore()로 acquire 대기 중인 스레드를 해제한 뒤
     * awaitTermination으로 실행 중인 작업 완료를 기다린다.
     */
    public void stopScheduler() {
        shutdownFlag.set(true);
        drainSemaphore();
        shutdownExecutor();

        ScheduledFuture<?> future = futureRef.getAndSet(null);
        if (future != null) future.cancel(false);

        ThreadPoolTaskScheduler old = schedulerRef.getAndSet(null);
        if (old != null) old.shutdown();
    }

    // ─── 작업 제출 ─────────────────────────────────────────────────────────

    /**
     * 작업을 가상 스레드 executor에 제출한다.
     *
     * <p>세마포어로 동시 실행 수를 제한하여 DB 커넥션 폭증을 방지한다.
     * {@code taskId}는 InterruptedException 발생 시 로그에 기록되어
     * 유실 건 수기 확인에 활용된다.
     *
     * @param task   실행할 작업 (NotiTxThread::run 등)
     * @param taskId 거래 식별자 (tid, uuid 등) — 유실 추적용
     */
    public void submit(Runnable task, String taskId) {
        if (executorService == null || executorService.isShutdown()) {
            log.warn("[{}] ExecutorService 미초기화 — submit 스킵 taskId=[{}]",
                    getClass().getSimpleName(), taskId);
            return;
        }
        executorService.submit(() -> {
            boolean acquired = false;
            try {
                semaphore.acquire();
                acquired = true;
                log.debug("[{}] 실행 시작 taskId=[{}]", getClass().getSimpleName(), taskId);
                task.run();
                log.debug("[{}] 실행 완료 taskId=[{}]", getClass().getSimpleName(), taskId);
            } catch (InterruptedException e) {
                log.warn("[{}] 인터럽트 — 미처리 taskId=[{}] 수기 확인 필요",
                        getClass().getSimpleName(), taskId);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.error("[{}] 실행 오류 taskId=[{}]", getClass().getSimpleName(), taskId, e);
            } finally {
                if (acquired) semaphore.release();
            }
        });
    }

    /** taskId 없는 단순 제출 오버로드 */
    public void submit(Runnable task) {
        submit(task, "unknown");
    }

    // ─── 추상 메서드 ────────────────────────────────────────────────────────

    /**
     * 스케줄 주기마다 실행할 비즈니스 로직.
     *
     * <p>구현 클래스에서 재전송 대상 조회 및 처리를 정의한다.
     */
    protected abstract void runner();

    // ─── 내부 유틸 ─────────────────────────────────────────────────────────

    private void safeRunner() {
        if (isShuttingDown()) return;
        try {
            runner();
        } catch (Exception e) {
            log.error("[{}] runner 오류", getClass().getSimpleName(), e);
        }
    }

    /**
     * acquire 대기 중인 스레드를 즉시 해제한다.
     *
     * <p>세마포어의 역할은 폭증 방지(동시 실행 수 상한)이다.
     * shutdown 시 acquire 대기로 영원히 블로킹되는 상황을 방지하기 위해
     * 대기 큐를 전부 drain한다. 해제된 스레드는 {@code task.run()}을 실행하고
     * {@code awaitTermination}이 완료를 기다린다.
     */
    private void drainSemaphore() {
        if (semaphore == null) return;
        int waiting = semaphore.getQueueLength();
        if (waiting > 0) {
            semaphore.release(waiting);
            log.info("[{}] drainSemaphore — {}개 해제", getClass().getSimpleName(), waiting);
        }
    }

    private void shutdownExecutor() {
        ExecutorService es = executorService;
        if (es == null || es.isShutdown()) return;
        es.shutdown();
        try {
            if (!es.awaitTermination(60, TimeUnit.SECONDS)) {
                log.warn("[{}] shutdown 타임아웃(60s) — 강제 종료, 미처리 건은 RE_TABLE에서 재처리",
                        getClass().getSimpleName());
                es.shutdownNow();
            } else {
                log.info("[{}] shutdown 완료", getClass().getSimpleName());
            }
        } catch (InterruptedException e) {
            es.shutdownNow();
            Thread.currentThread().interrupt();
        }
        executorService = null;
    }

    protected boolean isShuttingDown() {
        return shutdownFlag.get();
    }

    // ─── Setter ─────────────────────────────────────────────────────────────

    public void setSemaphorePermits(int semaphorePermits) {
        this.semaphorePermits = semaphorePermits;
    }

    public void setInitialDelay(int initialDelay) {
        this.initialDelay = initialDelay;
    }

    public void setFixedDelay(int fixedDelay) {
        this.fixedDelay = fixedDelay;
    }
}
