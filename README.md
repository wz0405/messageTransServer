# virtual-thread-scheduler

> Java 21 Virtual Thread + Semaphore 기반 동적 스케줄러

[![Java](https://img.shields.io/badge/Java-21-orange)](https://openjdk.org/projects/jdk/21/)
[![Spring Boot](https://img.shields.io/badge/Spring_Boot-3.3-green)](https://spring.io/projects/spring-boot)
[![License](https://img.shields.io/badge/License-MIT-blue)](LICENSE)

---

## 개발 배경

결제 시스템에서 **재전송(reSend)** 과 **실시간 발송(realtime)** 이 동시에 처리될 때 다음 문제가 있었다.

### AS-IS 문제 (`new Thread().start()`)

```
재전송 500건 → new Thread().start() × 500
  → 플랫폼 스레드 500개 = OS 스택 ~500MB 소비
  → DB 커넥션 풀(100개)에 500개가 동시 접근
  → 400개는 커넥션 대기 → WAS 전체가 느려짐
  → 실시간 발송도 같은 DB 풀 경쟁 → 실시간 응답 수백ms~수초 지연
  → kill -15 명령어로 프로세스를 종료하더라도, 스레드로 등록된 I/O를 제어하기 어려움
```

### 고려했던 대안

**Netty / WebFlux(리액티브 스택)** 도입을 검토했으나:
- 기존 명령형(imperative) 코드베이스와의 패러다임 충돌
- 콜백 체인으로 인한 디버깅 난이도 증가
- 팀 러닝 커브

**Java 21 Virtual Thread** 가 동일한 문제를 더 직관적인 코드로 해결함을 확인하여 채택했다.  
또한 Spring 라이프사이클에 스레드를 위임할 수 있다는 점에서 이 방식을 채택했다.

---

## 핵심 설계

```
┌─────────────────────────────────────────────────────────────┐
│                 DynamicAbstractSchedulerSE                  │
│                                                             │
│  ExecutorService (Virtual Thread per task)                  │
│  ├── 가상 스레드 1개 = KB 단위  (플랫폼 스레드 1개 = ~1MB)      │
│  └── I/O 대기 중 carrier 스레드 반납 → OS 스레드 재사용         │
│                                                             │
│  Semaphore(permits, fair=true)                              │
│  ├── 동시 실행 수를 DB 풀 크기 기준으로 제한 (폭증 방지)          │
│  └── fair=true → FIFO → 요청 접수 순서대로 처리               │
│                                                             │
│  독립 ExecutorService                                        │
│  ├── 재전송 채널 executor (클러스터 선점 노드)                   │
│  └── 실시간 채널 executor (모든 노드)                          │
│      → 재전송이 아무리 바빠도 실시간에 영향 없음                   │
│                                                             │
│  Spring 라이프사이클 연동                                      │
│  ├── @EventListener(ApplicationReadyEvent) → startExecutor  │
│  └── @PreDestroy → stopScheduler → awaitTermination(60s)   │
│      → kill -15 시 처리 중인 작업 전부 완료 보장                 │
└─────────────────────────────────────────────────────────────┘
```

### 타임아웃 발생 임계값 공식

```
T = DB_POOL_SIZE × (DB_TIMEOUT_MS / LATENCY_MS + 1)

예시 (poolSize=100, timeout=500ms, latency=100ms):
  T = 100 × (500/100 + 1) = 600건

동시 요청 600건 초과 시:
  AS-IS → DB 타임아웃 폭발
  TO-BE → Semaphore(200)가 흡수 → 전량 처리
```

---

## 프로젝트 구조

```
src/main/java/io/github/virtualscheduler/
├── scheduler/
│   └── DynamicAbstractSchedulerSE.java   # 핵심 추상 스케줄러
├── demo/
│   ├── realtime/
│   │   ├── RealtimeNotificationScheduler.java
│   │   └── NotificationTask.java
│   └── resend/
│       └── ResendNotificationScheduler.java
└── config/
    └── SchedulerInitializer.java          # 기동 시 semaphorePermits 자동 계산

src/test/java/io/github/virtualscheduler/
└── VirtualThreadSchedulerTest.java        # AS-IS vs TO-BE 비교 테스트 (7개)
```

---

## 성능 비교 (테스트 결과)

> 환경: JVM 500MB, DB 커넥션 풀 100개, DB 타임아웃 500ms

| 시나리오 | AS-IS (`new Thread`) | TO-BE (Virtual Thread + Semaphore) |
|---|---|---|
| 동시 800건 DB 접근 | 타임아웃 **200건+** 발생 | 타임아웃 **0건**, 전량 성공 |
| 스레드 메모리 (800건) | ~800MB OS 스택 | **수십MB** |
| 재전송 500건 중 실시간 10건 | 실시간 평균 대기 **수백ms** | 실시간 평균 대기 **~0ms** |
| kill -15 처리 중 100건 | 유실 가능 | **전량 완료** 후 종료 |
| 동시 실행 수 제어 | 무제한 | **Semaphore(200) 이하** 보장 |

---

## 주요 컴포넌트

### `DynamicAbstractSchedulerSE`

| 메서드 | 설명 |
|---|---|
| `startExecutor()` | 실시간 발송용 executor 기동 (모든 노드) |
| `startScheduler()` | 재전송 스케줄러 기동 (선점 노드만) |
| `stopScheduler()` | Graceful shutdown — drainSemaphore → awaitTermination(60s) |
| `submit(task, taskId)` | 가상 스레드에 작업 제출 — 세마포어 제어 포함 |
| `runner()` | 추상 메서드 — 재전송 조회 및 처리 로직 구현 |

### `SchedulerInitializer`

```java
@PostConstruct
void init() {
    int poolSize = hikari.getMaximumPoolSize();
    realtimeScheduler.setSemaphorePermits(Math.max(20,  poolSize * 2));
    resendScheduler.setSemaphorePermits(Math.max(100, poolSize * 4));
}

@EventListener(ApplicationReadyEvent.class)
void onReady() {
    realtimeScheduler.startExecutor(); // 모든 노드: 실시간 발송 준비
    // 재전송: 클러스터 선점 후 resendScheduler.startScheduler() 호출
}
```

---

## Graceful Shutdown 동작

```
kill -15
  ├── Tomcat: 새 HTTP 요청 거부, 진행 중인 요청 완료 대기
  └── @PreDestroy → stopScheduler()
        ├── drainSemaphore()    — acquire 대기 스레드 즉시 해제 (막는 게 아니라 빨리 실행)
        ├── executor.shutdown() — 새 submit 거부, 큐 잔여 작업은 계속 실행
        └── awaitTermination(60s) — 전부 완료될 때까지 최대 60초 대기

재전송(RE_TABLE 기반): kill -15 시점과 무관하게 안전
  → DB에 레코드 이미 존재 → 다음 기동 시 재처리
실시간(신규 발송): LB에서 선제 제거 후 kill -15 권장
  → 큐 잔여 건이 60초 내 소진됨
```

---

## 실행 방법

```bash
# 빌드
./gradlew build

# 테스트 실행 (AS-IS vs TO-BE 비교 결과 출력)
./gradlew test
```

### 테스트 출력 예시

```
┌─ [AS-IS] 동시 800건 — DB 커넥션 고갈
│  성공=605건 | 타임아웃=195건 | 최대 동시 DB=100 | 소요=758ms
└→ 플랫폼 스레드 800개 × 1MB ≈ ~800MB OS 스택 | 임계값(600) 초과 → 타임아웃 폭발

┌─ [TO-BE] 동시 800건 — 세마포어 보호
│  성공=800건 | 타임아웃=0건 | 최대 동시 DB=100 | 소요=873ms | 이론최소=800ms
└→ 가상 스레드 800개 = 수십MB | Semaphore(200) → 타임아웃 없음

┌─ [AS-IS] 재전송 500 + 실시간 10 — DB 공유
│  실시간 평균 DB 대기=405ms | DB 풀=100개 공유
└→ 재전송이 커넥션 선점 → 실시간이 대기 → WAS 응답 지연

┌─ [TO-BE] 재전송 500 + 실시간 10 — 독립 executor
│  실시간 평균 대기=0ms | 재전송/실시간 executor 완전 분리
└→ 독립 executor → 재전송 DB 점유가 실시간에 영향 없음 | 실시간 10건 < Semaphore(200) → 즉시 처리

┌─ [AS-IS] kill -15 시뮬레이션
│  kill 시점 완료=0건 | 실제 완료=100건 | 유실 가능=100건
└→ join() 없음 → kill -15 시 처리 중인 100건 유실 | RE_TABLE 미생성

┌─ [TO-BE] kill -15 → graceful shutdown
│  submit된 100건 모두 완료=100건
└→ awaitTermination(60s) → 큐에 남은 작업 전부 처리 후 JVM 종료 | 데이터 유실 없음

┌─ [TO-BE] 세마포어 동시 실행 제한
│  완료=500건 | 최대 동시 실행=200건 | semaphorePermits=200
└→ 최대 동시 실행 ≤ 200 → DB 풀 보호 | fair=true → FIFO 순서 보장```
```

---

## 요구사항

- Java 21+
- Spring Boot 3.3+
- Lombok

---

## License

MIT