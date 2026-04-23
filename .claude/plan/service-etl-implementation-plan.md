# Service ETL 구현 계획

## 1. 목표

`ingest` 스키마를 truth source로 사용해 `service` 스키마 전체를 증분 ETL로 유지한다.

- 기본 실행 경로는 `ingest` 동기화 성공 직후 자동 실행이다.
- 수동 진입점은 `/api/batch` 하위 경로를 사용할 수 있지만, 구현 소유는 `batch`가 아니라 상위 orchestration/admin 계층이 가진다.
- `service ETL` 실패가 `ingest` 성공 상태를 실패로 되돌리면 안 된다.

## 1.1 현재 진행 상태

- Slice 1은 승인 완료 상태다.
- Slice 2 차원 테이블 증분 ETL은 승인 완료 상태다.
- Slice 3 affected `game_id` 계산기도 승인 완료 상태다.
- Slice 4 핵심 game projection 재구성도 승인 완료 상태다.
- Slice 5 game 종속 bridge projection 재구성도 승인 완료 상태다.
- Slice 5A DB 분리 대응도 승인 완료 상태다.
- 다음 작업 시작점은 Slice 6 미디어/부가 projection 재구성이다.
- Slice 5 승인 이후에는 Slice 6으로 바로 가지 않고, `Slice 5A. DB 분리 대응: Datasource/Repository 분리`를 선행한다.
- Slice 1 관련 잔여 이슈는 모두 non-blocking 후속 개선 항목으로 관리한다.
- Slice 2 관련 잔여 이슈는 모두 non-blocking 후속 개선 항목으로 관리한다.
- Slice 3 관련 잔여 이슈는 모두 non-blocking 후속 개선 항목으로 관리한다.
- Slice 4 관련 잔여 이슈도 모두 non-blocking 후속 개선 항목으로 관리한다.
- Slice 5 관련 잔여 이슈도 모두 non-blocking 후속 개선 항목으로 관리한다.

### Slice 2 후속 메모

- 대량 초기 동기화 시 메모리 사용량
- 차원 테이블 null 허용 계약의 장기 유지 여부

### Slice 3 후속 메모

- Slice 4 도입 전에는 dry-run으로 유지되어 cursor가 전진하지 않았음
- 기존 환경에 남아 있을 수 있는 Slice 3 cursor는 운영 절차로 확인 필요
- JDBC/Flyway 기준 repository 통합 테스트는 후속 보강 대상

### Slice 4 후속 메모

- 대규모 affected set에 대한 chunk 처리와 단일 트랜잭션 길이 모니터링
- 핵심 projection rebuild SQL의 repository 통합 테스트 보강
- `service.game.updated_at` 활용 consumer가 생길 경우 false positive 변경 신호 여부 재검토
- core source는 projection diff 기반으로 유지되고 deferred source는 dry-run으로 남는 전략을 Slice 5/6에서도 깨지지 않게 유지
- nullable migration이 장기 계약인지 이행용 완충인지 후속 문서화

### Slice 5 후속 메모

- bridge rebuild SQL과 diff SQL은 Postgres 의존 구문이 많아 repository 통합 테스트 보강 우선순위가 높다.
- 더 이상 사용하지 않는 dry-run/helper 코드와 구 cursor 기반 repository 메서드는 정리 후보다.
- source log의 `processedRows`가 실제 projection row 수가 아니라 source별 affected game 수라는 의미를 문서/로그에서 혼동하지 않도록 주의한다.
- relation 회귀를 고정하는 integration test는 추가됐지만 기본 `test` task에는 포함되지 않으므로 CI/로컬 실행 전략을 함께 관리할 필요가 있다.
- 현재 integration test는 수기 스키마 기반이어서, 장기적으로는 실제 Flyway 스키마 기준 검증으로 옮기는 편이 안전하다.

### Slice 5A 후속 메모

- datasource/repository/transaction boundary 분리는 현재 기준으로 완료된 것으로 본다.
- `application.yml.sample`에 분리 datasource 설정 예시가 아직 드러나지 않아 운영 재현성 보강이 필요하다.
- split-DB 핵심 회귀 테스트가 `integrationTest` task에만 있으므로 CI와 로컬 기본 워크플로에 반영할지 결정이 필요하다.
- 완전히 다른 JDBC URL을 쓰는 end-to-end 검증은 아직 후속 보강 대상이다.
- 권장 방향은 `ingest` source 추출 repository와 `service` projection repository를 분리하고, 필요 시 `service` 측 staging/temp snapshot을 도입하는 것이다.

## 2. 확정 제약사항

- ETL 대상은 `service` 스키마 전체다.
- 방식은 `incremental`이다.
- 삭제 반영 기준은 `IGDB 원천`이 아니라 현재 `ingest` 상태다.
- 장기적으로 `ingest DB`와 `service DB`는 물리적으로 분리될 가능성이 높다.
- 모듈 소유권은 `batch=ingest`, `calendar=service`로 고정한다.
- `batch` 모듈은 `service` 스키마를 직접 쓰면 안 된다.
- `calendar` 모듈은 `service ETL`과 `service` 전용 로그/커서를 소유한다.
- `calendar` 모듈은 `batch` 코드에 의존하지 않고, 필요 시 `ingest` 스키마를 자체 SQL/DAO로 읽는다.
- 다만 최종적으로는 `ingest`와 `service`를 같은 SQL에서 함께 참조하는 query는 남기지 않는다.
- 단계 ii 이후에는 `ingest`와 `service`가 서로 다른 datasource를 가지므로, 하나의 repository/DAO가 두 DB를 함께 다루면 안 된다.
- 단계 ii 이후 ETL 트랜잭션은 `service` DB 로컬 트랜잭션만 전제하고, `ingest` 읽기는 별도 단계로 취급한다.
- `ingest` 성공 후 `service ETL` 자동 연계는 `batch -> calendar` 직접 호출이 아니라 상위 orchestration/event 계층으로 연결한다.
- `service ETL`은 `ingest`와 별도 로그, 별도 커서, 별도 성공/실패 상태를 가진다.
- `service ETL`의 `service` 적용 단계는 `service DB` 기준 단일 로컬 트랜잭션으로 실행한다.
- `ingest`에서 페이지네이션 보호 로직(`MAX_LOOP_GUARD` 등)이 발동한 경우, 그 실행은 성공으로 간주하면 안 된다.
- 보호 로직에 의한 조기 종료는 실패로 승격되어야 하며, `completed` 기록, 커서 전진, downstream 트리거를 모두 막아야 한다.
- 검증은 `affected scope set diff`를 주 기준으로 사용한다.
- mismatch가 1건이라도 나오면 전체 롤백 후 1회 자동 재시도한다.
- 재시도 후에도 실패하면 최종 실패로 처리하고 커서는 전진시키지 않는다.
- 수동 `service ETL only` API는 비동기 백그라운드 실행이며 동시 1개만 허용한다.

## 3. ETL 전략

### 3.1 하이브리드 증분 방식

- 차원 테이블은 테이블별 delta 반영으로 처리한다.
- `game` 종속 projection은 affected `game_id` 기준 replace로 처리한다.

### 3.2 초기 실행

- `service ETL` 커서가 없거나 초기 실행이면 예외적으로 `ingest` 전체를 기준으로 `service` 전체를 1회 구축한다.

### 3.3 차원 테이블 처리 원칙

- `create/update`: 차원 테이블만 delta upsert
- `delete`: 관련 game 참조를 먼저 정리한 뒤, `ingest`에 존재하지 않는 고아 차원만 삭제
- 공용 차원 수정은 관련 game 재구성 없이 차원 테이블만 갱신

예상 차원 테이블 범위:

- `service.game_status`
- `service.game_type`
- `service.language`
- `service.region`
- `service.release_region`
- `service.release_status`
- `service.genre`
- `service.theme`
- `service.player_perspective`
- `service.game_mode`
- `service.keyword`
- `service.language_support_type`
- `service.website_type`
- `service.platform_logo`
- `service.platform_type`
- `service.platform`
- `service.company`

### 3.4 affected `game_id` 계산 원칙

게임에 직접 연결된 원천 테이블 전체를 포함해 affected `game_id`를 계산한다.

- `ingest.game`
- `ingest.release_date`
- `ingest.involved_company`
- `ingest.language_support`
- `ingest.game_localization`
- `ingest.cover`
- `ingest.artwork`
- `ingest.screenshot`
- `ingest.game_video`
- `ingest.website`
- `ingest.alternative_name`

### 3.5 game 종속 projection 처리 원칙

affected `game_id`마다 아래 테이블을 replace 방식으로 재구성한다.

- `service.game`
- `service.game_release`
- `service.game_language`
- `service.game_genre`
- `service.game_theme`
- `service.game_player_perspective`
- `service.game_game_mode`
- `service.game_keyword`
- `service.game_company`
- `service.game_relation`
- `service.game_localization`
- `service.cover`
- `service.artwork`
- `service.screenshot`
- `service.game_video`
- `service.website`
- `service.alternative_name`

### 3.6 루트 game 삭제

- `service.game`에는 존재하지만 `ingest.game`에는 존재하지 않는 `game_id`를 루트 삭제 대상으로 본다.
- 루트 game 삭제를 기준으로 하위 데이터는 FK cascade와 선행 정리로 제거한다.

### 3.7 분리 단계 대응 원칙

- 단계 i: 서비스(`batch`, `calendar`)는 분리될 수 있지만, 당분간 두 DB 모두 접근 가능할 수 있다.
- 단계 ii: `ingest DB`와 `service DB`는 분리되며, 각 서비스가 두 DB를 읽더라도 한 SQL에서 둘을 join하지 않고, 한 repository가 두 DB를 함께 다루지 않는다.
- 단계 iii: `batch+ingest`, `calendar+service`만 남기고 source 전달은 HTTP/RPC 등 프로토콜 기반으로 전환한다.
- 따라서 Slice 5 이후에는 media projection을 더 늘리기 전에, 기존 ETL을 "source 추출"과 "service 적용" 두 단계 및 두 datasource/repository 경계로 정리하는 선행 리팩터링이 필요하다.

## 4. 로그와 커서

`calendar` 모듈 소유의 `service ETL` 전용으로 아래를 추가한다.

- 실행 1건 단위 로그 테이블
- 원천 테이블별 처리 로그 테이블
- 검증 mismatch 상세 로그 테이블
- 원천 테이블별 커서 테이블

로그 원칙:

- `ingest` 로그와 완전히 분리
- mismatch 총건수는 모두 기록
- mismatch 상세 row는 최대 100건만 저장

## 5. 실패와 재시도

- 참조 불일치나 검증 mismatch가 발생하면 ETL 전체를 실패 처리하고 롤백한다.
- 실패 시 `service ETL` 커서는 갱신하지 않는다.
- 자동 재시도는 새 트랜잭션에서 ETL 전체를 다시 실행한다.
- 검증 실패도 1회 자동 재시도 대상에 포함한다.
- 2회 연속 실패 시 최종 실패 상태와 mismatch 요약을 로그에 남긴다.

## 6. 수동 실행 API

- 수동 실행 endpoint 경로는 `/api/batch` 하위를 사용할 수 있다.
- 단, controller/handler 구현 위치는 `batch`가 아니라 상위 orchestration/admin 패키지다.
- 경로명은 구현 시 적절히 선택하되, 권장 예시는 `/api/batch/service-sync`다.
- 이미 실행 중이면 `409 Conflict`
- 실행 시작 시 `202 Accepted`
- 결과 확인은 로그와 전용 실행 로그 기준

## 7. 구현 단계

1. 모듈 경계를 먼저 고정한다.
2. `calendar` 모듈에 `service ETL` 서비스, repository, 검증, 로그/커서 책임을 둔다.
3. `batch` 모듈은 계속 `ingest` 적재와 `ingest` 로그만 소유하도록 유지한다.
4. 상위 orchestration/admin 패키지를 도입해 `ingest` 완료 후 `service ETL` 자동 연계를 담당하게 한다.
5. `service` 전용 로그/커서 마이그레이션을 추가한다.
6. ETL 동시 실행 방지 락을 추가한다.
7. 수동 `service ETL only` API를 orchestration/admin 계층에 추가한다.
8. 차원 테이블 delta upsert를 `calendar` 모듈에 구현한다.
9. 차원 삭제 시 참조 정리 후 고아 차원 삭제를 구현한다.
10. affected `game_id` 수집 로직을 `calendar` 모듈에 구현한다.
11. affected `game_id` 기준 game 종속 projection replace를 구현한다.
12. Slice 5A에서 datasource/repository를 분리하고, cross-db query 제거와 `ingest` source 추출 / `service` 적용 단계 분리를 수행한다.
13. `service.game - ingest.game` 차집합 기반 루트 game 삭제를 구현한다.
14. affected scope set diff 검증 SQL을 구현한다.
15. mismatch 로그 저장과 자동 재시도를 구현한다.

## 8. 검증

- JDK 17+ 또는 프로젝트가 실제 사용하는 JDK 21 환경에서 테스트가 실행 가능해야 한다.
- `.java-version`과 로컬 JDK 설정이 불일치하면 `JAVA_HOME`을 명시해 테스트를 강제로 실행해 확인한다.
- `./gradlew test`
- `./gradlew bootRun`
- `curl -X POST http://localhost:8100/api/batch/sync`
- `curl -X POST http://localhost:8100/api/batch/service-sync`
- `service` 전용 실행 로그 확인
- `service` 원천 테이블별 커서 갱신 확인
- affected 범위 기준 expected vs actual set diff 결과가 0건인지 확인
- mismatch 유도 시 전체 롤백과 자동 재시도 1회 확인
- 수동 API 중복 호출 시 `409 Conflict` 확인
- `batch`가 `calendar`에 직접 의존하지 않고, `calendar`도 `batch`에 직접 의존하지 않는지 아키텍처 테스트로 확인
- `MAX_LOOP_GUARD` 초과 유도 시 `ingest`가 실패로 기록되고 downstream `service ETL`이 트리거되지 않는지 확인
- `MAX_LOOP_GUARD` 초과 유도 시 관련 커서가 전진하지 않는지 확인

## 9. 승인 기준

- `batch`는 `ingest`만 소유하고 `service` 스키마를 직접 쓰지 않는다.
- `calendar`는 `service ETL`과 `service` 스키마를 소유한다.
- `ingest -> service ETL` 자동 연계는 직접 모듈 의존이 아니라 상위 orchestration/event로 연결된다.
- `service ETL`이 `ingest`와 독립적으로 실행/실패/재시도/로그 관리된다.
- 권위 있는 `ingest` 성공 후에만 `service ETL`이 자동으로 이어진다.
- 페이지네이션 보호 로직(`MAX_LOOP_GUARD`) 초과는 `ingest` 실패로 취급되며 downstream을 트리거하지 않는다.
- 수동 `service ETL only` API가 비동기 실행된다.
- 차원 테이블은 delta 반영되고 삭제는 참조 정리 후 처리된다.
- game 종속 projection은 affected `game_id` 기준으로만 replace된다.
- Slice 5A 이후에는 `ingest`와 `service`를 같은 SQL에서 함께 참조하는 ETL query가 남아 있지 않다.
- Slice 5A 이후에는 하나의 repository/DAO가 두 DB를 함께 다루지 않는다.
- Slice 5A 이후 ETL은 두 DB에 걸친 단일 트랜잭션을 전제하지 않는다.
- `service.game`에만 존재하는 game은 루트 삭제된다.
- 검증은 affected scope set diff로 수행된다.
- mismatch 발생 시 전체 롤백되고 1회 자동 재시도된다.
- 성공 시에만 `service ETL` 커서가 전진한다.
- mismatch 상세 로그는 최대 100건만 저장된다.
