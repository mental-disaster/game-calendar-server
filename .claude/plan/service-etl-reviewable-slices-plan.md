# Service ETL 단계별 분할 계획

## 목적

`service ETL` 구현을 한 번에 밀어넣지 않고, 사람이 리뷰 가능한 크기의 단계로 분할한다.

분할 기준은 다음과 같다.

- 각 단계가 독립적으로 이해 가능해야 한다.
- 단계별 변경 목적이 명확해야 한다.
- 실패 원인과 회귀 범위를 좁게 유지해야 한다.
- 가능한 경우 각 단계가 자체 검증 가능한 상태여야 한다.

## 분할 원칙

- `batch=ingest`, `calendar=service` 소유권을 먼저 고정한다.
- `batch`가 `service` 스키마를 직접 다루는 방향으로 계획을 확장하지 않는다.
- `calendar`는 `batch` 코드에 의존하지 않고 `ingest` 스키마를 읽는 방식으로 ETL을 구현한다.
- 모듈 간 자동 연계는 상위 orchestration/admin 또는 공용 event 계층에서 처리한다.
- 스키마/실행 제어/적재 로직/검증 로직을 한 PR에 섞지 않는다.
- `affected game_id` 계산과 `game 단위 replace`와 `set diff 검증`은 서로 분리한다.
- 캘린더 핵심 read model과 부가 projection을 분리한다.
- 후반 단계일수록 위험도가 높아지므로, 앞 단계에서 관측성과 실행 골격을 먼저 만든다.

## Slice 1. 서비스 ETL 기반 골격

### 현재 상태

승인.

정리:

- 모듈 경계 복구 완료
- 권위 있는 `ingest` 성공 판정 고정 완료
- `MAX_LOOP_GUARD` 초과 시 실패 승격, 커서 미전진, downstream 미트리거 보장 완료
- Slice 1 필수 테스트 추가 완료
- JDK 21 명시 환경에서 관련 테스트 통과 확인 완료

### 후속 메모

- 로컬 기본 실행 환경의 JDK 재현성은 여전히 약할 수 있으므로, `.java-version`과 실제 설치 버전 정리는 별도 후속 작업으로 관리한다.
- `ServiceEtlCoordinator`의 `AtomicBoolean` 락은 Slice 1 기준으로 충분하지만, 다중 인스턴스 환경에서는 외부 락으로 전환을 검토한다.
- 비정상 종료 시 `running` 로그 정리 정책은 후속 실패 복구 단계에서 다룬다.
- `ServiceEtlService`의 run/source log 저장 경계 테스트는 실제 적재 로직이 들어가는 Slice 2 이후에 추가한다.

### 목표

`service ETL`의 실행 기반을 추가하되, 아래 3가지를 Slice 1에서 함께 고정한다.

- 모듈 경계 복구
- 권위 있는 `ingest 성공` 판정
- 필수 테스트 실행 가능 상태 확보

### 포함 범위

- `calendar` 모듈 소유의 `service` 전용 실행 로그 테이블
- `calendar` 모듈 소유의 `service` 원천 테이블별 처리 로그 테이블
- `calendar` 모듈 소유의 `service` mismatch 상세 로그 테이블
- `calendar` 모듈 소유의 `service` 원천 테이블별 커서 테이블
- `calendar` 모듈의 `service ETL` 서비스 골격
- 상위 orchestration/admin 계층 골격
- 동시 실행 방지 락
- `service ETL only` 비동기 API
- `ingest` 성공 후 `service ETL` 자동 연계 골격
- `batch -> orchestration` 직접 의존 제거
- 자동 연계 계약을 공용 계층 또는 orchestration 소유 방식으로 재배치
- "권위 있는 ingest 성공" 판정 로직 고정
- 부분 실패, 전체 실패, `finishSyncLog(..., "completed")` 실패 시 downstream 미트리거 보장
- `MAX_LOOP_GUARD` 초과를 단순 `break`가 아니라 실패로 승격
- 루프 가드 초과 시 커서 미전진, `completed` 미기록, downstream 미트리거 보장
- 완료 상태 확정 전 `completedAt` 또는 후속 트리거 조건을 세팅하지 않도록 정리
- Slice 1 관련 테스트 추가 및 실제 실행 가능 상태 복구
- Gradle 테스트가 JDK 17+에서 실제로 돌 수 있는 환경 전제 정리

### 제외 범위

- 실제 차원 테이블 적재
- affected `game_id` 계산
- game projection 재구성
- set diff 검증
- `service ETL` 내부 적재 SQL

### 필수 수정 항목

- `batch`가 `orchestration` 패키지를 직접 import/publish하지 않도록 수정
- `service ETL` 자동 연계는 "ingest가 실제로 성공했을 때만" 발생하도록 수정
- 개별 source 적재 실패를 성공으로 오인해 downstream을 트리거하지 않도록 수정
- `finishSyncLog(..., "completed")` 저장 실패 시 downstream을 트리거하지 않도록 수정
- `syncWithCursor`, `syncPaginated`, `syncMediaByGameIds`, `syncCompaniesByInvolvedCompanyIds`의 `MAX_LOOP_GUARD` 초과를 실패로 승격
- 루프 가드 초과가 `failedTables` 또는 동등한 실패 상태에 반영되도록 수정
- 루프 가드 초과 시 커서 전진과 성공 이벤트 발행이 차단되도록 수정
- 수동 API의 `202 Accepted` / `409 Conflict` 동작을 테스트로 고정
- 자동 연계 경로와 coordinator 호출을 테스트로 고정
- 루프 가드 초과 시 `failed + no downstream trigger` 테스트를 추가

### 리뷰 포인트

- 모듈 소유권이 `batch=ingest`, `calendar=service`로 유지되는지
- `batch`가 `calendar`나 `orchestration`에 직접 의존하지 않는지
- 로그/커서 스키마 명명 일관성
- `ingest`와 `service ETL` 성공/실패 분리 보장
- "권위 있는 ingest 성공"의 정의가 코드와 로그에서 일관되게 적용되는지
- 부분 실패, 전체 실패, 완료 로그 저장 실패에서 downstream 미트리거가 보장되는지
- `MAX_LOOP_GUARD` 초과가 조용히 성공으로 흡수되지 않는지
- 보호 로직에 의해 조기 종료된 경우에도 커서가 전진하지 않는지
- 비동기 실행과 `409 Conflict` 제어 방식
- 관련 테스트가 실제 JVM 환경에서 실행 가능한지

### 승인 기준

- `service ETL only`를 비동기로 호출할 수 있다.
- 중복 실행 시 `409 Conflict`가 발생한다.
- `ingest`의 권위 있는 성공 시에만 `service ETL`이 상위 orchestration을 통해 독립적으로 트리거된다.
- 부분 실패 시 `service ETL` 자동 연계가 발생하지 않는다.
- 전체 실패 시 `service ETL` 자동 연계가 발생하지 않는다.
- `finishSyncLog(..., "completed")` 저장 실패 시 `service ETL` 자동 연계가 발생하지 않는다.
- `MAX_LOOP_GUARD` 초과 시 `ingest`는 실패로 기록되고 `service ETL` 자동 연계가 발생하지 않는다.
- `MAX_LOOP_GUARD` 초과 시 관련 커서가 전진하지 않는다.
- 전용 로그/커서 테이블이 생성된다.
- `GameReleaseBatchService` 성공/부분실패/전체실패/finish log 실패 경계 테스트가 존재한다.
- `GameReleaseBatchService`의 루프 가드 초과 경계 테스트가 존재한다.
- `ServiceEtlAdminController`의 `202 Accepted` / `409 Conflict` 테스트가 존재한다.
- 자동 연계 수신 계층의 coordinator 호출 테스트가 존재한다.
- Slice 1 관련 테스트가 JDK 17+ 환경에서 실제 실행된다.

## Slice 2. 차원 테이블 증분 ETL

### 현재 상태

승인.

정리:

- `calendar`가 `batch` 코드에 의존하지 않고 `ingest` 스키마를 직접 읽는 구조가 유지되었다.
- 17개 차원 테이블의 증분 ETL이 연결되었다.
- source table 특성에 따라 커서 기반 전략과 diff 기반 전략이 분리되었다.
- `platform_logo`는 diff 기반 병합으로 처리된다.
- `company` 자기참조 FK와 source table별 증분 전략 관련 blocking 이슈는 최신 수정본에서 해소된 것으로 리뷰 확인되었다.
- Slice 2 관련 테스트가 실제 JDK 21 환경에서 통과했다는 리뷰가 있다.

### 후속 메모

- 초기 동기화 시 차원 테이블 전체를 한 번에 메모리로 읽는 방식은 현재 규모에선 허용되지만, 데이터가 커지면 chunk/streaming 방식으로 바꾸는 것을 검토한다.
- 차원 테이블 null 허용 마이그레이션의 장기 계약은 후속 consumer 관점에서 다시 점검할 수 있다.

### 목표

1:1 성격이 강한 차원 테이블의 delta 반영을 먼저 완성한다.

### 포함 범위

- `game_status`
- `game_type`
- `language`
- `region`
- `release_region`
- `release_status`
- `genre`
- `theme`
- `player_perspective`
- `game_mode`
- `keyword`
- `language_support_type`
- `website_type`
- `platform_logo`
- `platform_type`
- `platform`
- `company`

### 제외 범위

- 차원 삭제
- affected `game_id` 계산
- game 종속 projection

### 리뷰 포인트

- `calendar`가 `batch` 코드에 의존하지 않는지
- `ingest -> service` 컬럼 매핑 정확성
- upsert SQL의 idempotency
- 커서 전진 기준
- source table별 증분 전략 선택이 원천 적재 방식과 맞는지
- `company` 자기참조 FK 반영이 안전한지

### 승인 기준

- 차원 테이블 create/update가 delta 기준으로 반영된다.
- 재실행 시 중복 없이 같은 상태를 유지한다.
- source table별 전략이 원천 ingest 적재 방식과 충돌하지 않는다.
- `company` 자기참조 FK가 런타임 실패 없이 반영된다.
- `updated_at`이 null이거나 cursor보다 오래된 late-arriving row도 영구 누락 없이 반영된다.

## Slice 3. affected `game_id` 계산기

### 현재 상태

승인.

정리:

- `calendar` 내부에서만 source 계산이 닫혀 있는 구조는 유지되었다.
- 11개 source table 모두에 대해 affected `game_id` 계산 경로가 구현되었다.
- 초기 실행 시 전체 game sweep과 source table별 delta union이 구현되었다.
- Slice 3는 projection materialization 도입 전까지 dry-run으로 전환되어 cursor를 전진시키지 않는다.
- 따라서 Slice 4 도입 전 변경분이 영구 소진되지 않도록 방어되었다.
- 관련 단위 테스트와 서비스 레벨 테스트가 JDK 21 환경에서 통과했다는 리뷰가 있다.

### 후속 메모

- 미디어/부가 테이블이 `ingest.game.updated_at`을 대리 커서로 사용하는 도메인 계약은 타당하지만, calculator 코드나 계획 문서에 짧은 주석으로 남겨두면 유지보수성이 더 좋아진다.
- 초기 전체 sweep 시 모든 game id를 메모리에 Set으로 적재하는 방식은 현 규모에서 허용 가능하더라도, 이후 Slice 4~6에서 chunk 처리 전략이 필요할 수 있다.
- Slice 3 계산 자체의 JDBC 동작은 향후 repository 통합 테스트로 더 강하게 고정할 수 있다.
- 이전 실험 버전이나 수동 조작으로 이미 Slice 3 cursor가 전진한 환경이 있다면, 운영 절차로 초기화/무시 기준을 확인하는 것이 안전하다.
- Slice 4가 도입된 이후에는 core projection이 실제로 materialize된 뒤에만 관련 cursor가 전진해야 한다.

### 목표

어떤 원천 변경이 game 재구성을 유발하는지 계산하는 로직을 독립적으로 구현한다.

### 포함 범위

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

### 제외 범위

- 실제 `service` 테이블 replace
- 루트 game 삭제
- set diff 검증

### 리뷰 포인트

- `calendar` 내부에서만 ETL source 계산이 닫히는지
- 누락 없는 영향 범위 계산
- 초기 실행/커서 없음 시 전체 game 선택 처리
- source table 별 커서 사용 일관성
- 계산된 affected 집합이 projection 재구성 전에 유실되지 않는지
- cursor 전진 시점이 downstream 소비 성공과 정합적인지

### 승인 기준

- source table 변경에 따라 affected `game_id` 집합을 계산할 수 있다.
- 초기 실행에서는 전체 game을 반환한다.
- projection materialization 또는 동등한 durable handoff 없이 Slice 3 cursor가 먼저 전진하지 않는다.
- Slice 4 도입 전에도 Slice 3 변경분이 영구 소진되지 않는다.

## Slice 4. 핵심 game projection 재구성

### 현재 상태

승인.

정리:

- `service.game`, `service.game_release`, `service.game_localization`의 핵심 projection 재구성이 구현되었다.
- `calendar` 모듈 내부에서만 재구성 책임이 유지되었다.
- FK 무결성을 지키는 replace 순서와 누락 참조 null 처리 로직이 정리되었다.
- 변경된 `affectedGameIds` 범위만 대상으로 부분 재구성이 수행된다.
- 핵심 source는 `updated_at` cursor가 아니라 projection diff 기반으로 전환되었다.
- 아직 materialize하지 않는 source는 deferred dry-run으로 남기고 cursor를 전진시키지 않도록 정리되었다.
- 관련 테스트와 repository support 테스트가 JDK 21 환경에서 통과했다는 리뷰가 있다.

### 후속 메모

- 대규모 full sweep 시 chunk 단위 재구성이 단일 트랜잭션 시간을 길게 만들 수 있으므로 메트릭을 보며 chunk/트랜잭션 전략을 조정할 수 있다.
- `service.game.slug`의 UNIQUE 제약과 원천 데이터 품질 문제는 운영 중 예외 메시지와 함께 관찰할 필요가 있다.
- `service.game.updated_at = now()` 정책은 하류 consumer가 생기면 false positive 변경 신호가 될 수 있으므로 이후 사용처가 생기면 다시 검토한다.
- 핵심 projection rebuild SQL은 raw JDBC 비중이 높으므로 후속 단계에서 repository 통합 테스트로 더 강하게 고정할 수 있다.
- 핵심 projection diff 계산과 rebuild SQL은 JDBC/Flyway 통합 테스트로 추가 보강하는 편이 안전하다.
- core projection 컬럼 null 허용 마이그레이션이 장기 계약인지 Slice 4 이행용 완충인지 문서에 남겨 둘 필요가 있다.

### 목표

캘린더 핵심 read model을 먼저 완성한다.

### 포함 범위

- `service.game`
- `service.game_release`
- `service.game_localization`
- affected `game_id` 기준 replace

### 제외 범위

- 나머지 bridge/미디어/부가 projection
- 검증 mismatch 처리
- 자동 재시도

### 리뷰 포인트

- `service` 재구성 책임이 `calendar` 모듈 안에 머무는지
- replace 순서와 FK 안정성
- 루트 `game` row와 `game_release`/`game_localization` 관계 정합성
- changed game만 부분 재구성되는지
- 핵심 source가 projection diff 기반으로 계산되는지
- 아직 소비하지 않는 source가 deferred dry-run 상태로 유지되는지

### 승인 기준

- affected `game_id` 범위에 대해 3개 핵심 테이블이 재구성된다.
- 기존에 영향 없는 game은 건드리지 않는다.
- projection rebuild 실패 시 deferred source를 포함한 Slice 3 cursor가 전진하지 않는다.
- 핵심 source는 projection diff 기반으로 계산되어 cursor 조기 소진 문제가 없다.
- Slice 4가 아직 소비하지 않는 source는 deferred dry-run으로 남고 cursor를 전진시키지 않는다.

## Slice 5. game 종속 bridge projection 재구성

### 목표

관계/집계 성격의 projection을 추가한다.

### 포함 범위

- `service.game_language`
- `service.game_genre`
- `service.game_theme`
- `service.game_player_perspective`
- `service.game_game_mode`
- `service.game_keyword`
- `service.game_company`
- `service.game_relation`

### 제외 범위

- 미디어/부가 테이블
- 검증 mismatch 처리

### 리뷰 포인트

- batch 쪽 SQL/서비스로 책임이 새지 않는지
- 배열/관계 기반 source에서 stale row 없이 replace되는지
- 집계 플래그 계산 정확성
- 역할/관계 타입 매핑 정확성

### 승인 기준

- bridge/집계 projection이 affected `game_id` 기준으로 정확히 replace된다.

## Slice 6. 미디어/부가 projection 재구성

### 목표

게임 부가 데이터 projection을 추가한다.

### 포함 범위

- `service.cover`
- `service.artwork`
- `service.screenshot`
- `service.game_video`
- `service.website`
- `service.alternative_name`

### 제외 범위

- 검증 mismatch 처리
- 자동 재시도

### 리뷰 포인트

- 미디어/부가 projection 책임이 `calendar`에 머무는지
- `cover.is_main` 같은 교차 의존 판단
- 미디어 삭제 시 stale row 제거
- game 단위 replace 범위 적절성

### 승인 기준

- 미디어/부가 projection이 affected `game_id` 기준으로 정확히 replace된다.

## Slice 7. 삭제, 검증, 재시도 마감

### 목표

정합성 보증과 실패 복구를 마무리한다.

### 포함 범위

- `service.game - ingest.game` 차집합 기반 루트 game 삭제
- 공용 차원 삭제 시 참조 정리 후 고아 삭제
- affected scope set diff 검증
- mismatch 총건수 기록
- mismatch 상세 100건 샘플 저장
- 검증 실패 포함 1회 자동 재시도
- 실패 시 커서 미전진 처리

### 리뷰 포인트

- 자동 연계와 재시도가 모듈 경계를 깨지 않는지
- 검증 SQL이 false positive/false negative 없이 설계됐는지
- 삭제 순서가 FK와 충돌하지 않는지
- 재시도 시 동일 실패를 과도하게 반복하지 않는지

### 승인 기준

- mismatch가 1건이라도 있으면 전체 롤백된다.
- 자동 재시도는 1회만 수행된다.
- 재시도 후에도 실패하면 최종 실패와 상세 로그가 남는다.
- 성공 시에만 커서가 전진한다.

## 권장 구현 순서

1. Slice 1
2. Slice 2
3. Slice 3
4. Slice 4
5. Slice 5
6. Slice 6
7. Slice 7

## 권장 리뷰 전략

- Slice 1-2는 인프라/기초 SQL 리뷰
- Slice 3은 영향 범위 계산 로직 리뷰
- Slice 4는 핵심 read model 리뷰
- Slice 5-6은 파생 projection 리뷰
- Slice 7은 운영 안정성과 정합성 보증 리뷰

## 최종 메모

특히 아래 3가지는 한 PR에 같이 넣지 않는 것이 좋다.

- affected `game_id` 계산
- game 단위 replace SQL
- affected scope set diff 검증

이 셋을 분리해야 리뷰어가 “무엇이 잘못됐는지”를 추적할 수 있다.

또한 아래 두 가지도 한 PR에 섞지 않는 것이 좋다.

- 모듈 경계 재배치
- 대량 SQL projection 구현

소유권 정리와 ETL SQL 구현을 분리해야 리뷰가 가능하다.
