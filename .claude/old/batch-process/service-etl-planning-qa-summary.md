# Service ETL 계획 수립 문답 요약

## 목적

이 문서는 Planner와 사용자 사이에서 `service ETL` 계획을 확정하기 위해 주고받은 주요 문답과 최종 결론을 정리한 기록이다.

## 문답 요약

1. 질문: 이번 작업 범위는 어디까지인가?
답변: `service` 스키마 전체를 만드는 작업으로 간다.

2. 질문: `full refresh`와 `incremental` 중 어떤 방식으로 갈까?
답변: `incremental`로 진행하되, 완료 후 정합성 검증을 통해 신뢰성을 확보한다.

3. 질문: 삭제까지 포함한 `incremental`의 제약은 무엇인가?
답변: 현재 구조에서는 IGDB 원천 삭제를 엄밀히 검증하기 어렵고, 수집기를 수정하지 않으면 tombstone 없이 원천 삭제를 보장할 수 없다.

4. 질문: 삭제 기준은 무엇으로 삼을까?
답변: `ingest`를 truth source로 두고, `ingest`에서 사라진 데이터만 `service`에서 삭제 동기화한다.

5. 질문: 실행 시점은 어떻게 가져갈까?
답변: 기본은 `ingest` 성공 직후 자동 실행으로 두되, `service ETL only`도 수동으로 실행 가능해야 한다.

6. 질문: `service ETL` 실패가 `ingest` 적재 실패를 유발해도 되는가?
답변: 안 된다. 두 단계는 성공/실패 판정을 분리한다.

7. 질문: 수동 실행은 어떤 형태가 필요한가?
답변: 기존 `/api/batch/sync`와 유사한 `service ETL only` API가 필요하다.

8. 질문: 트랜잭션 단위는 어떻게 할까?
답변: `service ETL` 전체를 단일 트랜잭션으로 처리한다.

9. 질문: 커서와 로그는 `ingest`와 공유할까?
답변: 아니다. 별도 실행 로그와 별도 커서를 가진다.

10. 질문: 증분 처리 기본 단위는 무엇으로 잡을까?
답변: 하이브리드 방식으로 간다.

- 차원 테이블은 테이블별 delta 반영
- game 종속 projection은 affected `game_id` 기준 재구성

11. 질문: 모든 테이블을 그냥 각자 변경분만 반영하면 되지 않는가?
답변: 차원 테이블은 가능하지만, bridge/집계/파생 테이블은 stale row 제거를 위해 game 단위 replace가 필요하다.

12. 질문: affected `game_id` 계산 범위는?
답변: 넓게 잡는다. 게임에 직접 연결된 원천 테이블 전체를 포함한다.

포함 대상:

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

13. 질문: game 종속 projection 재구성 방식은?
답변: game 단위 replace로 처리한다.

14. 질문: game 단위 replace는 전체 재구성과 같은가?
답변: 아니다. `service` 전체를 비우는 것이 아니라, 변경된 `game_id`에 해당하는 row만 지우고 다시 생성한다.

15. 질문: game 종속 projection 전체를 replace 대상으로 볼까?
답변: 그렇다. 아래 테이블 전체를 game 단위 replace 대상으로 확정했다.

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

16. 질문: 공용 차원 테이블도 삭제를 반영할까?
답변: 반영한다.

17. 질문: 공용 차원 삭제는 어떻게 처리할까?
답변: 관련 game 참조를 먼저 정리한 뒤, 고아 차원만 삭제한다.

18. 질문: 차원 수정/추가는 game 재구성까지 해야 하는가?
답변: 아니다. 차원 테이블만 갱신한다.

19. 질문: 초기 실행이나 커서가 없을 때는?
답변: 예외적으로 `ingest` 전체 기준 `service` 전체 1회 구축으로 간다.

20. 질문: 검증 기준은 총 건수 비교로 충분한가?
답변: 아니다. 총 건수 비교보다 `affected scope set diff`가 더 정확하므로 이를 주 검증 기준으로 한다.

21. 질문: 검증 실패 시 어떻게 처리할까?
답변: mismatch가 1건이라도 있으면 전체 롤백하고 ETL을 실패 처리한다.

22. 질문: 실패 시 방어 전략은?
답변: 단순 롤백으로 끝내지 않고, 새 트랜잭션에서 ETL 전체를 1회 자동 재시도한다.

23. 질문: 검증 실패도 자동 재시도 대상에 포함할까?
답변: 포함한다. 1회 허용한다.

24. 질문: 수동 API는 동기/비동기 중 무엇인가?
답변: 기존 ingest와 동일하게 비동기 백그라운드 실행으로 간다.

25. 질문: 수동 API 중복 실행 허용 여부는?
답변: 동시에 1개만 허용한다. 실행 중이면 `409 Conflict`.

26. 질문: `service ETL` 로그는 어떻게 구성할까?
답변: `ingest` 로그와 분리된 전용 로그 테이블을 사용한다.

27. 질문: 로그 세분화 수준은?
답변: 아래 3종으로 분리한다.

- 실행 1건 단위 로그
- 원천 테이블별 처리 로그
- 검증 mismatch 상세 로그

28. 질문: mismatch 상세 로그는 전량 저장할까?
답변: 아니다. 샘플 상한을 둔다.

29. 질문: 샘플 상한은 몇 건인가?
답변: 100건으로 확정했다.

30. 질문: 수동 API 경로는 어떻게 할까?
답변: `/api/batch` 하위에 적절한 이름으로 구현한다. 권장 예시는 `/api/batch/service-sync`다.

## 최종 합의 요약

- `service` 전체 스키마를 대상으로 하는 `incremental ETL`
- truth source는 `ingest`
- 삭제 기준도 `ingest`
- 차원 테이블은 delta 반영
- game 종속 projection은 affected `game_id` 기준 replace
- 초기 실행은 1회 전체 구축
- `service ETL`은 전체 단일 트랜잭션
- `ingest`와 `service ETL`은 상태/로그/커서 완전 분리
- `service ETL only` 비동기 수동 API 제공
- 동시 실행은 1개만 허용
- 검증은 `affected scope set diff`
- mismatch 발생 시 롤백 후 1회 자동 재시도
- mismatch 상세 로그는 최대 100건 저장
