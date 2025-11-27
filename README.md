# ProjectGC

ProjectGC는 Kotlin과 Spring Boot로 구축한 게임 출시 캘린더 웹 서비스입니다.
현재 단계의 목표는 이후 기능 확장과 마이크로서비스 전환을 염두에 둔 최소한의 안정적인 뼈대를 마련하는 것입니다.


## 개발 환경
- Java 21
- Kotlin 1.9
- Spring Boot 3.5.6
- Gradle 8
- PostgreSQL 17


## 디렉터리 구조
- `src/main/kotlin/com/projectgc/calendar`
  - 웹 API와 직접 연결된 도메인(컨트롤러, 서비스, 영속성 등)
- `src/main/kotlin/com/projectgc/batch` 
  - 배치/예약 작업용 컴포넌트(향후 분리 대비)
- `src/main/resources/db/migration` 
  - Flyway 마이그레이션 스크립트 보관 위치


## 개발 준비
1. JDK 21을 설치
3. `application.properties.sample`을 복사해 로컬 `application.properties`를 만든 뒤 필요한 정보를 수정
4. Gradle 의존성을 모두 다운로드


## 주요 명령어

- 전체 빌드: `./gradlew clean build`
- 테스트만 실행: `./gradlew test`
- 애플리케이션 실행: `./gradlew bootRun`


## API 스켈레톤
- `GET /api/releases/upcoming` — 출시 예정 목록 (서비스 로직 구현 예정)
- `GET /api/releases/recent` — 최근 출시 목록 (서비스 로직 구현 예정)

현재 두 엔드포인트는 서비스 계층에서 `NotImplementedError`를 던지도록 되어 있으므로 실제 데이터는 응답하지 않습니다.


## 문제 해결 가이드
- Gradle 경고가 나타나면 `./gradlew build --warning-mode=all`로 세부 정보를 확인하세요.
- IDE가 Gradle과 동일하게 JDK 21을 사용하도록 설정하세요.
