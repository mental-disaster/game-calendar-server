# ProjectGC

ProjectGC는 Kotlin과 Spring Boot로 구축한 게임 출시 캘린더 웹 서비스입니다.
현재 단계의 목표는 이후 기능 확장과 마이크로서비스 전환을 염두에 둔 최소한의 안정적인 뼈대를 마련하는 것입니다.


## 개발 환경
- Java 21
- Kotlin 1.9
- Spring Boot 3.5.6
- Gradle 8


## 설정
`src/main/resources/application.properties`에서 주요 설정을 관리합니다.
- `spring.datasource.*`는 `projectgc`라는 인메모리 H2 데이터베이스를 가리킵니다. `MODE=PostgreSQL`로 설정해 향후 PostgreSQL 전환 시 호환성을 높였습니다.
- Flyway가 스키마를 생성하며, Hibernate는 `validate` 모드로 엔티티와 스키마 일관성만 확인합니다.
- H2 콘솔을 `/h2-console` 경로에서 사용할 수 있습니다.
- 저장소에는 `application.properties.sample`만 포함되어 있고, 실제 `application.properties` 파일은 Git에 커밋하지 않습니다. 처음 실행할 때는 `cp src/main/resources/application.properties.sample src/main/resources/application.properties`로 복사한 뒤 민감 정보를 채워 넣으세요.
- 별도 환경 구성이 필요하면 `application-<profile>.properties` 파일을 추가하고 `--spring.profiles.active=<profile>`로 실행하세요.


## 개발 준비
1. JDK 21을 설치합니다 (Temurin 권장).
2. 저장소를 클론한 뒤 IntelliJ IDEA 등 Kotlin/Spring 지원 IDE에서 Gradle 프로젝트로 임포트합니다.
3. `application.properties.sample`을 복사해 로컬 `application.properties`를 만든 뒤 필요한 DB 접속 정보를 수정합니다.
4. IDE가 Gradle 의존성을 모두 다운로드할 때까지 대기합니다.


## 주요 명령어
- 전체 빌드: `./gradlew clean build`
- 테스트만 실행: `./gradlew test`
- 애플리케이션 실행: `./gradlew bootRun`

`bootRun` 실행 후에는 다음과 같이 동작합니다.
- http://localhost:8080/h2-console 에서 H2 웹 콘솔을 사용할 수 있습니다.<br>H2 웹 콘솔 접속 오류 발생시 설정 확인 후 아래와 같이 설정해주세요.
  - JDBC URL: `jdbc:h2:mem:projectgc;MODE=PostgreSQL;DB_CLOSE_DELAY=-1`
  - 사용자명: `sa`
  - 비밀번호: 공백
- Flyway가 `db/migration` 폴더에 있는 마이그레이션을 자동으로 적용합니다.
- 인메모리 DB 특성상 애플리케이션을 재기동하면 데이터가 초기화됩니다.


## 문제 해결 가이드
- Gradle 경고가 나타나면 `./gradlew build --warning-mode=all`로 세부 정보를 확인하세요.
- IDE가 Gradle과 동일하게 JDK 21을 사용하도록 설정하세요.
- 영속 데이터가 필요하면 H2 대신 로컬 PostgreSQL을 설정하고 프로필을 분리하세요.
