package com.projectgc.architecture

import com.tngtech.archunit.core.importer.ImportOption
import com.tngtech.archunit.junit.AnalyzeClasses
import com.tngtech.archunit.junit.ArchTest
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses

/**
 * 배치와 캘린더 모듈 사이의 의존 관계를 아키텍처 수준에서 차단합니다.
 */
@AnalyzeClasses(
    packages = ["com.projectgc"],
    importOptions = [ImportOption.DoNotIncludeTests::class]
)
class ModuleBoundariesTest {

    @ArchTest
    val batchDoesNotDependOnCalendar = noClasses()
        .that().resideInAPackage("com.projectgc.batch..")
        .should().dependOnClassesThat().resideInAPackage("com.projectgc.calendar..")
        .because("배치 계층은 웹 캘린더 모듈에 의존하지 않아야 합니다.")

    @ArchTest
    val batchDoesNotDependOnOrchestration = noClasses()
        .that().resideInAPackage("com.projectgc.batch..")
        .should().dependOnClassesThat().resideInAPackage("com.projectgc.orchestration..")
        .because("배치 계층은 orchestration 계층에 직접 의존하지 않아야 합니다.")

    @ArchTest
    val calendarDoesNotDependOnBatch = noClasses()
        .that().resideInAPackage("com.projectgc.calendar..")
        .should().dependOnClassesThat().resideInAPackage("com.projectgc.batch..")
        .because("캘린더 웹 계층은 배치 모듈에 의존하지 않아야 합니다.")
}
