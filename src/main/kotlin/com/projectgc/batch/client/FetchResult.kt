package com.projectgc.batch.client

data class ParseError(
    val recordId: Long?,
    val rawJson: String,
    val errorMsg: String,
)

data class FetchResult<T>(
    val items: List<T>,
    val errors: List<ParseError>,
) {
    // API가 반환한 전체 레코드 수 (파싱 성공 + 실패)
    // 페이지 끝 여부 판단에 items.size 대신 fetched를 사용해야
    // 파싱 실패로 items가 PAGE_SIZE 미만이 되어도 페이지네이션이 중단되지 않음
    val fetched: Int get() = items.size + errors.size
}
