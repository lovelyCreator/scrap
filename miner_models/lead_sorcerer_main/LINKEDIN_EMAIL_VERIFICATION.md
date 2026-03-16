# 링크드인 검증 + 이메일 검증 가이드 (ScrapingDog 대체)

발리데이터가 쓰는 방식을 **역이용**해서:
1. **링크드인**: 검색 결과로 URL 검증 + 스니펫에서 이름/회사 등 추출 → 마이너 리드 품질 향상
2. **이메일**: SMTP 검증 + 여러 템플릿으로 실존 이메일만 수집

이렇게 하면 이메일·링크드인 문제를 마이너 단에서 줄일 수 있다.

---

## ScrapingDog 대신 뭘 쓸까?

| 용도 | ScrapingDog | 대체 옵션 (권장) | 비고 |
|------|-------------|------------------|------|
| **Google 검색** (링크드인 URL 검증, Q4/Q1 쿼리) | `api.scrapingdog.com/google` | **Serper** (`google.serper.dev`) | 이미 마이너에 `SERPER_API_KEY` 사용 중. 동일 쿼리로 `organic` 결과의 `title`/`snippet`/`link` 파싱하면 됨. |
| **링크드인 프로필 전체 스크래핑** (이름/헤드라인/경력 등 JSON) | `api.scrapingdog.com/linkedin` | **Proxycurl** / **Serper 스니펫만** | 프로필 전체가 필요하면 Proxycurl 등 유료 API. 검증만 하면 Serper 스니펫으로 충분. |
| **일반 웹 스크래핑** | `api.scrapingdog.com/scrape` | **httpx + BeautifulSoup** | 회사 사이트 등은 이미 마이너에서 사용 중. |

**결론: Google 검색 기반 링크드인 검증은 ScrapingDog 대신 Serper만 쓰면 된다.** (추가 비용 없이 기존 Serper로 validator와 같은 방식 적용 가능)

---

## 1. 링크드인 검증 (Serper로 validator 방식 역이용)

발리데이터는 다음만 사용한다:
- **Q4**: `"{full_name}" "{company}" linkedin location`  
- **Q1 (폴백)**: `site:linkedin.com/in/{slug}`  

검색 결과의 `title`/`snippet`/`link`에서:
- 링크가 우리가 찾은 개인 LinkedIn URL과 같은지 확인 (slug 매칭)
- 매칭된 결과의 title/snippet에서 이름·회사 일치 여부 확인

**Serper 응답:** `organic` 배열, 각 항목 `title`, `link`, `snippet` (ScrapingDog의 `organic_results`와 동일 구조).

마이너에 추가된 함수:
- `get_linkedin_slug(url)` — URL에서 `/in/xxx` 슬러그 추출
- `verify_linkedin_with_serper(client, full_name, company, linkedin_url)` — Q4 → 필요 시 Q1 호출, URL 매칭 및 성공 시 매칭된 결과(스니펫) 반환

리드 생성 후 `find_personal_linkedin`으로 URL을 얻었다면, `verify_linkedin_with_serper`로 한 번 더 검증하고, 반환된 스니펫으로 full_name/company를 보강할 수 있다.

---

## 2. 이메일 검증 (SMTP + 여러 템플릿)

- **SMTP 검증**: 생성한 이메일 주소에 대해 메일 서버에 연결 후 `RCPT TO` 단계까지 수행해 메일박스 존재 여부 확인. (실제 메일 발송 없이 가능)
- **여러 템플릿**: `first.last@`, `firstlast@`, `flast@` 등 여러 패턴으로 후보를 만들고, SMTP(또는 API)로 검증해 **실제 존재하는 주소만** 리드에 넣으면 발리데이터 스테이지 통과율이 올라간다.

구현 옵션:
- **Python `smtplib`**: 직접 MX 조회 후 SMTP 연결, `RCPT TO` 만 검사 (무료, 레이트 리밋·방화벽 주의).
- **이메일 검증 API**: TrueList(발리데이터가 사용), NeverBounce, ZeroBounce, Hunter 등 — 유료지만 안정적.

마이너에서는 먼저 2~3개 패턴으로 후보를 만들고, SMTP 또는 소량 API로 검증해 실존 주소만 채우는 흐름을 추천한다.

---

## 3. 환경 변수 요약

| 변수 | 용도 |
|------|------|
| `SERPER_API_KEY` | Serper 검색 (링크드인 검증 포함) — ScrapingDog Google 대체 |
| (선택) SMTP 검증용 MX/연결 설정 | 직접 SMTP 검증 시 |
| (선택) 이메일 검증 API 키 | TrueList 등 외부 검증 사용 시 |

ScrapingDog 키 없이도 Serper만으로 링크드인 검증까지 처리 가능하다.
