# AFours ERP 회계 시스템

## 실행 방식
- Streamlit 앱: 기존 `app.py`
- HTML + 서버 앱(신규): `server.py` (Flask)

## HTML 서버 기능
- 엑셀 업로드(`.xlsx`, `.xls`)
- 전표 자동 생성 및 분개 반영
- 대시보드(월별 매출/매입/손익)
- 전표 원장, 분개장
- 시산표 / 손익계산
- 월마감 / 마감해제
- SQLite 데이터 저장(`accounting.db`)

## 설치
```bash
cd /Users/lastorder/Desktop/afours/afours
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 실행
### 1) HTML + 서버(추천)
```bash
python server.py
```
브라우저: `http://127.0.0.1:5000`

### 2) Streamlit
```bash
streamlit run app.py
```

## Render 배포
저장소 루트에 `render.yaml`이 포함되어 있어 Blueprint 배포가 가능합니다.

1. GitHub 저장소를 Render에 연결
2. `New +` -> `Blueprint` 선택
3. 저장소 선택 후 생성 (`render.yaml` 자동 인식)
4. 배포 완료 후 제공 URL 접속

현재 설정:
- 서비스 타입: Web Service
- 앱 경로: `afours/`
- 시작 명령: `gunicorn server:app`

주의:
- 기본 SQLite(`accounting.db`)는 인스턴스 재배포 시 데이터 유실될 수 있습니다.
- 운영 환경은 Render Disk 또는 외부 DB(Postgres) 사용을 권장합니다.

## 엑셀 컬럼
필수:
- 날짜
- 구분(매입/매출)
- 공급가액

선택:
- 부가세 (없으면 10% 자동 계산)
- 거래처
- 적요
