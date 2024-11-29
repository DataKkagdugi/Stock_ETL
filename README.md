# Stock_ETL

# 📌 프로젝트 개요

## 프로젝트 주제

<aside>
💡

소비자에게 다양한 미국 주식의 정보를 효과적으로 제공

</aside>

## 프로젝트 목적

<aside>
📈

국내 주식 보다 변동성이 큰 미국 주식의 지표를 시각적으로 표현하고 투자자들에게 도움이 되는 정보를 추출하여 제공한다.

</aside>

- 목적에 맞는 데이터를 추출해 클라우드 서비스에 적재해본다.
- Docker 기반 작업으로 Container에 대해 이해한다.
- Airflow를 사용해 데이터 파이프라인 설계에 대해 이해하고, 직접 DAG 파일을 작성하여 ETL 프로세스를 자동화하는 작업을 진행해본다.
- 대시보드를 통해 데이터를 활용할 줄 알고, 주어진 데이터에 맞게 효율적으로 시각화 해보도록 한다.

---

# 📌 프로젝트 구성

## 기술 스택

| Language | Python, SQL |
| --- | --- |
| DB | Amazon Redshift |
| Visualization | Preset |
| Data pipeline | Apache Airflow |
| Development tool | Github, Docker |
| Collaboration tool | Slack, Zep, Notion |

## 활용 데이터

- 한국투자증권 API
    
    https://apiportal.koreainvestment.com/intro
    
- 한국수출입은행 API
    
    https://www.koreaexim.go.kr/ir/HPHKIR020M01?apino=2&viewtype=O#tab1
    
- Yahoo finence
    
    https://github.com/ranaroussi/yfinance
    
- CNN 웹 크롤링 with Selenium (fear and greed index)
    
    https://edition.cnn.com/markets/fear-and-greed 
    

## SW 아키텍처

![image.png](https://prod-files-secure.s3.us-west-2.amazonaws.com/29dd0948-7b38-443b-af5a-c6bc908226ab/d6b5a996-8092-4bee-89c6-a93a2e0e9886/63c239a7-f246-4eb2-9086-1bc514f97153.png)

## 주요 기능

- **Fear and Greed Index**
    - CNN비즈니스 홈페이지에서 공개하는 공포와 탐욕 지수는 주식시장의 7가지 지표를 종합해서 0에서 100까지 점수를 계산해 점수가 0에 가까울수록 공포, 100에 가까울수록 탐욕적인 시장을 뜻하는 지표입니다. **[출처](https://www.frism.io/jusig-sijang-bunwigie-iggeulriji-anhgo-tujahaneun-bangbeob-gongpo-tamyog-jisuran/)**
    - 최근 한 달 치 데이터로 Fear and Greed Index 추이 확인
    - 최근 한 달 간 rating 비율 확인
- **환욜 등락 지표**
    - 미국 주식은 환율과 상관 관계가 깊으므로 환율 추이 분석
- **기간 내 테마별 주식 거래량**
    - 최근 뜨고 있는 주식의 테마가 무엇인지 제공
    - 일주일 간 테마별 거래량을 가져와 분석
- **지표를 통한 매수/매도 시점 파악**
    - RSI 지표를 통한 매수 매도 시점 파악
    - RSI란 주식의 가격 움직임 속도를 측정하여 과매수(Overbought)와 과매도(Oversold) 상태 식별하는 데 사용하는 지표
    - RSI값을 통해 status를 정의하고, status를 분석하여 매수해야 할지, 매도해야 할지 분단위 데이터 수집을 통해 파악

---

# 📌프로젝트 상세

## [전반적인 Process]

### ☑️ **Docker와 Airflow 설정**

Docker 설치 및 Airflow 이미지 pull / Container 실행 후 Airflow 웹 UI 접속 확인

```python
$ docker compose -f docker-compose.yaml pull # 다운로드
$ docker compose -f docker-compose.yaml up   # 다운로드 받은 docker 이미지를 실행
```

### ☑️ AWS 환경 설정

1. **AWS 리소스 생성**
    - S3 버킷 생성
        - 이름, region, 접근 권한 설정
    - Redshift Serverless 생성
        - 작업그룹, 네임스페이스 설정
        - VPC 및 서브넷 구성
        - 보안 그룹 설정
2. **S3와 Redshift Serverless 연동**
    - VPC 및 서브넷 구성 및 보안 그룹 설정
3. **Redshift 스키마 생성**
    - `raw_data` : 정제되지 않은 데이터 테이블
    - `analytics` : 필요한 정보를 정제한 데이터 테이블
4. **외부 접근을 위한 IAM 설정**
    - Redshift 접근용 IAM 사용자 생성
        
        팀원들이 정상적으로 접근, 사용할 수 있도록 적절한 권한 정책 연결
        

### ☑️ Airflow 연동 및 DAG 작성

1. **Airflow Connection 설정**
    
    *Airflow 웹 UI Admin > Connections*
    
    - AWS, Redshift 연결 정보 추가
2. **DAG 파일 작성**
    - 기능 별 새 Python 파일 생성
3. **각 Task 정의 및 의존성 설정**
    
    <aside>
    
    - **API 데이터 추출 및 S3 적재**
        - 데이터 추출, S3 업로드
        - `PythonOperator`  사용
    - **S3에서 Redshift 데이터 `COPY`**
        - `raw_data` 에  `CTAS` 쿼리 작성
        - `RedshiftDataOperator` 사용
    - **데이터 변환 및 `analytics`  스키마 적재**
        - `raw_data` → `analytics` 데이터 변환 쿼리 작성
        - `RedshiftDataOperator` 사용
    
    ✅ 이후 의존성을 설정해준다.
    
    </aside>
    
4. **DAG 테스트 및 활성화**

### ☑️ DAG 보완

1. **모니터링 및 로깅 설정**
2. **오류 처리 및 재시도 로직 구현**
    - Task 실패 시 재시도 설정 / 알림 설정

## [전체 결과물 보기]

![image.png](https://prod-files-secure.s3.us-west-2.amazonaws.com/29dd0948-7b38-443b-af5a-c6bc908226ab/ba423a06-b6e9-46cb-aa1c-fee979668af1/image.png)


https://9d7ac61c.us2a.app.preset.io/superset/dashboard/p/1MrwGm6yg95/

https://github.com/DataKkagdugi/Stock_ETL

---

# 📌 마무리

## 기대 효과

- 최근 급등한 테마의 종목은 무엇인지, 전체적으로 거래량이 상승한 종목이 무엇인지 제공함으로써 미국 주식 투자자들에게 도움이 될 수 있도록 한다.
- 환율 지표를 통해서 고액의 투자자들은 어느 시점에 환전을 하는 것이 지 정보를 제공한다.
- 여러 정보를 하나의 대시보드에 표현하여 투자자들이 정보를 찾는데 드는 시간을 감소시키고 한 눈에 투자 결정을 위한 주요 정보 파악이 가능하도록 한다.

## 한계점

- 거래량 관련 지표를 완성 시키지 못한 아쉬움
- 주식 종목과 관련된 데이터들의 크기가 매우 커서 전부 불러오지 못해 데이터의 정확도나 속도에 한계가 있을 수 있다.
- 사용된 데이터 API나 소스가 제한적이기 때문에, 더 다양한 투자 지표나 시장 데이터를 포함하지 못할 수 있다.
- 고액 투자자들을 위한 환율 정보 제공 시, 특정 시점의 환율 변동성을 완전히 반영하지 못할 가능성

## 역할 분담

| 이름 | 역할 |
| --- | --- |
| 김경준 | DAG 작성,  |
| 김정희 | DAG 작성,  |
| 김효정 | DAG 작성, Preset 시각화, Preset 데이터베이스 연결 |
| 배동제 | AWS 환경설정, DAG 작성, Preset 차트 생성 |
| 이상훈 | DAG 작성,  |
| 전혜림 | DAG 작성, 보고서 작성 |

## 프로젝트 회고

[프로젝트 회고](https://www.notion.so/bb3d6a51108d440eb17d936e4352b403?pvs=21)
