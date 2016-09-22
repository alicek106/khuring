# Monitor (Edited by nypark 2016. 5. 24)

broker를 통해 전달받은 client의 정보를 관리하거나 topic request에 따라 response 전달.

### class description
  - creativedesign.namu.monitor : 주요 기능
    1. NamuNodeMonitor.java : mqtt client paho library 사용. 전달받은 메시지에 따라 기능을 수행한다.
    2. MysqlAccessor.java : broker의 요청에 따라 데이터베이스에 접근하여 정보 입력 혹은 전달
  - creativedesign.namu.assistance : 부가 기능
    1. AssistClass.java : timeStamp 등 간단한 함수들을 모아놓음
    2. Main.java : monitor Main 함수
    3. PrintConsole.java : 콘솔창에 깔끔하게 출력하는 클래스

### 사용법
  - 소스코드를 다운받은 후 Main.java 실행

### 진행중
  - PrintConsole.java 일부 메시지 출력 안함 - 수정 중
  - 그룹 생성 완료 기능 - 수정 중
  
### Version
0.1.0

License
----

nypark, Kyung-hee univ. ICNS lab.
