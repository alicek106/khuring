# Edited by alicek106 2016. 5. 13

1. 클라이언트, 모니터 (임시) 이클립스로 가져오기
eclipse - import - Existing project 어쩌고저쩌고.. - client&monitor 안의 paho 선택

2. 브로커 이클립스로 가져오기
eclipse - import - Existing... - broker 안의 MqttBroker 선택

MqttBroker는 라이브러리를 설정해줘야 하는데.. 자동으로 라이브러리가 잡히지가 않아서 적어놓음.
 
Project Explorer - MqttBroker 프로젝트 오른쪽 클릭 - properties - Java build path - Library - BrokerLibrary 더블클릭 - User Libraries 클릭 - new 클릭 - BrokerLibrary 입력해서 추가 - BrokerLibrary 누르고 Add JARS - MqttBroker 프로젝트 내의 libs 폴더에 있는 jar들 모두 선택 - OK 클릭해서 빠져나옴 - Finish 클릭 - OK 해서 끝~


# 2016 05 26 추가

브로커 import 뒤 testembedded 안의 EmbeddedLauncher.java 가 main 함수. 실행시키면 바로 동작.

모니터는 나연이거 import 해서 연결하면 되는데, 모니터 내에서 연결하는 브로커 IP를 유동적으로 바꿔줘야함.

테스트 해보기 위해서는, 일단 1. 브로커 실행 -> 2. 모니터 실행해서 브로커에 연결 뒤 -> 3. alicek106폴더의 client&monitor 안의 프로젝트 import 해서
4. publisher로 publish 테스트 할 수 있음. client는 unsubscribe, subscribe 를 함수로 만들어서 테스트 해놓았음.

참고로 ID : test, test2, test4 는 client, test3은 publisher 권한임.

subscribe, unsubscribe 됬는지 확인하려면 163.180.117.199/phpmyadmin 이나, client로 로그인 뒤에 publish 메시지 오는지 확인하면 가능.

# 2016 06 03 추가

유저가 구독하고 있는 정보 조회 기능 연동 완료

새로운 그룹 생성 기능 연동 완료

단, 박나연은 NamuMysqlAccessor 에서 내가 수정한 부분이 맘에 안들면 추후에 수정할 것.

테스트용 client의 interactive 입력 추가