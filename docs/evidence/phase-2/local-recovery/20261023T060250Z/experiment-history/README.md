# 수용 결과에 합산하지 않는 이전 실험

- initial: 6 passed / 2 failed. IPv4-only Docker 포트에 advertised localhost를
  사용해 IPv6 ::1 연결이 실패했다. 127.0.0.1로 통일하고 restart readiness를 추가했다.
- intermediate: 6 passed 뒤 Broker 중단이 lazy source fetch를 대기시켜
  소유한 pytest controller를 SIGINT로 중단했다. 전용 컨테이너는 정리됐다.
- 최종 harness는 source materialization 이후 Broker를 중단해 Producer 실패를
  검증한다. production Job2의 source/validation/sink/checkpoint 코드는 바꾸지 않았다.
- 이 로그의 테스트 소스는 최종 버전과 다르며 acceptance source hash로 취급하지 않는다.
