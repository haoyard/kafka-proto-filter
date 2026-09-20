@echo off
REM kfilter 环境变量方式传递凭据示例（避免命令行历史泄漏）
REM set KAFKA_BROKERS=broker1:9093
REM kfilter -topic my_topic -tls -sasl scram-sha256 -sasl-user alice -sasl-pass *** ...
