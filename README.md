1. 主项目为manager项目，用来实现服务注册和服务发现等功能，尚未开发
2. mleap为mleap的测试代码，供开发者参考
3. worker为预测服务的模板工程，后续的线上服务代码都基于此模板
4. tools为辅助工具，主要根据mleap模型自动生成protobuf和相应的Scala文件
5. grpcclient工具：grpcui -plaintext -proto xgb.proto 172.16.98.13:8282

