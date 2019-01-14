##### 本woker程序作为模板项目，业务项目需要以此项目为父项目，引入特定的代码
- 本项目基于akka和grpc实现，同时为了方便开发自定义服务，引入了对mysql,redis,es,solr,mongo,hbase等常见
数据库的支持
- 不需要特殊处理的，根据mleap模型直接调用build脚本即可自动生成相应的proto和scala文件
，并最终编译生成可以运行的jar包
- 需要自定义service的，将自定义服务的proto文件放在src/main/protobuf下，将自定义service代码放在
src/main/scala/service目录下。
- predict.proto和PredictServiceImpl.scala都是tools工具根据mleap模型自动生成的，不要修改！
- worker.proto和WorkerServiceImpl主要是演示如何开发自定义服务，供用户参考
