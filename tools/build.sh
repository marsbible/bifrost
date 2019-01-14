#!/bin/bash

#第一个参数项目目录
#第二个参数模型文件名

if [ "$#" -lt 2 ]; then
   echo "Usage: build.sh projectDir modelFile"
   exit 0
fi

mkdir "$1"
cp -r ../bifrost/worker/* "$1"

java -jar tools/bifrost-tools.jar --output-pb "$1/src/main/protobuf/predict.proto" --output-service "$1/src/main/scala/org/bifrost/worker/service/PredictServiceImpl.scala" "$2"

cd "$1"
gradle build

jar=`ls build/libs/*.jar`
cp "$jar" -f build/output/ 
echo `pwd`
cp -r "src/main/protobuf" build/output/

cd -
cp -r "./conf" "$1/build/output/"

echo -e "\e[92m编译完成：cd $1/build/output && java -Dcom.sun.management.jmxremote.port=12345 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dconfig.file=conf/application.conf -jar bifrost-worker.jar ../../../$2\e[0m"
echo ""
# 打包成docker镜像
