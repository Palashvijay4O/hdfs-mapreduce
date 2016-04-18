rm -r ../bin/* 
rm com/example/assignment/*.class
killall -9 java
javac -d ../bin -cp .:protobuf.jar com/example/assignment/assignment.java *.java
java -Djava.rmi.server.hostname=10.0.0.2 -Djava.security.policy=server.policy  -cp .:protobuf.jar:com/example/assignment:../bin StartFileServer
