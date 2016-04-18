rm -r ../bin/* 
rm com/example/assignment/*.class
rm *.txt && touch halwai.txt
killall -9 java
javac -d ../bin -cp .:protobuf.jar com/example/assignment/assignment.java *.java
java -Djava.rmi.server.hostname=10.0.0.3 -Djava.security.policy=server.policy  -cp .:protobuf.jar:com/example/assignment:../bin StartDataNode palashvm 10.0.0.3 0
