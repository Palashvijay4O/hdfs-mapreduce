rm -r ../bin/* 
rm com/example/assignment/*.class
javac -d ../bin -cp .:protobuf.jar com/example/assignment/assignment.java com/example/assignment2/assignment2.java *.java
java -Djava.rmi.server.hostname=10.0.0.2 -Djava.security.policy=server.policy  -cp .:protobuf.jar:com/example/assignment:com/example/assignment2:../bin StartJobTracker