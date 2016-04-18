rm -r ../bin/* 
rm com/example/assignment/*.class
rm *.txt
javac -d ../bin -cp .:protobuf.jar com/example/assignment/assignment.java com/example/assignment2/assignment2.java *.java
java -Djava.security.policy=server.policy -cp .:protobuf.jar:com/example/assignment:com/example/assignment2:../bin StartTaskTracker 10.0.0.2 2