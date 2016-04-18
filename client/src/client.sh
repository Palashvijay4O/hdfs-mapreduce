rm -r ../bin/*
rm com/example/assignment/*.class
javac -d ../bin -cp .:protobuf.jar com/example/assignment/assignment.java *.java
java -Djava.security.policy=client.policy -cp .:protobuf.jar:com/example/assignment/:../bin StartFileClient 10.0.0.2
