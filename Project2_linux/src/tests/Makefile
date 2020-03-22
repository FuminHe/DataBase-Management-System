# modify the paths appropriately
#JDKPATH = /usr/java/jdk1.3.1_02
#LIBPATH = .:..
JDKPATH=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.242.b08-0.el7_7.x86_64
LIBPATH=.:..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)


# test query

Test:project2_main_func.java
	$(JAVAC) TestDriver.java project2_main_func.java

main: Test
	$(JAVA) tests.project2_main_func

# test 
#project2_main_func: project2_main_func.java
 #   $(JAVAC) TestDriver.java insert.java project2_main_func.java 
    
#main: project2_main_func
#	$(JAVA) project2_main_func.java


