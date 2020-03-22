#JDKPATH = /s/java
#LIBPATH = /p/course/cs784-raghu/minibase_share/javaminibase/construction
JDKPATH=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.242.b08-0.el7_7.x86_64
LIBPATH=.:..

CLASSPATH = $(JDKPATH)/lib/classes.zip:.:..:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

PROGS = together

all: $(PROGS)

together:*.java
	$(JAVAC) *.java

clean:
	\rm -f *.class *~ \#* core 
