#! /bin/sh

java --add-exports java.base/sun.nio.ch=ALL-UNNAMED -Xms256m -Xmx16384m -classpath 'lib/*' copytab.Copier "$@"
