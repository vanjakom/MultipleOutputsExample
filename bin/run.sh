#!/bin/sh

java -cp target/MultipleOutputsExample-1.0-jar-with-dependencies.jar:/etc/hadoop/conf/ com.busywait.MultipleOutputsExample.MainClass $@