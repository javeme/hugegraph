#!/bin/bash

CONFIG_PATH=$1
CONFIG_PATH=$(echo $CONFIG_PATH|sed 's/hugegraph-test\///g')

function serial_test() {
#    mvn test "-Dtest=CoreTestSuite, UnitTestSuite, StructureStandardTest, ProcessStandardTest" -P$PROFILE
    mvn test -Dtest=CoreTestSuite -Dconfig_path=$CONFIG_PATH

    if [ $? -ne 0 ]; then
        echo "Failed to test."
        exit 1
    fi
}

function parallel_test() {
    # Run tests with background process
    (mvn test "-Dtest=CoreTestSuite" -Dconfig_path=$CONFIG_PATH) &
    (mvn test "-Dtest=UnitTestSuite" -Dconfig_path=$CONFIG_PATH) &
    (mvn test "-Dtest=StructureStandardTest" -Dconfig_path=$CONFIG_PATH) &
    (mvn test "-Dtest=ProcessStandardTest" -Dconfig_path=$CONFIG_PATH) &

    # Wait for all child process finished
    for i in `seq 0 3`; do
        num=$(echo "$i+1" | bc -l)
        wait %$num
        if [ $? -ne 0 ]; then
            echo "Failed to test."
            exit 1
        fi
    done
}

echo "Start test with config $CONFIG_PATH"

# Enter test project
cd hugegraph-test

# Get the run-mode and run test
if [ $RUNMODE = "serial" ]; then
    echo "Run test in serial mode"
    serial_test
elif [ $RUNMODE = "parallel" ]; then
    echo "Run test in parallel mode"
    parallel_test
else
    echo "RUNMODE can only be 'serial' or 'parallel', but got $RUNMODE"
    echo "Failed to test."
    exit 1
fi

cd ../

echo "Finish test."

