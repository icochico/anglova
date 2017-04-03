# Set the GOPATH
GOPATH := ${PWD}/
export GOPATH

# This is how we want to name the binary output
BINARY=anglova

# These are the values we want to pass for Version and BuildTime
VERSION=1.0.0
BUILD_TIME=`date +%FT%T%z`
GIT_HASH=`git rev-parse HEAD`

# Setup the -ldflags option for go build here, interpolate the variable values
LDFLAGS=-ldflags "-X ihmc.us/anglova/cmd.Version=${VERSION} -X ihmc.us/anglova/cmd.BuildTime=${BUILD_TIME} -X ihmc.us/anglova/cmd.GitHash=${GIT_HASH}"

all:
	go get ihmc.us/anglova
	go build ${LDFLAGS} -o bin/${BINARY} ihmc.us/anglova
