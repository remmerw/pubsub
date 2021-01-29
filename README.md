# pubsub


go get -d github.com/remmerw/pubsub

cd $GOPATH/src/github.com/remmerw/pubsub

go mod vendor

go mod tidy

cd $HOME

set GO111MODULE=off

gomobile bind -o pubsub-1.0.6.aar -v -androidapi=26 -target=android -ldflags="-s -w" github.com/remmerw/pubsub
