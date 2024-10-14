go clean

(go build -buildmode=plugin ../mrapps/wc.go) || exit 1
(go build mrcoordinator.go) || exit 1
(go build mrworker.go) || exit 1

TIMEOUT=timeout 10s

($TIMEOUT ./mrworker wc.so) &
($TIMEOUT ./mrworker wc.so) &
($TIMEOUT ./mrworker wc.so) &
