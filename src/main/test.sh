# 清理相关
go clean
rm -f mr-map-*

# 兜底相关
ISQUIET=$1
maybe_quiet() {
  if [ "$ISQUIET" == "quiet" ]; then
    "$@" >/dev/null 2>&1
  else
    "$@"
  fi
}

TIMEOUT=timeout
TIMEOUT2=""
if timeout 2s sleep 1 >/dev/null 2>&1; then
  :
else
  if gtimeout 2s sleep 1 >/dev/null 2>&1; then
    TIMEOUT=gtimeout
  else
    # no timeout command
    TIMEOUT=
    echo '*** Cannot find timeout command; proceeding without timeouts.'
  fi
fi
if [ "$TIMEOUT" != "" ]; then
  TIMEOUT2=$TIMEOUT
  TIMEOUT2+=" -k 2s 120s "
  TIMEOUT+=" -k 2s 45s "
fi

# 方便cd
cd mr-tmp || exit 1

# 编译插件
(cd ../../mrapps && go clean)
(cd .. && go clean)
(cd ../../mrapps && go build -buildmode=plugin wc.go) || exit 1
# (cd ../../mrapps && go build -buildmode=plugin indexer.go) || exit 1
# (cd ../../mrapps && go build -buildmode=plugin mtiming.go) || exit 1
# (cd ../../mrapps && go build -buildmode=plugin rtiming.go) || exit 1
# (cd ../../mrapps && go build -buildmode=plugin jobcount.go) || exit 1
# (cd ../../mrapps && go build -buildmode=plugin early_exit.go) || exit 1
# (cd ../../mrapps && go build -buildmode=plugin crash.go) || exit 1
# (cd ../../mrapps && go build -buildmode=plugin nocrash.go) || exit 1
(cd .. && go build mrcoordinator.go) || exit 1
(cd .. && go build mrworker.go) || exit 1
(cd .. && go build mrsequential.go) || exit 1

# =========================== wordcount

# 准备工作
../mrsequential ../../mrapps/wc.so ../pg*txt || exit 1
sort mr-out-0 >mr-correct-wc.txt
rm -f mr-out*

# 开Coordinator
maybe_quiet $TIMEOUT ../mrcoordinator ../pg*txt &

# 开Worker
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/wc.so) &
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/wc.so) &
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/wc.so) &
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/wc.so) &
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/wc.so) &
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/wc.so) &
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/wc.so) &

wait $pid

# 验证答案
sort mr-out* | grep . >mr-wc-all
if cmp mr-wc-all mr-correct-wc.txt; then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
  failed_any=1
fi

wait
