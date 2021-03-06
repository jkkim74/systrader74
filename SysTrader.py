import sys
import os
import time
from collections import deque
import threading
from threading import Event
from threading import Lock
import logging
from logging import FileHandler

from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtCore import QObject
from PyQt5.QtCore import QThread
from PyQt5.QtCore import QEventLoop
from PyQt5.QtWidgets import QApplication
import FinanceDataReader as fdr
from datetime import datetime
import pickle
import numpy as np
import pandas as pd
import logging
from logging.handlers import TimedRotatingFileHandler

class RequestThreadWorker(QObject):
    def __init__(self):
        """요청 쓰레드
        """
        super().__init__()
        self.request_queue = deque()  # 요청 큐
        self.request_thread_lock = Lock()

        # 간혹 요청에 대한 결과가 콜백으로 오지 않음
        # 마지막 요청을 저장해 뒀다가 일정 시간이 지나도 결과가 안오면 재요청
        self.retry_timer = None

    def retry(self, request):
        logger.debug("키움 함수 재시도: %s %s %s" % (request[0].__name__, request[1], request[2]))
        self.request_queue.appendleft(request)

    def run(self):
        while True:
            # 큐에 요청이 있으면 하나 뺌
            # 없으면 블락상태로 있음
            try:
                request = self.request_queue.popleft()
            except IndexError as e:
                time.sleep(2)
                continue

            # 요청 실행
            logger.debug("키움 함수 실행: %s %s %s" % (request[0].__name__, request[1], request[2]))
            request[0](trader, *request[1], **request[2])

            # 요청에대한 결과 대기
            if not self.request_thread_lock.acquire(blocking=True, timeout=5):
                # 요청 실패
                time.sleep(2)
                self.retry(request)  # 실패한 요청 재시도

            time.sleep(2)  # 0.2초 이상 대기 후 마무리

class SyncRequestDecorator:
    '''키움 API 동기화 데코레이터
    '''
    @staticmethod
    def kiwoom_sync_request(func):
        def func_wrapper(self, *args, **kwargs):
            self.request_thread_worker.request_queue.append((func, args, kwargs))
        return func_wrapper

    @staticmethod
    def kiwoom_sync_callback(func):
        def func_wrapper(self, *args, **kwargs):
            logger.debug("키움 함수 콜백: %s %s %s" % (func.__name__, args, kwargs))
            func(self, *args, **kwargs)  # 콜백 함수 호출
            if self.request_thread_worker.request_thread_lock.locked():
                self.request_thread_worker.request_thread_lock.release()  # 요청 쓰레드 잠금 해제
        return func_wrapper

class SysTrader():
    def __init__(self):
        """자동투자시스템 메인 클래스
        """
        # 요청 쓰레드
        self.request_thread_worker = RequestThreadWorker()
        self.request_thread = QThread()
        self.request_thread_worker.moveToThread(self.request_thread)
        self.request_thread.started.connect(self.request_thread_worker.run)
        self.request_thread.start()

        self.kiwoom = QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")
        self.kiwoom.OnEventConnect.connect(self.kiwoom_OnEventConnect)  # 로그인 결과를 받을 콜백함수 연결
        self.kiwoom.OnReceiveChejanData.connect(self.kiwoom_OnReceiveChejanData)

        self.order_loop = None

    # -------------------------------------
    # 로그인 관련함수
    # -------------------------------------
    @SyncRequestDecorator.kiwoom_sync_request
    def kiwoom_CommConnect(self):
        """로그인 요청
        키움증권 로그인창 띄워주고, 자동로그인 설정시 바로 로그인 진행됨.
        OnEventConnect()으로 콜백 전달됨.
        :param kwargs:
        :return: 1: 로그인 요청 성공, 0: 로그인 요청 실패
        """
        lRet = self.kiwoom.dynamicCall("CommConnect()")
        return lRet

    @SyncRequestDecorator.kiwoom_sync_callback
    def kiwoom_OnEventConnect(self, nErrCode):
        """로그인 결과 수신
        :param nErrCode: 0: 로그인 성공, 100: 사용자 정보교환 실패, 101: 서버접속 실패, 102: 버전처리 실패
        :param kwargs:
        :return:
        """
        if nErrCode == 0:
            logger.debug("로그인 성공")
        elif nErrCode == 100:
            logger.debug("사용자 정보교환 실패")
        elif nErrCode == 101:
            logger.debug("서버접속 실패")
        elif nErrCode == 102:
            logger.debug("버전처리 실패")

    # -------------------------------------
    # 주문 관련함수
    # OnReceiveTRData(), OnReceiveMsg(), OnReceiveChejan()
    # -------------------------------------
    @SyncRequestDecorator.kiwoom_sync_request
    def kiwoom_SendOrder(self, sRQName, sScreenNo, sAccNo, nOrderType, sCode, nQty, nPrice, sHogaGb, sOrgOrderNo,
                         **kwargs):
        logger.debug("주문: %s %s %s %s %s %s %s %s %s" % (
            sRQName, sScreenNo, sAccNo, nOrderType, sCode, nQty, nPrice, sHogaGb, sOrgOrderNo))
        lRet = self.kiwoom.dynamicCall("SendOrder(QString, QString, QString, int, QString, int, int, QString, QString)",
                                [sRQName, sScreenNo, sAccNo, nOrderType, sCode, nQty, nPrice, sHogaGb,
                                 sOrgOrderNo])
        self.order_loop = QEventLoop
        self.order_loop.exec_()

    @SyncRequestDecorator.kiwoom_sync_callback
    def kiwoom_OnReceiveChejanData(self, sGubun, nItemCnt, sFIdList, **kwargs):
        logger.debug("체결/잔고: %s %s %s" % (sGubun, nItemCnt, sFIdList))
        for fid in sFIdList:
            print(self.kiwoom_GetChejanData(fid))
        self.order_loop.exit()

    @SyncRequestDecorator.kiwoom_sync_callback
    def kiwoom_GetChejanData(self, nFid):
        res = self.kiwoom.dynamicCall("GetChejanData(int)", [nFid])
        return res


if __name__ == "__main__":
    # --------------------------------------------------
    # 로거 (Logger) 준비하기
    # --------------------------------------------------
    # 로그 파일 핸들러
    fh_log = TimedRotatingFileHandler('logs/log', when='midnight', encoding='utf-8', backupCount=120)
    fh_log.setLevel(logging.DEBUG)

    # 콘솔 핸들러
    sh = logging.StreamHandler()
    sh.setLevel(logging.DEBUG)

    # 로깅 포멧 설정
    formatter = logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s')
    fh_log.setFormatter(formatter)
    sh.setFormatter(formatter)

    # 로거 생성 및 핸들러 등록
    logger = logging.getLogger(__file__)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(fh_log)
    logger.addHandler(sh)

    # --------------------------------------------------
    # 자동투자시스템 시작
    # --------------------------------------------------
    app = QApplication(sys.argv)  # Qt 애플리케이션 생성
    trader = SysTrader()
    trader.kiwoom_CommConnect()  # 로그인
    buy_stock_list = [('005690','16950'),('005930','44750')]
    for stock in buy_stock_list:
        trader.kiwoom_SendOrder('RQ_1', '0101', '8111294711', 1, stock[0], 1, stock[1], '00', '')

    sys.exit(app.exec_())