#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from logging.handlers import TimedRotatingFileHandler
from collections import deque
from threading import Lock
import time
from PyQt5.QtCore import *
from PyQt5.QtWidgets import *
from PyQt5.QAxContainer import *
import sys
import util

ACCOUNT_NO = '8111294711'

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

class SysTrader(QObject):
    def __init__(self):
        """자동투자시스템 메인 클래스
        """
        super().__init__()
        self.kiwoom = QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")
        self.kiwoom.OnEventConnect.connect(self.kiwoom_OnEventConnect)
        self.kiwoom.OnReceiveTrData.connect(self.kiwoom_OnReceiveTrData)
        # 요청 쓰레드
        self.request_thread_worker = RequestThreadWorker()
        self.request_thread = QThread()
        self.request_thread_worker.moveToThread(self.request_thread)
        self.request_thread.started.connect(self.request_thread_worker.run)
        self.request_thread.start()

        self.kiwoom_CommConnect()  # 로그인
        self.kiwoom_TR_OPW00001_DEPOSIT_DETAIL(self,ACCOUNT_NO,screenNo='0101')  # 계좌정보 확인
        #self.get_account(self) # 계좌번호

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

    @SyncRequestDecorator.kiwoom_sync_request
    def kiwoom_TR_OPW00001_DEPOSIT_DETAIL(self,accountNo, **kwargs):
        res = self.kiwoom_SetInputValue("계좌번호", accountNo)
        res = self.kiwoom_CommRqData("예수금상세현황요청", "opw00001", 0, '0136')

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

    def kiwoom_SetInputValue(self, sID, sValue):
        res = self.kiwoom.dynamicCall("SetInputValue(QString, QString)", [sID, sValue])
        return res

    def kiwoom_CommRqData(self, sRQName, sTrCode, nPrevNext, sScreenNo):
        res = self.kiwoom.dynamicCall("CommRqData(QString, QString, int, QString)",
                                      [sRQName, sTrCode, nPrevNext, sScreenNo])
        return res

    @SyncRequestDecorator.kiwoom_sync_callback
    def kiwoom_OnReceiveTrData(self, sScrNo, sRQName, sTRCode, sRecordName, sPreNext, nDataLength, sErrorCode, sMessage,
                               sSPlmMsg, **kwargs):
        """TR 요청에 대한 결과 수신
        데이터 얻어오기 위해 내부에서 GetCommData() 호출
          GetCommData(
          BSTR strTrCode,   // TR 이름
          BSTR strRecordName,   // 레코드이름
          long nIndex,      // TR반복부
          BSTR strItemName) // TR에서 얻어오려는 출력항목이름
        :param sScrNo: 화면번호
        :param sRQName: 사용자 구분명
        :param sTRCode: TR이름
        :param sRecordName: 레코드 이름
        :param sPreNext: 연속조회 유무를 판단하는 값 0: 연속(추가조회)데이터 없음, 2:연속(추가조회) 데이터 있음
        :param nDataLength: 사용안함
        :param sErrorCode: 사용안함
        :param sMessage: 사용안함
        :param sSPlmMsg: 사용안함
        :param kwargs:
        :return:
        """

        if sRQName == "예수금상세현황요청":
            self.int_주문가능금액 = int(self.kiwoom_GetCommData(sTRCode, sRQName, 0, "주문가능금액"))
            logger.debug("예수금상세현황요청: %s" % (self.int_주문가능금액,))
            if "예수금상세현황요청" in self.dict_callback:
                self.dict_callback["예수금상세현황요청"](self.int_주문가능금액)

        elif sRQName == "주식기본정보":
            cnt = self.kiwoom_GetRepeatCnt(sTRCode, sRQName)
            list_item_name = ["종목명", "현재가", "등락율", "거래량"]
            종목코드 = self.kiwoom_GetCommData(sTRCode, sRQName, 0, "종목코드")
            종목코드 = 종목코드.strip()
            dict_stock = self.dict_stock.get(종목코드, {})
            for item_name in list_item_name:
                item_value = self.kiwoom_GetCommData(sTRCode, sRQName, 0, item_name)
                item_value = item_value.strip()
                dict_stock[item_name] = item_value
            self.dict_stock[종목코드] = dict_stock
            logger.debug("주식기본정보: %s, %s" % (종목코드, dict_stock))
            if "주식기본정보" in self.dict_callback:
                self.dict_callback["주식기본정보"](dict_stock)

        elif sRQName == "시세표성정보":
            cnt = self.kiwoom_GetRepeatCnt(sTRCode, sRQName)
            list_item_name = ["종목명", "현재가", "등락률", "거래량"]
            dict_stock = {}
            for item_name in list_item_name:
                item_value = self.kiwoom_GetCommData(sTRCode, sRQName, 0, item_name)
                item_value = item_value.strip()
                dict_stock[item_name] = item_value
            if "시세표성정보" in self.dict_callback:
                self.dict_callback["시세표성정보"](dict_stock)

        elif sRQName == "주식분봉차트조회" or sRQName == "주식일봉차트조회":
            cnt = self.kiwoom_GetRepeatCnt(sTRCode, sRQName)

            종목코드 = self.kiwoom_GetCommData(sTRCode, sRQName, 0, "종목코드")
            종목코드 = 종목코드.strip()

            done = False  # 파라미터 처리 플래그
            result = self.result.get('result', [])
            cnt_acc = len(result)

            list_item_name = []
            if sRQName == '주식분봉차트조회':
                # list_item_name = ["현재가", "거래량", "체결시간", "시가", "고가",
                #                   "저가", "수정주가구분", "수정비율", "대업종구분", "소업종구분",
                #                   "종목정보", "수정주가이벤트", "전일종가"]
                list_item_name = ["체결시간", "시가", "고가", "저가", "현재가", "거래량"]
            elif sRQName == '주식일봉차트조회':
                list_item_name = ["일자", "시가", "고가", "저가", "현재가", "거래량"]

            for nIdx in range(cnt):
                item = {'종목코드': 종목코드}
                for item_name in list_item_name:
                    item_value = self.kiwoom_GetCommData(sTRCode, sRQName, nIdx, item_name)
                    item_value = item_value.strip()
                    item[item_name] = item_value

                # 범위조회 파라미터
                date_from = int(self.params.get("date_from", "000000000000"))
                date_to = int(self.params.get("date_to", "999999999999"))

                # 결과는 최근 데이터에서 오래된 데이터 순서로 정렬되어 있음
                date = None
                if sRQName == '주식분봉차트조회':
                    date = int(item["체결시간"])
                elif sRQName == '주식일봉차트조회':
                    date = int(item["일자"])
                    if date > date_to:
                        continue
                    elif date < date_from:
                        done = True
                        break

                # 개수 파라미터처리
                if cnt_acc + nIdx >= self.params.get('size', float("inf")):
                    done = True
                    break

                #result.append(util.convert_kv(item))

            # 차트 업데이트
            self.result['result'] = result

            if not done and cnt > 0 and sPreNext == '2':
                self.result['nPrevNext'] = 2
                self.result['done'] = False
            else:
                # 연속조회 완료
                logger.debug("차트 연속조회완료")
                self.result['nPrevNext'] = 0
                self.result['done'] = True

        elif sRQName == "업종일봉조회":
            cnt = self.kiwoom_GetRepeatCnt(sTRCode, sRQName)

            업종코드 = self.kiwoom_GetCommData(sTRCode, sRQName, 0, "업종코드")
            업종코드 = 업종코드.strip()

            done = False  # 파라미터 처리 플래그
            result = self.result.get('result', [])
            cnt_acc = len(result)

            list_item_name = []
            if sRQName == '업종일봉조회':
                list_item_name = ["일자", "시가", "고가", "저가", "현재가", "거래량"]

            for nIdx in range(cnt):
                item = {'업종코드': 업종코드}
                for item_name in list_item_name:
                    item_value = self.kiwoom_GetCommData(sTRCode, sRQName, nIdx, item_name)
                    item_value = item_value.strip()
                    item[item_name] = item_value

                # 결과는 최근 데이터에서 오래된 데이터 순서로 정렬되어 있음
                date = int(item["일자"])

                # 범위조회 파라미터 처리
                date_from = int(self.params.get("date_from", "000000000000"))
                date_to = int(self.params.get("date_to", "999999999999"))
                if date > date_to:
                    continue
                elif date < date_from:
                    done = True
                    break

                # 개수 파라미터처리
                if cnt_acc + nIdx >= self.params.get('size', float("inf")):
                    done = True
                    # break

                #result.append(util.convert_kv(item))

            # 차트 업데이트
            self.result['result'] = result

            if not done and cnt > 0 and sPreNext == '2':
                self.result['nPrevNext'] = 2
                self.result['done'] = False
            else:
                # 연속조회 완료
                logger.debug("차트 연속조회완료")
                self.result['nPrevNext'] = 0
                self.result['done'] = True

        elif sRQName == "계좌수익률요청":
            cnt = self.kiwoom_GetRepeatCnt(sTRCode, sRQName)
            for nIdx in range(cnt):
                list_item_name = ["종목코드", "종목명", "현재가", "매입가", "보유수량"]
                dict_holding = {item_name: self.kiwoom_GetCommData(sTRCode, sRQName, nIdx, item_name).strip() for
                                item_name in list_item_name}
                dict_holding["현재가"] = util.safe_cast(dict_holding["현재가"], int, 0)
                # 매입가를 총매입가로 키변경
                dict_holding["총매입가"] = util.safe_cast(dict_holding["매입가"], int, 0)
                dict_holding["보유수량"] = util.safe_cast(dict_holding["보유수량"], int, 0)
                dict_holding["수익"] = (dict_holding["현재가"] - dict_holding["총매입가"]) * dict_holding["보유수량"]
                종목코드 = dict_holding["종목코드"]
                self.dict_holding[종목코드] = dict_holding
                logger.debug("계좌수익: %s" % (dict_holding,))
            if '계좌수익률요청' in self.dict_callback:
                self.dict_callback['계좌수익률요청'](self.dict_holding)

        if self.event is not None:
            self.event.exit()

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
    sys.exit(app.exec_())