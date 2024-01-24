import os
from PyQt5.QtCore import *
from PyQt5.QtTest import * 
from PyQt5.QtWidgets import *
from PyQt5.QAxContainer import *
from config.kiwoomType import *
from config.errorCode import errors
import sqlite3
from datetime import datetime
import asyncio
import socket
import json
import threading
import time 
# Initialize the event object somewhere accessible to both signal sender and this function




class Kiwoom(QAxWidget):
    def __init__(self):
        super().__init__()
        
        
        ########## 이벤트 루프 모음 
        
        self.login_event_loop = None
        self.detail_acc_info_event_loop = None
        self.account_eval_event_loop = None
        self.michaegul_event_loop = None
        self.day_chart_event_loop =None
        self.top_trading_volume_event_loop = None
        self.get_tick_event_loop = None

        
        ########## 변수 모음 
        
        self.realType = RealType()
        self.account_num = None
        self.account_stock_dict = {}
        self.michaegul_dict = {}
        self.day_data_all = []
        self.portfolio_stock_dict = {"047310":{}}
        self.jango_dict = {}
        self.top_volume_dict = {}
        self.columns_definition = {}
        self.columns_definition_chaegul = {}
        self.batch_data_chaegul = []
        self.batch_data_hoga = []
        self.BATCH_SIZE_CHAEGUL = 1000 
        self.BATCH_SIZE_HOGA = 400 
        self.test = {}
        
        ########## 스크린번호 모음
        
        self.market_time_screen = '1000'  # 장시간 구분 
        self.top_volume_screen = '1001'   # 거래대금상위
        self.screen_my_info = '2000'      # 계좌조회
        self.day_chart_screen = '4000'    # 일봉조회 
        self.tick_screen = '4001'         # 틱봉조회
        self.minute_screen = '4002'       # 분봉조회
        self.screen_real_stock = '5000'   # 종목별로할당할 스크린 번호
        self.screen_meme_stock = '6000'   # 종목별 매매할 스크린 번호
        self.screen_hoga_stock = '7000'   # 호가 스크린 번호 
        
        
        ########### 함수 실행
        
        print('Kiwoom class')
        self.setting_done = False 
        self.get_ocx_instance()             # 실행
        self.event_slot()                   # 이벤트 슬롯 
        self.real_event_slot()              # 실시간 데이터 슬롯 
        self.signal_login_CommConnect()     # 로그인하기 
        self.get_account_info()             # 계좌번호가져오기
        self.detail_acc_info()              # 예수금 가져오기

        
        self.get_minute('047310')
        
        
        # self.account_eval()                 # 계좌평가잔고내역
        # self.michaegul()                    # 미체결조회
        
        
        # self.top_trading_volume()           # 당일거래량상위요청
        # self.init_screen()
        
        
        # self.get_market_time()              # 장시간운영구분
        # self.screen_number_set()            # 스크린번호세팅
        # self.register_stock_on_real_time()  # 실시간 코드등록 , 주식체결 
        # self.hoga_remain()                  # 실시간 호가등록
        # print('* screen setting complete ! *')
        # self.setting_done = True

        
    ################# 함수 모음
    def get_minute(self,code, sPrevNext="0"):
        self.dynamicCall(f"SetInputValue(String,String)","종목코드", code)
        self.dynamicCall(f"SetInputValue(String,String)",'틱범위','1')
        self.dynamicCall(f"SetInputValue(String,String)",'수정주가구분','1')
        
        self.dynamicCall(f"CommRqData(String,String,int,String)",'주식분봉차트조회요청','OPT10080', sPrevNext, self.tick_screen)
        
        
        if sPrevNext == '0':
            self.get_minute_event_loop = QEventLoop()
            self.get_minute_event_loop.exec_()
    
    
    
    def get_tick(self,code, sPrevNext="0"):
        self.dynamicCall(f"SetInputValue(String,String)","종목코드", code)
        self.dynamicCall(f"SetInputValue(String,String)",'틱범위','1')
        self.dynamicCall(f"SetInputValue(String,String)",'수정주가구분','1')
        
        self.dynamicCall(f"CommRqData(String,String,int,String)",'주식틱차트조회요청','opt10079', sPrevNext, self.tick_screen)
        
        
        if sPrevNext == '0':
            self.get_tick_event_loop = QEventLoop()
            self.get_tick_event_loop.exec_()
    
    
    def init_screen(self):
        self.dynamicCall("SetRealRemove(QString,QString)","ALL","ALL")
        print('sleep for 5 seconds for init all screen ...')
        time.sleep(5)
        self.batch_data_chaegul = []
        self.batch_data_hoga = []

        
    def send_data_to_server_chaegul(self,data):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(('localhost', 12345))
            s.sendall(json.dumps(data).encode('utf-8'))
            
    def send_data_to_server_hoga(self,data):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(('localhost', 23456))
            s.sendall(json.dumps(data).encode('utf-8'))

    
    def does_table_exist_hoga(self, table_name):
        self.cursor_hoga.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if self.cursor_hoga.fetchone() is None:
            table_sql = self.create_dynamic_table_sql_hoga(table_name=table_name)
            self.cursor_hoga.execute(table_sql)
            
    def does_table_exist_chaegul(self, table_name):
        self.cursor_chaegul.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if self.cursor_chaegul.fetchone() is None:
            table_sql = self.create_dynamic_table_sql_chaegul(table_name=table_name)
            self.cursor_chaegul.execute(table_sql)
            
    
    def create_dynamic_table_sql_hoga(self,table_name):
        self.columns_definition = {
            "호가시간": "TEXT",  # Assuming 호가시간 is of TEXT type, modify as needed
        }

        for hoga_type in ['매도호가', '매수호가']:
            for i in range(10):
                adjusted_i = abs(i - 10) if hoga_type == '매도호가' else i + 1
                
                price = f'{hoga_type}{adjusted_i}'
                quantity = f'{hoga_type}수량{adjusted_i}'
                comparison = f'{hoga_type}직전대비{adjusted_i}'

                # Adding the columns to the definition, assuming they are all INTEGER type
                self.columns_definition[price] = "TEXT"
                self.columns_definition[quantity] = "TEXT"
                self.columns_definition[comparison] = "TEXT"
        create_table_sql = f'CREATE TABLE "{table_name}" (\nid INTEGER PRIMARY KEY AUTOINCREMENT,\n' + ',\n'.join([f'"{col_name}" {data_type}' for col_name, data_type in self.columns_definition.items()]) + '\n);'
        return create_table_sql
    
    
    def create_dynamic_table_sql_chaegul(self,table_name):
        self.columns_definition_chaegul = {
            '체결시간' : 'TEXT',
            '현재가' : 'TEXT',
            '거래량' : 'TEXT'
        }
        create_table_sql = f'CREATE TABLE "{table_name}" (\nid INTEGER PRIMARY KEY AUTOINCREMENT,\n' + ',\n'.join([f'"{col_name}" {data_type}' for col_name, data_type in self.columns_definition_chaegul.items()]) + '\n);'
        return create_table_sql
        
        
    
    def db_on(self):
        today_date = datetime.now().strftime("%Y%m%d")
        
        self.conn_hoga = sqlite3.connect(f'{today_date}_hoga.db')
        self.cursor_hoga = self.conn_hoga.cursor()
        
        self.conn_chaegul = sqlite3.connect(f'{today_date}_chaegul.db')
        self.cursor_chaegul = self.conn_chaegul.cursor()

        
        
    
    def top_trading_volume(self,sPrevNext='0'):
        '''
            시장구분 = 000:전체, 001:코스피, 101:코스닥
            정렬구분 = 1:거래량, 2:거래회전율, 3:거래대금
            관리종목포함 = 0:관리종목 포함, 1:관리종목 미포함, 3:우선주제외, 11:정리매매종목제외, 4:관리종목, 우선주제외, 5:증100제외, 6:증100마나보기, 13:증60만보기, 12:증50만보기, 7:증40만보기, 8:증30만보기, 9:증20만보기, 14:ETF제외, 15:스팩제외, 16:ETF+ETN제외
            신용구분 = 0:전체조회, 9:신용융자전체, 1:신용융자A군, 2:신용융자B군, 3:신용융자C군, 4:신용융자D군, 8:신용대주
            거래량구분 = 0:전체조회, 5:5천주이상, 10:1만주이상, 50:5만주이상, 100:10만주이상, 200:20만주이상, 300:30만주이상, 500:500만주이상, 1000:백만주이상
            가격구분 = 0:전체조회, 1:1천원미만, 2:1천원이상, 3:1천원~2천원, 4:2천원~5천원, 5:5천원이상, 6:5천원~1만원, 10:1만원미만, 7:1만원이상, 8:5만원이상, 9:10만원이상
            거래대금구분 = 0:전체조회, 1:1천만원이상, 3:3천만원이상, 4:5천만원이상, 10:1억원이상, 30:3억원이상, 50:5억원이상, 100:10억원이상, 300:30억원이상, 500:50억원이상, 1000:100억원이상, 3000:300억원이상, 5000:500억원이상
            장운영구분 = 0:전체조회, 1:장중, 2:장전시간외, 3:장후시간외
        '''
        self.dynamicCall(f"SetInputValue(String,String)","시장구분",'101')
        self.dynamicCall(f"SetInputValue(String,String)","정렬구분",'1')
        self.dynamicCall(f"SetInputValue(String,String)","관리종목포함",'16')
        self.dynamicCall(f"SetInputValue(String,String)","신용구분",'0')
        self.dynamicCall(f"SetInputValue(String,String)","거래량구분",'100')
        self.dynamicCall(f"SetInputValue(String,String)","가격구분",'2')
        self.dynamicCall(f"SetInputValue(String,String)","거래대금구분",'10')
        self.dynamicCall(f"SetInputValue(String,String)","장운영구분",'0')
        
        self.dynamicCall(f"CommRqData(String,String,int,String)",'당일거래량상위요청','opt10030', sPrevNext, self.top_volume_screen)
        
        self.top_trading_volume_event_loop = QEventLoop()
        self.top_trading_volume_event_loop.exec_()
        
        
    def register_stock_on_real_time(self):
        for code in self.portfolio_stock_dict.keys():
            secreen_number = self.portfolio_stock_dict[code]['스크린번호']
            fids = self.realType.REALTYPE['주식체결']['체결시간']
            self.dynamicCall("SetRealReg(QString,QString,QString,QString)",secreen_number,code,fids,'1')
            print(f'{code} 실시간 주식체결 - 체결시간 등록')
    
    
    def get_market_time(self):
        self.dynamicCall("SetRealReg(QString,QString,QString,QString)",self.market_time_screen,'',self.realType.REALTYPE['장시작시간']['장운영구분'],'0')

    
    def hoga_remain(self):
        
        for code in list(self.portfolio_stock_dict.keys()):
            
            hoga_time_fid = self.realType.REALTYPE['주식호가잔량']['호가시간']
            hoga_sceen_num = self.portfolio_stock_dict[code]['호가스크린번호']
            # self.dynamicCall("SetRealReg(QString,QString,QString,QString)",hoga_sceen_num,code,hoga_time_fid,'0')
            
            
            for hoga_type in ['매도호가', '매수호가']:
                for i in range(10):
                    if hoga_type == '매도호가':
                        i = abs(i - 10 )
                    else : 
                        i += 1
                        
                    price = f'{hoga_type}{i}'
                    quantity = f'{hoga_type}수량{i}'
                    comparison = f'{hoga_type}직전대비{i}'
            
                    hoga_price_fid = self.realType.REALTYPE['주식호가잔량'][price]
                    hoga_quan_fid = self.realType.REALTYPE['주식호가잔량'][quantity]
                    hoga_com_fid = self.realType.REALTYPE['주식호가잔량'][comparison]
            

                    self.dynamicCall("SetRealReg(QString,QString,QString,QString)",hoga_sceen_num,code,hoga_price_fid,'1')
                    self.dynamicCall("SetRealReg(QString,QString,QString,QString)",hoga_sceen_num,code,hoga_quan_fid,'1')
                    self.dynamicCall("SetRealReg(QString,QString,QString,QString)",hoga_sceen_num,code,hoga_com_fid,'1')
            
            print(f'{code} 실시간 주식호가잔량 - 호가시간 등록')

     
    
    
        
    def get_ocx_instance(self):
        self.setControl('KHOPENAPI.KHOpenAPICtrl.1')  
        
    
    def event_slot(self):
        self.OnEventConnect.connect(self.login_slot)
        self.OnReceiveTrData.connect(self.trdata_slot)
        self.OnReceiveMsg.connect(self.msg_slot)

    def real_event_slot(self):
        self.OnReceiveRealData.connect(self.real_data_slot)   # 실시간 종목 정보
        self.OnReceiveChejanData.connect(self.chejan_slot)    # 주문전송 후 주문접수, 체결통보, 잔고통보를 수신
        
        
    def login_slot(self,errCode):
        err = errors(errCode)
        print(f'로그인 ... {err}')
        self.login_event_loop.exit()
        
    def msg_slot(self,sScrNo,sRQName,sTrCode,msg):
        print(f'스크린 : {sScrNo}, 요청이름 : {sRQName}, tr코드 : {sTrCode} - {msg}')
    
    def signal_login_CommConnect(self):
        self.dynamicCall('CommConnect')
        self.login_event_loop = QEventLoop()
        self.login_event_loop.exec_()
        
        
    def get_account_info(self):
        account_list = self.dynamicCall("GetLoginInfo(ACCNO)")    
        self.account_num = account_list.split(';')[0]
        print(f'계좌번호 : {self.account_num}')  # 모의 : 8065597211
        
        
    def detail_acc_info(self): 
        
        self.dynamicCall(f"SetInputValue(String,String)","계좌번호",self.account_num)
        self.dynamicCall(f"SetInputValue(String,String)",'비밀번호','0000')
        self.dynamicCall(f"SetInputValue(String,String)",'비밀번호입력매체구분','00')
        self.dynamicCall(f"SetInputValue(String,String)",'조회구분','2')
        
        self.dynamicCall(f"CommRqData(String,String,int,String)",'예수금상세현황요청','OPW00001', '0', self.screen_my_info)
        self.detail_acc_info_event_loop = QEventLoop()
        self.detail_acc_info_event_loop.exec_()
        
        
    def account_eval(self,sPrevNext="0"):
        self.dynamicCall(f"SetInputValue(String,String)","계좌번호",self.account_num)
        self.dynamicCall(f"SetInputValue(String,String)",'비밀번호','0000')
        self.dynamicCall(f"SetInputValue(String,String)",'비밀번호입력매체구분','00')
        self.dynamicCall(f"SetInputValue(String,String)",'조회구분','1')
        
        self.dynamicCall(f"CommRqData(String,String,int,String)",'계좌평가잔고내역','OPW00018', sPrevNext, self.screen_my_info)
        
        
        if sPrevNext == '0':
            self.account_eval_event_loop = QEventLoop()
            self.account_eval_event_loop.exec_()
            
    
    def michaegul(self):
        self.dynamicCall(f"SetInputValue(String,String)","계좌번호",self.account_num)
        self.dynamicCall(f"SetInputValue(String,String)",'전체종목구분','0')
        self.dynamicCall(f"SetInputValue(String,String)",'매매구분','0')
        # self.dynamicCall(f"SetInputValue(String,String)",'종목코드','')
        self.dynamicCall(f"SetInputValue(String,String)",'체결구분','1')  # 1 : 체결 , 0 : 미체결
        
        self.dynamicCall(f"CommRqData(String,String,int,String)",'미체결요청','OPT10075', '0', self.screen_my_info)
        
        self.michaegul_event_loop = QEventLoop()
        self.michaegul_event_loop.exec_()
        
            
    def get_code_list_by_market(self,market_code):  # 마켓에 있는 종목코드 반환 
        '''
          0 : 코스피
          10 : 코스닥
          3 : ELW
          8 : ETF
          50 : KONEX
          4 :  뮤추얼펀드
          5 : 신주인수권
          6 : 리츠
          9 : 하이얼펀드
          30 : K-OTC
        '''
        
        code_list = self.dynamicCall("GetCodeListByMarket(QString)",market_code)
        code_list = code_list.split(';')[:-1]
        
        return code_list
    
    

    def screen_number_set(self):
        
        screen_overwrite = []
        
        # 계좌평가잔고내역에 있는 종목들 
        for code in self.jango_dict.keys():
            if code not in screen_overwrite : 
                screen_overwrite.append(code)
    
        # 미체결에 있는 종목들 
        for order_no in self.michaegul_dict.keys():
            code = self.michaegul_dict[order_no]['종목코드']
            if code not in screen_overwrite : 
                screen_overwrite.append(code)
                
        for code in self.top_volume_dict.keys():
            if code not in screen_overwrite : 
                screen_overwrite.append(code)
                
        # 포트폴리오에 있는 종목들 
        for code in self.portfolio_stock_dict.keys():
            if code not in screen_overwrite : 
                screen_overwrite.append(code)
        
        # 스크린번호 할당  
        '''
        스크린 번호는 200개 생성 가능 
        스크린 하나에는 100개의 요청을 할 수 있음
        '''
        
        
        cnt = 0 
        for code in screen_overwrite : 
            temp_screen = int(self.screen_real_stock)
            meme_screen = int(self.screen_meme_stock)
            hoga_screen = int(self.screen_hoga_stock)
            
            if (cnt % 50) == 0 : # 스크린 하나당 50개의 종목 할당
                temp_screen += 1   
                meme_screen += 1
                
                self.screen_real_stock = str(temp_screen)
                self.screen_meme_stock = str(meme_screen)
                
                

            hoga_screen += 1
            self.screen_hoga_stock = str(hoga_screen)

            if code in self.portfolio_stock_dict.keys():
                self.portfolio_stock_dict[code].update({'스크린번호':str(self.screen_real_stock)})
                self.portfolio_stock_dict[code].update({'주문용스크린번호':str(self.screen_meme_stock)})
                self.portfolio_stock_dict[code].update({'호가스크린번호':str(self.screen_hoga_stock)})
                
            elif code not in self.portfolio_stock_dict.keys(): 
                self.portfolio_stock_dict.update({code : {'스크린번호':str(self.screen_real_stock),'주문용스크린번호':str(self.screen_meme_stock),'호가스크린번호':str(self.screen_hoga_stock)}})

            
            cnt += 1
            

    def trdata_slot(self,sScrNo,sRQName,sTrCode,sRecordName,sPrevNext):
        """     
            BSTR sScrNo,       // 화면번호
            BSTR sRQName,      // 사용자 구분명
            BSTR sTrCode,      // TR이름
            BSTR sRecordName,  // 레코드 이름 ( 사용안함 )
            BSTR sPrevNext,    // 연속조회 유무를 판단하는 값 0: 연속(추가조회)데이터 없음, 2:연속(추가조회) 데이터 있음
        """
        ####################################################################################################
        if sRQName == "주식분봉차트조회요청":
            
            day_limit = '20231201'
            
            
            
            code = self.dynamicCall("GetCommData(String,String,int,String)",sTrCode,sRQName,0,'종목코드')
            code = code.strip()
            
            rows = self.dynamicCall("GetRepeatCnt(QString,QString)",sTrCode,sRQName)
            
            for i in range(rows):
                close = self.dynamicCall("GetCommData(QString,QString,int,QString)",sTrCode,sRQName,i,'현재가')
                volume = self.dynamicCall("GetCommData(QString,QString,int,QString)",sTrCode,sRQName,i,'거래량')
                minute_time = self.dynamicCall("GetCommData(QString,QString,int,QString)",sTrCode,sRQName,i,'체결시간')
                open = self.dynamicCall("GetCommData(QString,QString,int,QString)",sTrCode,sRQName,i,'시가')
                high = self.dynamicCall("GetCommData(QString,QString,int,QString)",sTrCode,sRQName,i,'고가')
                low = self.dynamicCall("GetCommData(QString,QString,int,QString)",sTrCode,sRQName,i,'저가')
                
                close = abs(int(close))
                volume = abs(int(volume))
                minute_time = minute_time.strip()
                open = abs(int(open))
                high = abs(int(high))
                low = abs(int(low))

                
                self.test[minute_time] = {
                    'close' : close,
                    'open' : open,
                    'high' : high,
                    'low'  : low,
                    'volume' : volume 
                }
                
                
                
                if day_limit in minute_time : 
                    print(minute_time)
                    # print(list(self.test)[-1])
                    self.get_minute_event_loop.exit()
                    sPrevNext = 0 
                    print('hello')
                    break
            print(list(self.test)[-1])    
            # print(code)
            
            if sPrevNext == '2' : 
                time.sleep(0.33)
                # QTest.qWait(3300)
                self.get_minute(code, sPrevNext=sPrevNext)
            
                
        
        
        
        ####################################################################################################
        
        
        if sRQName == "예수금상세현황요청":
            deposit = self.dynamicCall("GetCommData(String,String,int,String)",sTrCode,sRQName,0,'예수금')
            draw_deposit = self.dynamicCall("GetCommData(String,String,int,String)",sTrCode,sRQName,0,'출금가능금액')
            
            
            deposit = int(deposit)
            draw_deposit = int(draw_deposit)
            
            print(f"예수금 : {deposit}")
            print(f'출금가능금액 : {draw_deposit}')
            
            self.detail_acc_info_event_loop.exit()
            
        ####################################################################################################
        
        if sRQName == "계좌평가잔고내역":
            total_return = self.dynamicCall("GetCommData(String,String,int,String)",sTrCode,sRQName,0,'총수익률(%)') 
            total_return = float(total_return)

            
            print(f"계좌평가잔고내역 총수익률 : {total_return}%")
            
            rows = self.dynamicCall("GetRepeatCnt(QString,QString)",sTrCode,sRQName)

            cnt = 0 
            for i in range(rows):
                code = self.dynamicCall("GetCommData(QString,QString,int,QString)",sTrCode,sRQName,i,'종목번호')
                code_name = self.dynamicCall("GetCommData(QString,QString,int,QString)",sTrCode,sRQName,i,'종목명')
                stock_quantity = self.dynamicCall("GetCommData(QString,QString,int,QString)",sTrCode,sRQName,i,'보유수량')
                buy_price = self.dynamicCall("GetCommData(QString,QString,int,QString)",sTrCode,sRQName,i,'매입가')
                earn_rate = self.dynamicCall("GetCommData(QString,QString,int,QString)",sTrCode,sRQName,i,'수익률(%)')
                current_price = self.dynamicCall("GetCommData(QString,QString,int,QString)",sTrCode,sRQName,i,'현재가')
                total_buy_amount = self.dynamicCall("GetCommData(QString,QString,int,QString)",sTrCode,sRQName,i,'매입금액')
                possible_quantity = self.dynamicCall("GetCommData(QString,QString,int,QString)",sTrCode,sRQName,i,'매매가능수량')
                
                
                code = code.strip()[1:]
                code_name = code_name.strip()
                stock_quantity = int(stock_quantity)
                buy_price = int(buy_price)
                earn_rate = float(earn_rate.strip())
                current_price = int(current_price)
                total_buy_amount = int(total_buy_amount)
                possible_quantity = int(possible_quantity)

                self.jango_dict[code] = {}
                
                self.jango_dict[code].update({"종목명":code_name})
                self.jango_dict[code].update({"보유수량":stock_quantity})
                self.jango_dict[code].update({"매입가":buy_price})
                self.jango_dict[code].update({"수익률(%)":earn_rate})
                self.jango_dict[code].update({"현재가":current_price})
                self.jango_dict[code].update({"매입금액":total_buy_amount})
                self.jango_dict[code].update({"매매가능수량":possible_quantity})

                
                cnt += 1
            
            
            if sPrevNext == '2' : 
                self.account_eval(sPrevNext='2')
            
            else : 
              
                print(f'계좌 보유 종목 개수 : {len(self.jango_dict)}')
                print(self.jango_dict)

                self.account_eval_event_loop.exit()
                
                
        if sRQName == '미체결요청':
            
            rows = self.dynamicCall("GetRepeatCnt(QString,QString)",sTrCode,sRQName)
            
            for i in range(rows):
                
                code = self.dynamicCall("GetCommData(String,String,int,String)",sTrCode,sRQName,i,'종목코드') 
                code_name = self.dynamicCall("GetCommData(String,String,int,String)",sTrCode,sRQName,i,'종목명')       
                order_no = self.dynamicCall("GetCommData(String,String,int,String)",sTrCode,sRQName,i,'주문번호')  
                status = self.dynamicCall("GetCommData(String,String,int,String)",sTrCode,sRQName,i,'주문상태')   # 접수, 확인 ,체결
                quantity = self.dynamicCall("GetCommData(String,String,int,String)",sTrCode,sRQName,i,'주문수량') 
                price = self.dynamicCall("GetCommData(String,String,int,String)",sTrCode,sRQName,i,'주문가격') 
                order = self.dynamicCall("GetCommData(String,String,int,String)",sTrCode,sRQName,i,'주문구분')   # 매수, 매도
                michaegul_num = self.dynamicCall("GetCommData(String,String,int,String)",sTrCode,sRQName,i,'미체결수량') 
                chagul_num = self.dynamicCall("GetCommData(String,String,int,String)",sTrCode,sRQName,i,'체결량') 
                
                code = code.strip()
                code_name = code_name.strip()
                order_no = int(order_no)
                status = status.strip()
                quantity = int(quantity)
                price = int(price)
                order = order.strip().lstrip('+').lstrip('-')
                michaegul_num = int(michaegul_num)
                chagul_num = int(chagul_num)
 
                if order_no  in self.michaegul_dict:
                    pass
                else : 
                    self.michaegul_dict[order_no] = {}
                    
                    
                _michaegul_dict = self.michaegul_dict[order_no]
                
                _michaegul_dict.update({'종목코드':code})
                _michaegul_dict.update({'종목명':code_name})
                _michaegul_dict.update({'주문번호':order_no})
                _michaegul_dict.update({'주문상태':status})
                _michaegul_dict.update({'주문수량':quantity})
                _michaegul_dict.update({'주문가격':price})
                _michaegul_dict.update({'주문구분':order})
                _michaegul_dict.update({'미체결수량':michaegul_num})
                _michaegul_dict.update({'체결량':chagul_num})
                    
            print(self.michaegul_dict)
            print(f'미체결 종목 개수 : {len(self.michaegul_dict)}')

            
            self.michaegul_event_loop.exit()
            
            
        if sRQName == '당일거래량상위요청':
            
            rows = self.dynamicCall("GetRepeatCnt(QString,QString)",sTrCode,sRQName)
            for i in range(rows):
                code = self.dynamicCall("GetCommData(QString,QString,int,QString)",sTrCode,sRQName,i,'종목코드')
                code_name = self.dynamicCall("GetCommData(QString,QString,int,QString)",sTrCode,sRQName,i,'종목명')
                fluctuation = self.dynamicCall("GetCommData(QString,QString,int,QString)",sTrCode,sRQName,i,'등락률')
                volume = self.dynamicCall("GetCommData(QString,QString,int,QString)",sTrCode,sRQName,i,'거래량')
                amount = self.dynamicCall("GetCommData(QString,QString,int,QString)",sTrCode,sRQName,i,'거래금액')

                code = code.strip()
                code_name = code_name.strip()
                fluctuation = float(fluctuation)
                volume = int(volume)
                amount = int(amount)

   

                self.top_volume_dict[code] = {}
                
                self.top_volume_dict[code].update({"종목명":code_name})
                self.top_volume_dict[code].update({"등락률":fluctuation})
                self.top_volume_dict[code].update({"거래량":volume})
                self.top_volume_dict[code].update({"거래금액":amount})

            
            print(f'거래량상위 100 : {len(self.top_volume_dict)}')
            print(self.top_volume_dict)
            self.top_trading_volume_event_loop.exit()       

            

            
                
    
    def real_data_slot(self,sCode,sRealType,sRealData):
        '''
            BSTR sCode,        // 종목코드
            BSTR sRealType,    // 실시간타입
            BSTR sRealData    // 실시간 데이터 전문 (사용불가)
        '''
        if  self.setting_done == False : 
            pass
        else :
            if sRealType == '장시작시간':
                fid = self.realType.REALTYPE[sRealType]['장운영구분']
                value = self.dynamicCall("GetCommRealData(QString,int)",sCode,fid)
        
                if value == '0':
                    print('장 시작 전')
                    
                elif value == '3':
                    print('장 시작')
                    
                elif value == '2': 
                    print('장 종료, 동시호가로 넘어갑니다.')
                    print('프로그램을 종료합니다...')
                    
                elif value == '4':
                    print('3시30분 장 종료')
            


                


        
            elif sRealType == '주식체결':
    
                time_fid = self.realType.REALTYPE[sRealType]['체결시간']                                                               # 3
                current_price_fid = self.realType.REALTYPE[sRealType]['현재가']                                                        # 2
                # com_prev_day_fid = self.realType.REALTYPE[sRealType]['전일대비']
                # fluctuation_fid = self.realType.REALTYPE[sRealType]['등락율']
                # best_selling_price_fid = self.realType.REALTYPE[sRealType]['(최우선)매도호가']   # 호가창에서 매도쪽 첫부분 
                # best_buying_price_fid = self.realType.REALTYPE[sRealType]['(최우선)매수호가']    # 호가창에서 매수쪽 첫부분 
                volume_fid = self.realType.REALTYPE[sRealType]['거래량']                        # 틱봉의 거래량 (확실치않음)             # 1
                # cum_volume_fid = self.realType.REALTYPE[sRealType]['누적거래량']  
                # high_fid = self.realType.REALTYPE[sRealType]['고가']  
                # open_fid = self.realType.REALTYPE[sRealType]['시가']  
                # low_fid = self.realType.REALTYPE[sRealType]['저가']  
                
                
                time_tick_raw = self.dynamicCall("GetCommRealData(QString,int)",sCode,time_fid)                             # HHMMSS
                current_price_raw = self.dynamicCall("GetCommRealData(QString,int)",sCode,current_price_fid)                # +(-) 2500
                # com_prev_day = self.dynamicCall("GetCommRealData(QString,int)",sCode,com_prev_day_fid)                  # +(-) 50
                # fluctuation = self.dynamicCall("GetCommRealData(QString,int)",sCode,fluctuation_fid)                    # +(-) 12.98
                # best_selling_price = self.dynamicCall("GetCommRealData(QString,int)",sCode,best_selling_price_fid)      # +(-) 2500
                # best_buying_price = self.dynamicCall("GetCommRealData(QString,int)",sCode,best_buying_price_fid)        # +(-) 2500
                volume_raw = self.dynamicCall("GetCommRealData(QString,int)",sCode,volume_fid)                              # +(-) 120000
                # cum_volume = self.dynamicCall("GetCommRealData(QString,int)",sCode,cum_volume_fid)                      # +(-) 39933000
                # high = self.dynamicCall("GetCommRealData(QString,int)",sCode,high_fid)                                  # +(-) 2500
                # open = self.dynamicCall("GetCommRealData(QString,int)",sCode,open_fid)                                  # +(-) 2500
                # low = self.dynamicCall("GetCommRealData(QString,int)",sCode,low_fid)                                    # +(-) 2500
                
                time_tick = time_tick_raw                                   # 체결시간
                current_price = abs(int(current_price_raw))                 # 현재가
                # com_prev_day = abs(int(com_prev_day))                   # 전일대비
                # fluctuation = float(fluctuation)                        # 등락율
                # best_selling_price = abs(int(best_selling_price))       # 최우선매도호가
                # best_buying_price = abs(int(best_buying_price))         # 최우선매수호가
                volume = abs(int(volume_raw))                               # 거래량
                # cum_volume = abs(int(cum_volume))                       # 누적거래량
                # high = abs(int(high))                                   # 고가                
                # open = abs(int(open))                                   # 시가
                # low = abs(int(low))                                     # 저가
                
                
                if sCode not in self.portfolio_stock_dict:
                    self.portfolio_stock_dict.update({sCode:{}})
                    
                self.portfolio_stock_dict[sCode].update({'체결시간':time_tick})
                self.portfolio_stock_dict[sCode].update({'현재가':current_price})
                # self.portfolio_stock_dict[sCode].update({'전일대비':com_prev_day})
                # self.portfolio_stock_dict[sCode].update({'등락율':fluctuation})
                # self.portfolio_stock_dict[sCode].update({'(최우선)매도호가':best_selling_price})
                # self.portfolio_stock_dict[sCode].update({'(최우선)매수호가':best_buying_price})
                self.portfolio_stock_dict[sCode].update({'거래량':volume})
                # self.portfolio_stock_dict[sCode].update({'누적거래량':cum_volume})
                # self.portfolio_stock_dict[sCode].update({'고가':high})
                # self.portfolio_stock_dict[sCode].update({'시가':open})
                # self.portfolio_stock_dict[sCode].update({'저가':low})
                
                query_to_insert = ['체결시간','현재가','거래량']
                values_to_insert = [time_tick_raw,current_price_raw,volume_raw]


                current_time = datetime.now().strftime("%Y%m%d %H:%M:%S.%f")
                code_dict = {sCode: {'체결시간': current_time, '현재가':current_price_raw,'거래량': volume_raw}}
                # print(code_dict)
                self.batch_data_chaegul.append(code_dict)
                if len(self.batch_data_chaegul) >= self.BATCH_SIZE_CHAEGUL:
                    print('Send data to chaegul db worker...')
                    self.send_data_to_server_chaegul(self.batch_data_chaegul)
                    self.batch_data_chaegul = []
                    


                
                
                ############ 주식호가잔량
            elif sRealType == "주식호가잔량":
                
                time_fid = self.realType.REALTYPE['주식호가잔량']['호가시간']
                hoga_time = self.dynamicCall("GetCommRealData(QString,int)",sCode,time_fid)  
                current_time = datetime.now().strftime("%Y%m%d %H:%M:%S.%f")
                
                values_to_insert = [hoga_time]
                query_to_insert = ['호가시간']
                
                hoga_dict = {sCode : {'호가시간':current_time }}

                for hoga_type in ['매도호가', '매수호가']:
                    for i in range(10):
                        if hoga_type == '매도호가':
                            i = abs(i - 10 )
                        else : 
                            i += 1
                            
                        price = f'{hoga_type}{i}'
                        quantity = f'{hoga_type}수량{i}'
                        comparison = f'{hoga_type}직전대비{i}'

                        price_fid = self.realType.REALTYPE['주식호가잔량'][price]
                        quantity_fid = self.realType.REALTYPE['주식호가잔량'][quantity]
                        comparison_fid = self.realType.REALTYPE['주식호가잔량'][comparison]



                        price_real = self.dynamicCall("GetCommRealData(QString,int)", sCode, price_fid)
                        quantity_real = self.dynamicCall("GetCommRealData(QString,int)", sCode, quantity_fid)
                        comparison_real = self.dynamicCall("GetCommRealData(QString,int)", sCode, comparison_fid)
                        
                        
                        
                        hoga_dict[sCode].update({price:price_real})
                        hoga_dict[sCode].update({quantity:quantity_real})
                        hoga_dict[sCode].update({comparison:comparison_real})
                        
                self.batch_data_hoga.append(hoga_dict)
                if len(self.batch_data_hoga) >= self.BATCH_SIZE_HOGA:
                    print('Send data to hoga db worker...')
                    self.send_data_to_server_hoga(self.batch_data_hoga)
                    self.batch_data_hoga = []
                
                        
                #         values_to_insert.extend([price_real, quantity_real, comparison_real])
                #         query_to_insert.extend([price,quantity,comparison])
                #         # print(price_real)
                #         # print(quantity_real)
                #         # print(comparison_real)
                        
                
                # self.does_table_exist_hoga(sCode)
                # sql_query = f'INSERT INTO `{sCode}` (' + ', '.join([f'`{col}`' for col in query_to_insert]) + ') VALUES (' + ', '.join(['%s'] * len(values_to_insert)) + ')'
                
                # self.cursor_hoga.execute(sql_query, values_to_insert)
                # self.conn_hoga.commit()
                
                # print(sCode)
                
                # 계좌평가잔고내역에 있고 오늘 산 잔고에는 없을 경우 매도 
                # if sCode in self.account_stock_dict.keys() and sCode not in self.jango_dict.keys():
                #     print(f'계좌평가 잔고내역에서 신규매도를 한다. {sCode}')
                #     account_stock = self.account_stock_dict[sCode]
                    
                #     # meme_rate = (current_price - account_stock['매입가']) / account_stock['매입가'] * 100
                    
                    
                #     # if account_stock['매매가능수량'] > 0 and (meme_rate > 5 or meme_rate < -5):
                        
                #     order_success = self.send_order(order = '신규매도', sCode=sCode, quantity=account_stock['매매가능수량'])
                    
                #     # 주문전달 성공
                #     if order_success == 0 :
                #         print('주문 전달 성공')
                #         del self.account_stock_dict[sCode]    # 이건 너무 간단한 식임.. 고려하샘 
        
                #     # 주문전달 실패 
                #     else : 
                #         print('주문 전달 실패')
                

                # # 오늘 산 잔고에 종목에 있을 경우 매도
                # if sCode in self.jango_dict.keys():
                    
                #     jan_dict = self.jango_dict[sCode]
                #     # meme_rate = (current_price - jan_dict['매입단가']) / jan_dict['매입단가'] * 100
                    
                #     # if jan_dict['주문가능수량'] > 0 and (meme_rate > 5 or meme_rate < -5):
                #     #     print(f'잔고에서 신규매도 {sCode}')
                #     order_success = self.send_order(order = '신규매도', sCode=sCode, quantity=jan_dict['보유수량'])
                    
                #     # 주문전달 성공
                #     if order_success == 0 :
                #         print('주문 전달 성공')
        
                #     # 주문전달 실패 
                #     else : 
                #         print('주문 전달 실패')
                
                # 등락률이 2.0% 이상이고 오늘 산 잔고에 없을 경우 신규매수
                # elif fluctuation > 2.0 and sCode not in self.jango_dict.keys():
                #     print(f'신규매수를 한다. {sCode}')
                #     money = 1000000
                #     buy_quan = int(money / current_price)
                #     order_success = self.send_order(order = '신규매수', sCode=sCode, quantity=buy_quan)
                    
                #                     # 주문전달 성공
                #     if order_success == 0 :
                #         print('주문 전달 성공')
        
                #     # 주문전달 실패 
                #     else : 
                #         print('주문 전달 실패')
                
                
                # 미체결된 종목들 처리 
                
                # michaegul_list = list(self.michaegul_dict)  # list로 감싸기 때문에 새로운 주소가 할당됨. 
                
                # for order_num in michaegul_list:
                    
                #     code = self.michaegul_dict[order_num]['종목코드']
                #     order_price = self.michaegul_dict[order_num]['주문가격']
                #     michaegul_num = self.michaegul_dict[order_num]['미체결수량']
                #     order_gubun = self.michaegul_dict[order_num]['주문구분']
                
                
                #     if order_gubun == '매수' and michaegul_num > 0 and current_price > order_price : 
                #         print(f'미체결 수 : {michaegul_num}')
                #         order_success = self.send_order(order = '매수취소',sCode=code,quantity=0,order_number=order_num)  # 0 은 전량 취소
                #         if order_success == 0 :
                #             print('주문 전달 성공')
            
                #         # 주문전달 실패 
                #         else : 
                #             print('주문 전달 실패')
                
                    
                #     elif michaegul_num == 0 : 
                #         del self.michaegul_dict[order_num]
                        
        

          
    # 주문이 들어가면 ( send order ) 여기로 데이터가 반환됨
    def  chejan_slot(self,sGubun,nItemCnt,sFIdList):
        '''
          BSTR sGubun, // 체결구분. 접수와 체결시 '0'값, 국내주식 잔고변경은 '1'값, 파생잔고변경은 '4'
          LONG nItemCnt,
          BSTR sFIdList
        '''
        # 주문체결
        if int((sGubun)) == 0:
            jumun_chaegul = self.realType.REALTYPE['주문체결']
            
            account_num = self.dynamicCall("GetChejanData(int)",jumun_chaegul['계좌번호'])
            sCode = self.dynamicCall("GetChejanData(int)",jumun_chaegul['종목코드'])[1:]
            stock_name = self.dynamicCall("GetChejanData(int)",jumun_chaegul['종목명'])
            origin_order_no = self.dynamicCall("GetChejanData(int)",jumun_chaegul['원주문번호'])
            order_num = self.dynamicCall("GetChejanData(int)",jumun_chaegul['주문번호'])
            order_status = self.dynamicCall("GetChejanData(int)",jumun_chaegul['주문상태'])
            order_quan = self.dynamicCall("GetChejanData(int)",jumun_chaegul['주문수량'])
            order_price = self.dynamicCall("GetChejanData(int)",jumun_chaegul['주문가격'])
            michaegul_quan = self.dynamicCall("GetChejanData(int)",jumun_chaegul['미체결수량'])
            order_gubun = self.dynamicCall("GetChejanData(int)",jumun_chaegul['주문구분'])
            chaegul_time = self.dynamicCall("GetChejanData(int)",jumun_chaegul['주문/체결시간'])
            chaegul_price = self.dynamicCall("GetChejanData(int)",jumun_chaegul['체결가'])
            cheagul_quan = self.dynamicCall("GetChejanData(int)",jumun_chaegul['체결량'])
            current_price = self.dynamicCall("GetChejanData(int)",jumun_chaegul['현재가'])
            best_sell_price = self.dynamicCall("GetChejanData(int)",jumun_chaegul['(최우선)매도호가'])
            best_buy_price = self.dynamicCall("GetChejanData(int)",jumun_chaegul['(최우선)매수호가'])

            
            account_num = account_num                                         # 8065597211
            sCode = sCode                                                     # 005930
            stock_name = stock_name.strip()                                   # 삼성
            origin_order_no = origin_order_no                                 # 000000 
            order_num = order_num                                             # 0115061 ( 마지막 주문번호 )
            order_status = order_status                                       # 접수, 확인, 체결
            order_quan = int(order_quan)                                      # 245
            order_price = int(order_price)                                    # 75000
            michaegul_quan =int(michaegul_quan)                               # 180
            order_gubun = order_gubun.strip().lstrip('+').lstrip('-')         # +매수, -매도
            chaegul_time = chaegul_time                                       # 151028
            chaegul_price= chaegul_price                                      # 75000
            cheagul_quan = cheagul_quan                                       # 65
            current_price = abs(int(current_price))                           # - 750000
            best_sell_price = abs(int(best_sell_price))                       # - 751000
            best_buy_price = abs(int(best_buy_price))                         # - 750000
            
            
            if chaegul_price == '' : 
                chaegul_price = 0 
            else :  
                chaegul_price = int(chaegul_price)
                
            if cheagul_quan == '':
                cheagul_quan = 0 
            else : 
                cheagul_quan = int(cheagul_quan)
            
            if order_num not in self.michaegul_dict.keys():
                self.michaegul_dict.update({order_num:{}})
                self.michaegul_dict[order_num].update({'종목코드':sCode,
                                                       '주문번호':order_num,
                                                       '종목명':stock_name,
                                                       '주문상태':order_status,
                                                       '주문수량':order_quan,
                                                       '주문가격':order_price,
                                                       '미체결수량':michaegul_quan,
                                                       '원주문번호':origin_order_no,
                                                       '주문구분':order_gubun,
                                                       '주문/체결시간':chaegul_time,
                                                       '체결가':chaegul_price,
                                                       '체결량':cheagul_quan,
                                                       '현재가':current_price,
                                                       '(최우선)매도호가':best_sell_price,
                                                       '(최우선)매수호가':best_buy_price
                                                       })


                    
                    
                    
        # 잔고
        elif int(sGubun) == 1 : 
            jango = self.realType.REALTYPE['잔고']
            
            account_num = self.dynamicCall("GetChejanData(int)",jango['계좌번호'])
            sCode = self.dynamicCall("GetChejanData(int)",jango['종목코드'])[1:]
            stock_name = self.dynamicCall("GetChejanData(int)",jango['종목명'])
            current_price = self.dynamicCall("GetChejanData(int)",jango['현재가'])
            stock_quan = self.dynamicCall("GetChejanData(int)",jango['보유수량'])
            avail_quan = self.dynamicCall("GetChejanData(int)",jango['주문가능수량'])
            buy_price = self.dynamicCall("GetChejanData(int)",jango['매입단가'])
            total_buy_price = self.dynamicCall("GetChejanData(int)",jango['총매입가'])
            order_gubun = self.dynamicCall("GetChejanData(int)",jango['매도매수구분'])
            best_sell_price = self.dynamicCall("GetChejanData(int)",jango['(최우선)매도호가'])
            best_buy_price = self.dynamicCall("GetChejanData(int)",jango['(최우선)매수호가'])
            
            account_num = account_num
            sCode = sCode
            stock_name = stock_name.strip()
            current_price = abs(int(current_price))
            stock_quan = int(stock_quan)
            avail_quan = int(avail_quan)
            buy_price = abs(int(buy_price))
            total_buy_price = int(total_buy_price)
            meme_gubun = self.realType.REALTYPE['매도수구분'][order_gubun]
            best_sell_price = abs(int(best_sell_price))
            best_buy_price = abs(int(best_buy_price))
            
            if sCode not in self.jango_dict.keys():
                self.jango_dict.update({sCode:{}})
                self.jango_dict[sCode].update({
                    '현재가':current_price,
                    '종목코드':sCode,
                    '종목명':stock_name,
                    '보유수량':stock_quan,
                    '주문가능수량':avail_quan,
                    '매입단가':buy_price,
                    '매도매수구분':meme_gubun,
                    '(최우선)매도호가':best_sell_price,
                    '(최우선)매수호가':best_buy_price
                })
            
            if stock_quan == 0 :
                del self.jango_dict[sCode]
                self.dynamicCall("SetRealRemove(QString,QString)",self.portfolio_stock_dict[sCode]['스크린번호'],sCode)
            


 
            
    
    def send_order(self,order,sCode,quantity,order_number=''):
        '''
          BSTR sRQName, // 사용자 구분명
          BSTR sScreenNo, // 화면번호
          BSTR sAccNo,  // 계좌번호 10자리
          LONG nOrderType,  // 주문유형 1:신규매수, 2:신규매도 3:매수취소, 4:매도취소, 5:매수정정, 6:매도정정, 7:프로그램매매 매수, 8:프로그램매매 매도
          BSTR sCode, // 종목코드 (6자리)
          LONG nQty,  // 주문수량
          LONG nPrice, // 주문가격
          BSTR sHogaGb,   // 거래구분(혹은 호가구분)은 아래 참고
          BSTR sOrgOrderNo  // 원주문번호. 신규주문에는 공백 입력, 정정/취소시 입력합니다.
        '''
        
        state = None 
        
        if order == '신규매수' or order == '신규매도':
            state = 0 
        else : 
            state = 1
        
 
        gubun = {'신규매수': 1,
                 '신규매도': 2,
                 '매수취소': 3,
                 '매도취소': 4,
                 '매수정정': 5,
                 '매도정정': 6}
        
        gubun_num = gubun[order]
        
        
        order_success = self.dynamicCall("SendOrder(QString,QString,QString,int,QString,int,int,QString,QString)",
                                 [order,
                                 self.portfolio_stock_dict[sCode]['주문용스크린번호'],
                                 self.account_num,
                                 gubun_num,
                                 sCode,
                                 quantity,
                                 state,
                                 self.realType.SENDTYPE['거래구분']['시장가'],
                                 order_number])
        
        return order_success
            