from kiwoom_code.kiwoom import Kiwoom
from PyQt5.QtWidgets import * 
import sys 

class UI_class():
    def __init__(self):
        print("UI class")
        
        self.app = QApplication(sys.argv)  # 초기값을 잡아줌.
        
        
        
        self.kiwoom = Kiwoom()
        
        
        self.app.exec_()