@ECHO ON
start "Hoga Receiver" cmd /k "cd C:\Users\pari0\Desktop\kiwoom_data & call activate trade_bot_32bit & python db_worker_hoga.py"

start "Chaegul Receiver" cmd /k "cd C:\Users\pari0\Desktop\kiwoom_data & call activate trade_bot_32bit & python db_worker_chaegul.py"

start "Kiwoom Start" cmd /k "cd C:\Users\pari0\Desktop\kiwoom_data & call activate trade_bot_32bit & python __init__.py"
