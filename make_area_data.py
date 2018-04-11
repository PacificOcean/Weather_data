# -*- coding: utf-8 -*-
#
# discription: 気象庁で公開されている気象台・観測所の情報を取得する
# arguments:
#   argvs[1]: 出力先ディレクトリ
# output:
#   area_data_all.pickle:
#     全ての気象台・観測所の情報を持つpickleファイル
#   area_data_temp_valid.pickle:
#     気温が観測されていて、かつ、現在有効な気象台・観測所のpickleファイル
#   取得項目
#   "pref", "area", "proc_no", "block_no", "緯度_経度", "標高",
#   "ObservatoryType", "雨", "風", "気温", "日射", "雪", "観測終了日"

# --基本モジュール--
import pandas as pd
import os
import sys

import time
import dateutil.parser  # 変数の時間型への変換で使用

import urllib.request
from bs4 import BeautifulSoup
import re
import pickle


# ログ用
import traceback
from logging import getLogger, StreamHandler, FileHandler, INFO, WARN
import datetime
cmd = "make_area_data"
pid = str(os.getpid())
logfile = "/tmp/"+cmd+"_"+pid+".log"
logger = getLogger(cmd)
Fhandler = FileHandler(logfile)
Fhandler.setLevel(INFO)
logger.addHandler(Fhandler)
Shandler = StreamHandler()
Shandler.setLevel(WARN)
logger.addHandler(Shandler)
logger.setLevel(INFO)


# 変数定義
url_com = "http://www.data.jma.go.jp/obd/stats/etrn/select/"
url = url_com + "/prefecture00.php?prec_no=&block_no=&year=&month=&day=&view="
url1 = url_com + "/prefecture.php?prec_no="
url2 = "&block_no=&year=&month=&day=&view="
key = "onmouseover"


# 関数定義
def datetime_parser(x):
    return dateutil.parser.parse(x)


# main処理
if __name__ == '__main__':

    # 引数取得
    argvs = sys.argv
    arg_str = ' '.join(map(str, argvs))

    # ログ関数生成
    def error_exit(code, msg):
        d = datetime.datetime.today()
        logger.error(d.strftime("%Y-%m-%d %H:%M:%S")+" ERROR "+cmd+" - "
                     + str(msg)+" command: "+arg_str)
        logfile2 = \
            "/var/log/"+cmd+"_"+d.strftime("%Y%m%d%H%M%S")+"_"+pid+".log"
        os.rename(logfile, logfile2)
        sys.exit(code)

    def warn_print(msg):
        d = datetime.datetime.today()
        logger.warn(d.strftime("%Y-%m-%d %H:%M:%S")+" WARN "+cmd+" - "
                    + str(msg)+" command: "+arg_str)

    def debug_print(msg):
        d = datetime.datetime.today()
        logger.info(d.strftime("%Y-%m-%d %H:%M:%S")+" INFO "+cmd+" - "
                    + str(msg)+" command: "+arg_str)

    debug_print("start process.")

    # 引数チェック
    debug_print("start checking argments.")
    if len(argvs) <= 1:
        error_exit(1, "number of args is less than expected. [main]")

    out_dir = str(argvs[1])
    if not os.path.exists(out_dir):
        error_exit(1, "output directory does not exists. [main]")
    debug_print("end checking argments.")

    # URLからデータ取得
    debug_print("start scraping.")
    try:
        data = urllib.request.urlopen(url)
        soup = BeautifulSoup(data, 'html.parser')
    except:
        # Webページのスクレイピングに失敗した場合は、1度だけリトライ
        try:
            warn_print("failed scraping.")
            warn_print("retry scraping.")
            time.sleep(1)
            data = urllib.request.urlopen(url)
            soup = BeautifulSoup(data, 'html.parser')
        except:
            error_exit(2, "function error. trace: "
                       + traceback.format_exc()
                       + " [urllib.request.urlopen/BeautifulSoup]")
    debug_print("end scraping.")

    # データフレームへ変換
    debug_print("start making pref data.")
    try:
        # 地域名・地域コードのデータフレーム
        pref_DF = pd.DataFrame()
        for li in soup.find_all("area"):
            tmp1 = li.get("alt")
            tmp2 = str(re.split('prec_no=', str(li.get("href")))[1])
            tmp3 = re.split("&", tmp2)[0]
            tmp_DF = pd.DataFrame([tmp1, tmp3])
            pref_DF = pd.concat([pref_DF, tmp_DF.T])
        pref_DF = pref_DF.reset_index(drop=True)
    except:
        error_exit(2, "function error. trace: "
                   + traceback.format_exc() + " [main]")
    debug_print("end making pref data.")

    # 各観測所の情報を結合
    debug_print("start making detail data.")
    area_DF = pd.DataFrame()
    for i in range(len(pref_DF)):
        tmp_pref = pref_DF.ix[i, 0]
        tmp_prec_no = pref_DF.ix[i, 1]

        url = url1 + str(tmp_prec_no) + url2
        data = urllib.request.urlopen(url)
        soup2 = BeautifulSoup(data, 'html.parser')

        tmp_area_DF = pd.DataFrame()
        for li in soup2.find_all("area",):
            tmp_area = li.get("alt")
            tmp = str(re.split('block_no=', str(li.get("href")))[1])
            tmp_BN = re.split("&", tmp)[0]
            try:
                tmp_pos1 = int(re.split("\'\,\'", li.get(key))[4])
                tmp_pos2 = float(re.split("\'\,\'", li.get(key))[5])
                tmp_pos3 = int(re.split("\'\,\'", li.get(key))[6])
                tmp_pos4 = float(re.split("\'\,\'", li.get(key))[7])
                tmp_pos5 = float(re.split("\'\,\'", li.get(key))[8])
                tmp_type = re.split("\'\,\'", li.get(key))[0].split("\'")[1]
                tmp0 = re.split("\'\,\'", li.get(key))[9]
                tmp1 = re.split("\'\,\'", li.get(key))[10]
                tmp2 = re.split("\'\,\'", li.get(key))[11]
                tmp3 = re.split("\'\,\'", li.get(key))[12]
                tmp4 = re.split("\'\,\'", li.get(key))[13]
                tmp5 = re.split("\'\,\'", li.get(key))[14]
                tmp6 = re.split("\'\,\'", li.get(key))[15]
                tmp7 = re.split("\'\,\'", li.get(key))[16]
            except:
                continue
            ido = tmp_pos1 + tmp_pos2/60.0
            keido = tmp_pos3 + tmp_pos4/60.0
            tmp_DF = pd.DataFrame([tmp_pref, tmp_area, tmp_prec_no, tmp_BN,
                                   (ido, keido), tmp_pos5, tmp_type, tmp0,
                                   tmp1, tmp2, tmp3, tmp4,
                                   str(tmp5)+"/"+str(tmp6)+"/"+str(tmp7)])
            tmp_area_DF = pd.concat([tmp_area_DF, tmp_DF.T])

        area_DF = pd.concat([area_DF, tmp_area_DF])

        # スクレイピング先のサーバに負荷をかけないように念のため
        time.sleep(1)
    debug_print("end making detail data.")

    # データ整形、加工
    debug_print("start processing data.")
    try:
        area_DF = area_DF.drop_duplicates()
        area_DF.columns = ["pref", "area", "proc_no", "block_no",
                           "緯度_経度", "標高", "ObservatoryType", "雨",
                           "風", "気温", "日射", "雪", "観測終了日"]
        area_DF.index = range(len(area_DF))

        area_DF2 = area_DF[area_DF["気温"] == "1"].copy()
        area_DF2 = area_DF2[area_DF2["観測終了日"] == "9999/99/99"]
        area_DF2.index = range(len(area_DF2))
    except:
        error_exit(2, "function error. trace: "
                   + traceback.format_exc() + " [main]")
    debug_print("end processing data.")

    # pickleファイルの出力
    debug_print('start output file.')
    try:
        out_file1 = out_dir + "/area_data_all.pickle"
        out_file2 = out_dir + "/area_data_temp_valid.pickle"

        with open(out_file1, 'wb') as f:
            pickle.dump(area_DF, f)
        with open(out_file2, 'wb') as f:
            pickle.dump(area_DF2[area_DF2.columns[0:7]], f)
    except:
        error_exit(2, "function error. trace: "
                   + traceback.format_exc() + " [pickle.dump]")

    debug_print('end output file.')
    debug_print("end process.")
    os.remove(logfile)

    sys.exit(0)
