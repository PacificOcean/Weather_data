# -*- coding: utf-8 -*-
#
# discription: 郵便番号と期間を指定して、最寄りの観測所から気象データ(1時間単
#              位)を取得する
# arguments:
#   argvs[1]: 郵便番号(数字7桁)
#   argvs[2]: モードフラグ("check"、"all"、列番号)
#             ※列番号は、0始まりで数えて、1以上の値を"カンマ区切り2つ"で指定
#               気象台における対象列番号,その他の観測所における対象列番号
#               2018/2現在、"0"列目が時間、"4"列目(気象台)/"2"列目(その他)が気温
#               気温データのみを取得したい場合は、4,2 とする
#   argvs[3]: 出力先ディレクトリのパス
#   argvs[4]: 開始日(YYYYMMDD) "check"モード時は不要
#   argvs[5]: 終了日(YYYYMMDD) "check"モード時は不要
#   argvs[6]: 読込開始行番号 "check"モード時は不要  ※2018/2現在は"2"で固定
# output:
#   - "check"モード指定時
#     ファイル名：郵便番号.csv
#     内容：最寄りの気象台の都道府県名、都市名（CSV）
#   - "all"モード指定時
#     ファイル名：都道府県名_都市名_開始日_終了日.csv
#     内容：全気象データ（CSV）
#   - 列番号を指定した場合
#     ファイル名：都道府県名_都市名_開始日_終了日.csv
#     内容：日時と指定した列番号のデータ（CSV）

# --基本モジュール--
import pandas as pd
import numpy as np
import os
import sys

import time
import dateutil.parser  # 変数の時間型への変換で使用
from dateutil.relativedelta import relativedelta

import urllib.request
import pickle

import xml.etree.ElementTree as ET
from math import sin, cos, acos, radians

# ログ用
import traceback
from logging import getLogger, StreamHandler, FileHandler, INFO, WARN
import datetime
cmd = "weather_get"
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
check_flag = True
earth_rad = 6378.137
url1_s = "http://www.data.jma.go.jp/obd/stats/etrn/view/hourly_s1.php?prec_no="
url1_a = "http://www.data.jma.go.jp/obd/stats/etrn/view/hourly_a1.php?prec_no="
url2 = "http://geoapi.heartrails.com/api/xml?method=searchByPostal&postal="
# 2018/2時点のカラム名
# # 気象台
temp_col0_s = ['時', '気圧(hPa)', '降水量(mm)', '気温(℃)', '露点温度(℃)',
               '蒸気圧(hPa)', '湿度(％)', '風向・風速(m/s)', '日照時間(h)',
               '全天日射量(MJ/㎡)', '雪(cm)', '天気', '雲量', '視程(km)',
               np.nan, np.nan, np.nan]
temp_col1_s = ['現地', '海面', '風速', '風向', '降雪', '積雪',
               np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan,
               np.nan, np.nan, np.nan]
template_s = pd.DataFrame([temp_col0_s, temp_col1_s])
temp_cols_s = len(temp_col0_s)
output_col_s = ["日時", "気圧hPa_現地", "気圧hPa_海面", "降水量mm",
                "気温℃", "露点温度℃", "蒸気圧hPa", "湿度％",
                "風速m／s", "風向", "日照時間h", "全天日射量MJ／㎡",
                "雪cm_降雪", "雪cm_積雪", "天気", "雲量", "視程km"]

# # その他の観測所
temp_col0_a = ['時', '降水量(mm)', '気温(℃)', '風速・風向(m/s)',
               '日照時間(h)', '雪(cm)', np.nan, np.nan]
temp_col1_a = ['風速', '風向', '降雪', '積雪',
               np.nan, np.nan, np.nan, np.nan]
template_a = pd.DataFrame([temp_col0_a, temp_col1_a])
temp_cols_a = len(temp_col0_a)
output_col_a = ["日時", "降水量mm", "気温℃", "風速m／s", "風向",
                "日照時間h", "雪cm_降雪", "雪cm_積雪"]


# 関数定義
def latlng_to_xyz(lat, lng):
    rlat, rlng = radians(lat), radians(lng)
    coslat = cos(rlat)
    return coslat*cos(rlng), coslat*sin(rlng), sin(rlat)


def dist_on_sphere(pos0, pos1, radious=earth_rad):
    xyz0, xyz1 = latlng_to_xyz(*pos0), latlng_to_xyz(*pos1)
    return acos(sum(x * y for x, y in zip(xyz0, xyz1)))*radious


def datetime_parser(x):
    return dateutil.parser.parse(x)


def datetime_parser_2(x):
    return dateutil.parser.parse(x) + relativedelta(days=1)


def del_symbol(x):
    '''
    try:
        ret_x = int(str(x).replace(" ]", "").replace(" )", ""))
    except:
        ret_x = str(x).replace(" ]", "").replace(" )", "")
    return ret_x
    '''
    return str(x).replace(" ]", "").replace(" )", "")


def Observatory_get_main(post_num):
    # 郵便番号から緯度経度の取得
    str_url2 = url2 + str(post_num)
    req = urllib.request.Request(str_url2)

    with urllib.request.urlopen(req) as response:
        XmlData = response.read()
        response.close()

    root = ET.fromstring(XmlData)
    tgt_x = root.findtext(".//x")
    tgt_y = root.findtext(".//y")

    # 緯度経度から最も近い観測所を見つける
    def dist_frm_tgt(val):
        tgt_place = float(tgt_y), float(tgt_x)
        return dist_on_sphere(tgt_place, val)

    # 気象台データ（オブジェクトファイル）の読み込み
    with open('area_data_temp_valid.pickle', 'rb') as f:
        area_DF = pickle.load(f)

    area_DF["dist"] = area_DF["緯度_経度"].apply(dist_frm_tgt)

    nearest_index = area_DF.sort_values("dist").head(1).index

    nearest_pref = area_DF.ix[nearest_index].pref.values[0]
    nearest_area = area_DF.ix[nearest_index].area.values[0]
    tgt_proc_no = area_DF.ix[nearest_index].proc_no.values[0]
    tgt_block_no = area_DF.ix[nearest_index].block_no.values[0]
    tgt_type = area_DF.ix[nearest_index].ObservatoryType.values[0]

    return nearest_pref, nearest_area, tgt_proc_no, tgt_block_no, tgt_type


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

    # 引数チェック
    debug_print("start process.")

    debug_print("start checking argments.")
    if len(argvs) <= 3:
        error_exit(1, "number of args is less than expected. [main]")

    try:
        post_num = str(argvs[1])
        mode_flag = str(argvs[2])
        out_dir = str(argvs[3])
        start_date = "20170101"
        end_date = "20170131"
        start_row = 2
        tgt_col = 1
        tgt_col_s = 1
        tgt_col_a = 1
        if mode_flag != "check":
            start_date = str(argvs[4])
            end_date = str(argvs[5])
            start_row = int(argvs[6])
            if mode_flag != "all":
                tgt_col_s = int(str(mode_flag).split(",")[0])
                tgt_col_a = int(str(mode_flag).split(",")[1])
    except:
        error_exit(2, "function error. trace: "
                   # + traceback.format_exc(sys.exc_info()[2])+" [str]")
                   + traceback.format_exc() + " [str/int]")

    # post_numが7桁の数字であることのチェック
    if len(post_num) != 7:
        error_exit(1, "post_num is incorrect. [main]")
    else:
        try:
            int(post_num)
        except:
            error_exit(1, "post_num is incorrect. [main]")

    # 出力先ディレクトリが存在することのチェック
    if not os.path.exists(out_dir):
        error_exit(1, "output directory does not exists. [main]")

    # 開始日、終了日の日付の形式が正しいこと
    try:
        tmp_datetime = datetime_parser(start_date)
    except:
        error_exit(1, "start_date is incorrect. [main]")
    try:
        end_datetime = datetime_parser(end_date)
    except:
        error_exit(1, "end_date is incorrect. [main]")

    # 読込開始行番号が1以上であることのチェック
    if start_row < 1:
        error_exit(1, "start_row is less than 1. [main]")

    # 列指定が1以上であることのチェック。0列目は時間なので収集対象外
    # if tgt_col < 1:
    #    error_exit(1, "target_col is less than 1. [main]")

    debug_print("end checking argments.")

    # 気象台データ（オブジェクトファイル）の読み込み確認
    make_area_data = 0
    try:
        with open('area_data_temp_valid.pickle', 'rb') as f:
            area_DF = pickle.load(f)
    except:
        # 存在しないまたは、pythonバージョンが異なる等の理由でエラーとなる場合
        warn_print("failed to read area_data_temp_valid.pickle")
        make_area_data = 1

    # 気象台データ（オブジェクトファイル）の生成
    if make_area_data == 1:
        try:
            warn_print("try (re)creating area_data_temp_valid.pickle")
            os.system("python make_area_data.py .")
        except:
            error_exit(2, "function error. trace: "
                       + traceback.format_exc() + " [make_area_data.py]")

    debug_print("start getting post_num data.")
    # 郵便番号データの取得
    try:
        tmp_post_num = post_num
        post_num1 = tmp_post_num[0:3]
        post_num2 = tmp_post_num[3:7]
        err_cnt = 0
    except:
        error_exit(2, "function error. trace: "
                   + traceback.format_exc() + " [main]")
    while True:
        try:
            nearest_pref, nearest_area, tgt_proc_no, tgt_block_no, tgt_type\
                = Observatory_get_main(tmp_post_num)
            break
        except:
            # 郵便番号データの取得に失敗した場合は、以下の様に番号を変えてretry
            # 1. 下4桁を全て"0"にする
            # 2. 上3桁を"1"ずつ減らしていく(最大20回失敗するまで繰り返す)
            err_cnt = err_cnt + 1
            warn_print("failed to get post num data: " + str(tmp_post_num))
            if (err_cnt >= 20) | (post_num1 == "000"):
                error_exit(2, "function error. trace: "
                           # + traceback.format_exc(sys.exc_info()[2])
                           + traceback.format_exc()
                           + " [Observatory_get_main]")

            if post_num2 == "0000":
                tmp_post_num = str(int(post_num1)-1).zfill(3) + str(post_num2)
                post_num1 = tmp_post_num[0:3]
            else:
                tmp_post_num = post_num1 + "0000"
                post_num2 = "0000"
            warn_print("retry getting post num data: " + str(tmp_post_num))
            time.sleep(1)

    debug_print("acquired post_num: " + tmp_post_num + ", nearest_pref: " +
                nearest_pref + ", nearest_area: " + nearest_area +
                ", tgt_proc_no: " + tgt_proc_no + ", tgt_block_no: " +
                tgt_block_no)
    debug_print("end getting post_num data.")

    # checkモードなら、郵便番号の県名、市町村名を出力して終了
    if mode_flag == "check":
        debug_print("start output file.")
        try:
            outDF = pd.DataFrame([tmp_post_num, nearest_pref, nearest_area])
            outDF.to_csv(out_dir + "/" + post_num + ".csv",
                         index=False, header=False)
        except:
            error_exit(2, "function error. trace: "
                       # + traceback.format_exc(sys.exc_info()[2])
                       + traceback.format_exc() + " [DataFrame/to_csv]")
        debug_print("end output file.")
        debug_print("end process.")
        os.remove(logfile)
        sys.exit(0)

    debug_print("start checking weather data.")
    try:
        # 最寄が気象台か、その他観測所かでurl、データ形式が異なるための、対応
        if tgt_type == "s":
            tgt_col = tgt_col_s
            url1 = url1_s
            temp_col0 = temp_col0_s
            temp_col1 = temp_col1_s
            template = template_s
            temp_cols = temp_cols_s
            output_col = output_col_s
        elif tgt_type == "a":
            tgt_col = tgt_col_a
            url1 = url1_a
            temp_col0 = temp_col0_a
            temp_col1 = temp_col1_a
            template = template_a
            temp_cols = temp_cols_a
            output_col = output_col_a
        else:
            error_exit(1, "internal error, ObservatoryType unexpected: "
                       + str(tgt_type)+". [main]")

        # 気象データ取得用URLの基本部分生成
        url_str = url1 + str(tgt_proc_no) + "&block_no="\
                       + str(tgt_block_no) + "&"

        # 最新の日付のページでデータ取得/形式チェックする
        tmp_url = url_str + 'year=' + str(end_datetime.year) + '&month='\
                          + str(end_datetime.month) + '&day='\
                          + str(end_datetime.day) + '&view=p1'
    except:
        error_exit(2, "function error. trace: "
                   + traceback.format_exc() + " [main]")

    # 結果ファイルが既に存在する場合はスキップ
    if mode_flag == "all":
        out_file = out_dir + "/" + nearest_pref + "_" + nearest_area \
                   + "_all_" + start_date + "_" + end_date + ".csv"
    else:
        out_file = out_dir + "/" + nearest_pref + "_" + nearest_area \
                   + "_" + str(tgt_col) + "_" + start_date + "_" + end_date \
                   + ".csv"

    if os.path.exists(out_file):
        warn_print("output file already exists.")
        debug_print("end process.")
        os.remove(logfile)
        sys.exit(0)

    try:
        table_datas = pd.io.html.read_html(tmp_url)
    except:
        # Webページのスクレイピングに失敗した場合は、1度だけリトライ
        try:
            warn_print("failed to get data of "+str(end_datetime)[0:10]+".")
            warn_print("retry getting data of "+str(end_datetime)[0:10]+".")
            time.sleep(1)
            table_datas = pd.io.html.read_html(tmp_url)
        except:
            error_exit(2, "function error. trace: "
                       # + traceback.format_exc(sys.exc_info()[2])
                       + traceback.format_exc() + " [io.html.read_html]")

    # 気象データの形式チェック
    try:
        # カラム数の一致チェック
        if len(table_datas[0].columns) != temp_cols:
            check_flag = False
            warn_print("number of columns is defferent from template. cols:"
                       + len(table_datas[0].columns))
        # カラムの要素の一致チェック
        elif start_row != len(template):
            check_flag = False
            warn_print("start_row is defferent from length of template."
                       + " header records:" + str(len(table_datas[0])))
        else:
            check_DF = (table_datas[0].head(start_row).fillna(0)
                        == template.fillna(0))
            if check_DF.astype(int).sum().sum() != temp_cols*start_row:
                warn_print("columns are defferent from template. template: "
                           + str(list(template.ix[0])) + ","
                           + str(list(template.ix[1])) + ", acquired: "
                           + str(list(table_datas[0].ix[0])) + ","
                           + str(list(table_datas[0].ix[1])))
                check_flag = False
    except:
        error_exit(2, "function error. trace: "
                   + traceback.format_exc() + " [main]")

    debug_print("end checking weather data.")

    debug_print("start getting weather data.")
    # 指定された期間の気象データを取得処理
    out_data = pd.DataFrame()
    tmp_datetime = tmp_datetime - relativedelta(days=1)  # 前日から取得
    while tmp_datetime <= end_datetime:
        try:
            tmp_year = tmp_datetime.year
            tmp_month = tmp_datetime.month
            tmp_day = tmp_datetime.day

            tmp_url = url_str + 'year=' + str(tmp_year) + '&month='\
                              + str(tmp_month) + '&day=' + str(tmp_day)\
                              + '&view=p1'
        except:
            error_exit(2, "function error. trace: "
                       + traceback.format_exc() + " [main]")
        try:
            table_datas = pd.io.html.read_html(tmp_url)
        except:
            # Webページのスクレイピングに失敗した場合は、1度だけリトライ
            try:
                warn_print("failed to get data of "
                           + str(end_datetime)[0:10] + ".")
                warn_print("retry getting data of "
                           + str(end_datetime)[0:10] + ".")
                time.sleep(1)
                table_datas = pd.io.html.read_html(tmp_url)
            except:
                error_exit(2, "function error. trace: "
                           # + traceback.format_exc(sys.exc_info()[2])
                           + traceback.format_exc() + " [io.html.read_html]")

        try:
            # データの開始行をstart_rowとし、列指定の場合はtgt_colを抽出
            if mode_flag == "all":
                tmp_data = table_datas[0].ix[start_row:, :].copy()
            else:
                tmp_data = table_datas[0].ix[start_row:, [0, tgt_col]].copy()

            # 日付項を追加  ※後で日時のデータにするため、ここで日時としておく
            col_list = ["日時"]
            col_list.extend(tmp_data.columns)
            tmp_data["日時"] = str(tmp_datetime)[0:10]
            tmp_data = tmp_data[col_list]

            # 結合して翌日へ
            out_data = pd.concat([out_data, tmp_data])
            tmp_datetime = tmp_datetime + relativedelta(days=1)

            debug_print("getting data of "+str(tmp_datetime)[0:10]+" done.")

            # スクレイピング先のサーバに負荷をかけないように1秒待つ
            time.sleep(1)
        except:
            error_exit(2, "function error. trace: "
                       + traceback.format_exc() + " [main]")

    debug_print("end getting weather data.")

    debug_print("start processing data.")
    # データ整形、加工
    try:
        # "24時"のレコードを翌日の"0時"にする
        out_data_24h = out_data[out_data[0] == "24"].copy()
        out_data_24h["日時"] = out_data_24h["日時"].apply(datetime_parser_2)
        out_data_24h[0] = "0"
        out_data_24h["日時"] = out_data_24h["日時"].astype(str)
        out_data = pd.concat([out_data, out_data_24h])
        out_data = out_data[out_data[0] != "24"].copy()

        # 指定した開始日と終了日の範囲を出力
        start_date_str = str(datetime_parser(start_date))[0:10]
        end_date_str = str(datetime_parser(end_date))[0:10]
        out_data = out_data[(out_data["日時"] >= start_date_str) &
                            (out_data["日時"] <= end_date_str)].copy()

        # 日付と時間を結合し、datetime型に変換
        out_data["日時"] =\
            out_data["日時"] + " " + out_data[0].astype(str) + ":00:00"
        out_data["日時"] = out_data["日時"].apply(datetime_parser)
        out_data = out_data.sort_values("日時")
        del out_data[0]

        # 記号の除去
        out_data = out_data.replace("--", 0)
        out_data = out_data.replace("0+", 0)
        out_data = out_data.replace("10-", 10)
        out_data = out_data.replace("×", np.nan)
        out_data = out_data.replace("///", np.nan)
        out_data = out_data.replace("#", np.nan)
        out_data = out_data.replace("*", np.nan)
        out_data = out_data.fillna("")
        for tmp_col in out_data.columns:
            out_data[tmp_col] = out_data[tmp_col].apply(del_symbol)
            # 上の処理で各データが文字列型となるため注意

        # "all"指定でかつ、テンプレートから変更が無い場合、カラム名付与
        if (mode_flag == "all") & (check_flag):
            out_data.columns = output_col
    except:
        error_exit(2, "function error. trace: "
                   + traceback.format_exc() + " [main]")

    debug_print("end processing data.")

    debug_print('start output file.')
    try:
        # out_file = out_dir + "/" + nearest_pref + "_" + nearest_area + "_" \
        #             + mode_flag + "_" + start_date + "_" + end_date + ".csv"
        if mode_flag == "all":
            out_file = out_dir + "/" + nearest_pref + "_" + nearest_area \
                       + "_all_" + start_date + "_" + end_date + ".csv"
        else:
            out_file = out_dir + "/" + nearest_pref + "_" + nearest_area \
                       + "_" + str(tgt_col) + "_" + start_date + "_" \
                       + end_date + ".csv"

        out_data.to_csv(out_file, index=False)
    except:
        error_exit(2, "function error. trace: "
                   # + traceback.format_exc(sys.exc_info()[2]) + " [to_csv]")
                   + traceback.format_exc() + " [to_csv]")

    debug_print('end output file.')
    debug_print("end process.")
    os.remove(logfile)

    sys.exit(0)
