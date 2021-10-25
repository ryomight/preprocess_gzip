import datetime
import os

import pandas as pd

#GlobalVariable

#storage
INPUT_BROB_STORAGE = "INPUTBROB"
OUTPUT_BROB_STORAGE = "OUTPUTBROB"

#filename
##NYUKIN_NAME = 'nyukin.gz'
NYUKIN_NAME = 'sample_submission.csv.gz'
SEIKYU_NAME = 'seikyu.gz'
SEIKYU_NAME = 'sample_submission.csv.gz'

#分割数
SPLIT_ROWS = 3000 #N数


def main():

    #格納先フォルダ名の作成
    # JSTとUTCの差分(日本日時指定)
    DIFF_JST_FROM_UTC = 9
    now = datetime.datetime.utcnow() + datetime.timedelta(hours=DIFF_JST_FROM_UTC)
    today = str(now.year) + str(now.month)+ str(now.day)
    folder_name = 'INPUT_' + today

    #nyukin
    if os.path.exists(INPUT_BROB_STORAGE+'/'+today+'/'+NYUKIN_NAME):

        nyukin_df = pd.read_csv(INPUT_BROB_STORAGE+'/'+today+'/'+NYUKIN_NAME)
	    
        ##不要列名削除
        ##preprocessor()
    
        ##ファイル分割数取得
        file_split_num = (len(nyukin_df)+SPLIT_ROWS-1) // SPLIT_ROWS

        #分割/出力
        for i in range(file_split_num):
            split_nyukin = nyukin_df.iloc[SPLIT_ROWS*i:SPLIT_ROWS*(i+1),:]
            split_nyukin.to_csv(OUTPUT_BROB_STORAGE+'/'+today+'/'+'nyukin_%s.csv' % i)
	
    else :
        #エラー処理入れること
        print('error')

    #seikyu
    if os.path.exists(INPUT_BROB_STORAGE+'/'+today+'/'+SEIKYU_NAME):
        ##gzip解凍
        seikyu_df = pd.read_csv(INPUT_BROB_STORAGE+'/'+today+'/'+SEIKYU_NAME)
        seikyu_df.to_csv(OUTPUT_BROB_STORAGE+'/'+today +'/'+'seikyu.csv')
        ##不要列名削除
        ##preprocessor()
    else :
        #エラー処理入れること
        pass

    #configfile出力
    f = open(OUTPUT_BROB_STORAGE+'/'+today +'/'+'config.txt', 'w')
    f.write(str(file_split_num))
    f.close()

    return 0

if __name__ == "__main__":
    main()
