import pandas as pd
import json

import lark_oapi as lark
from lark_oapi.api.im.v1 import *

from parse_env import *
from get_product_img import *
from get_img_key import *

import pandas

import shutil

import os

PATH = os.path.dirname(os.path.abspath(__file__))
APP_ID, APP_SECRET, TEMPLATE_ID = parse_env()
# video_table = pd.read_csv("video_table.csv")
# video_table = video_table[video_table["In Product Table"]==1]

def download_data(message_id, file_id):
    """
    the funtion is used to down csv file from message
    into "input/video_table.csv"
    """
    client = lark.Client.builder() \
        .app_id(APP_ID) \
        .app_secret(APP_SECRET) \
        .log_level(lark.LogLevel.DEBUG) \
        .build()

    # 构造请求对象
    request: GetMessageResourceRequest = GetMessageResourceRequest.builder() \
        .message_id(message_id) \
        .file_key(file_id) \
        .type("file") \
        .build()

    # 发起请求
    response: GetMessageResourceResponse = client.im.v1.message_resource.get(request)

    # 处理失败返回
    if not response.success():
        lark.logger.error(
            f"client.im.v1.message_resource.get failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}, resp: \n{json.dumps(json.loads(response.raw.content), indent=4, ensure_ascii=False)}")
        return

    # 处理业务结果
    file_path = os.path.join(PATH, "test.csv")
    content = response.file.read()
    try:
        with open(file_path, 'wb') as file:
            file.write(content)
            file.close()
    except FileExistsError:
        print("failed")

    return file_path


def clean_data(df):
    """
    This function aims to clean the column names to prepare the transformation to json
    """
    df = df[["Product Name", "Video Link", "handle", "Product Category", "Product TTS Link"]]
    df = df.rename(columns={
        "Product Name":     "product_name",
        "Video Link":       "video_link",
        "handle":           "creator_handle",
        "Product Category": "product_category",
        "Product TTS Link": "product_link"
    })
    df.dropna(subset=["product_name"], inplace=True)

    # remove existing img_folder if it exists
    img_folder_path = "img_folder"
    shutil.rmtree(os.path.join(PATH, img_folder_path), ignore_errors=True)  # Clear existing images
    os.mkdir(os.path.join(PATH, img_folder_path))  # Create img_folder

    df["img_key"] = ""  # Initialize img_key column
    for index, row in df.iterrows():
        product_link = row["product_link"]
        product_name = row["product_name"]
        filename = os.path.join(PATH, img_folder_path, f"{index}.jpg")
        img_path, status = download_image(product_link, filename)
        img_key = {"img_key": get_img_key(img_path)}
        df.at[index, "img_key"] = img_key  # Add img_key to

        if status:
            os.remove(img_path) #delete image file after getting img_key

    return df

def dislike_videos(pop_row):
    """
    This function is used to delete the selected rows and regenerate json template for card generation
    Args:
        df: original dataframe
        pop_row: the row that want to be deleted
    """
    pop_row = [int(x)-1 for x in pop_row if int(x)<=5]
    video_table.drop(pop_row, inplace=True)
    video_table.reset_index(drop=True, inplace=True)
    return video_table

def get_json(video_table):
    """
    this function select top 5 videos based on video GMV and product GMV
    output: json template feeding to Feishu Card
    """
    data_list = video_table.to_dict(orient="records")
    # data_list = json.dumps(data_list, indent=2, ensure_ascii=False)
    df_selected = {"product_description": data_list}

    #template_variables  = json.dumps(df_selected, indent=4, ensure_ascii=False)
    # df_json = json.dumps(df_selected, indent=4, ensure_ascii=False)
    # template_variables = {
    #     "product_description":[
    #         {
    #         "product_name": "メモリカード拡張機能付きの翻訳ペン、134言語の双方向インターホン",
    #         "video_link": "https://www.tiktok.com/@gutzfam/video/7522478537056505096",
    #         "creator_handle": "@gutzfam",
    #         "product_category": "Phones & Electronics",
    #         "product_link": "https://shop.tiktok.com/view/product/1731812363594466741"
    #     },
    #     {
    #         "product_name": "ポータブルハンドヘルドターボファン、折りたたみ式パーソナルファン、ミニファン、夏の旅行用ポータブルファン",
    #         "video_link": "https://www.tiktok.com/@kami.labo/video/7523054894383729928",
    #         "creator_handle": "@kami.labo",
    #         "product_category": "Household Appliances",
    #         "product_link": "https://shop.tiktok.com/view/product/1731812363594466741"
    #     }
    #     ]
    #     }
    return df_selected
