# listen from chat bot csv message
# parse to json file
# send card message through template

import json
import lark_oapi as lark

## Import functions
from parse_env import *
from data_maker import *

import os

PATH = os.path.dirname(os.path.abspath(__file__))
APP_ID, APP_SECRET, TEMPLATE_ID = parse_env()

img_path = os.path.join(PATH, "img_folder")
txt_path = os.path.join(PATH, "output", "template.txt")

def get_response(response):
    """
    This function extracts the file key and message ID from the response JSON.
    It assumes the response is a JSON string containing an event with a message.
    """
    response = json.loads(response)
    message_dict = response["event"]["message"]

    content_str = message_dict["content"]
    prefix = '"file_key":"'
    start = content_str.find(prefix) + len(prefix)
    end = content_str.find('"', start)

    file_id = content_str[start:end]

    message_type = message_dict["message_type"]
    message_id = message_dict["message_id"]

    open_id = response["event"]["sender"]["sender_id"]["open_id"]

    return file_id, message_id, message_type, open_id

def send_message(receive_id, msg_type, content):
    client = lark.Client.builder() \
        .app_id(APP_ID) \
        .app_secret(APP_SECRET) \
        .log_level(lark.LogLevel.DEBUG) \
        .build()
    request = (
        CreateMessageRequest.builder()
        .receive_id_type("open_id")
        .request_body(
            CreateMessageRequestBody.builder()
            .receive_id(receive_id)
            .msg_type(msg_type)
            .content(content)
            .build()
        )
        .build()
    )
    # 使用发送OpenAPI发送通知卡片，你可以在API接口中打开 API 调试台，快速复制调用示例代码
    # Use send OpenAPI to send notice card. You can open the API debugging console in the API interface and quickly copy the sample code for API calls.
    # https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/reference/im-v1/message/create
    response = client.im.v1.message.create(request)
    if not response.success():
        raise Exception(
            f"client.im.v1.message.create failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}"
        )
    return response

def send_video_card(open_id, json_df):
    content = json.dumps(
        {
            "type": "template",
            "data": {
                "template_id": TEMPLATE_ID,
                "template_variable": json_df,
            },
        }
    )
    return send_message(open_id, "interactive", content)

def do_p2_im_message_receive_v1(data: lark.im.v1.P2ImMessageReceiveV1) -> None:
    # handle received P2P Message
    print(f'[ do_p2_im_message_receive_v1 access ], data: {lark.JSON.marshal(data, indent=4)}')
    response = lark.JSON.marshal(data, indent=4)

    file_id, message_id, message_type, open_id = get_response(response)


    if message_type == "file":
        file_path = download_data(message_id, file_id)
        df = pd.read_csv(file_path)
        cleaned_df = clean_data(df)

        json_template = get_json(cleaned_df)

        send_video_card(open_id, json_template)


# 注册事件 Register event
event_handler = lark.EventDispatcherHandler.builder("", "") \
    .register_p2_im_message_receive_v1(do_p2_im_message_receive_v1) \
    .build()

def main():
    # 构建 client Build client
    cli = lark.ws.Client(APP_ID, APP_SECRET,
                        event_handler=event_handler, log_level=lark.LogLevel.DEBUG)

    # 建立长连接 Establish persistent connection
    cli.start()

if __name__ == "__main__":
    main()