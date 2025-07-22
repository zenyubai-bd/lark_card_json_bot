import os

import lark_oapi as lark
import json
import pandas as pd

import lark_oapi as lark
from lark_oapi.api.im.v1 import *
from lark_oapi.api.application.v6 import *
from lark_oapi.api.sheets.v3 import *
from lark_oapi.api.cardkit.v1 import *
from lark_oapi.event.callback.model.p2_card_action_trigger import (
    P2CardActionTrigger,
    P2CardActionTriggerResponse,
)

from data_maker import *

## card template
WELCOME_CARD_ID = "AAqIDFlCvKu1K"
REC_CARD_ID = "AAqIBt7attlUN"
CONFIRMATION_CARD_ID = "AAqIDFlDc03wX"


################-------Data----------####################
def get_excel_data():
    # 构造请求对象
    request: GetSpreadsheetSheetRequest = GetSpreadsheetSheetRequest.builder() \
        .spreadsheet_token("PVTwsdlYRh8ecQtgG0CllyPugsb") \
        .sheet_id("lmDD1O") \
        .build()

    # 发起请求
    option = lark.RequestOption.builder().user_access_token("u-jyYK1vWQd50WKvzBIb02X44k2tvxkkyPoa20h0ME09ro").build()
    response: GetSpreadsheetSheetResponse = client.sheets.v3.spreadsheet_sheet.get(request, option)

    # 处理失败返回
    if not response.success():
        lark.logger.error(
            f"client.sheets.v3.spreadsheet_sheet.get failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}, resp: \n{json.dumps(json.loads(response.raw.content), indent=4, ensure_ascii=False)}")
        return

    # 处理业务结果
    lark.logger.info(lark.JSON.marshal(response.data, indent=4))
    return


################-------Message-------####################


def send_message(receive_id_type, receive_id, msg_type, content):
    request = (
        CreateMessageRequest.builder()
        .receive_id_type(receive_id_type)
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

def send_welcome_card(open_id):
    content = json.dumps(
        {
            "type": "template",
            "data": {
                "template_id": WELCOME_CARD_ID,
                "template_variable": {"open_id": open_id},
            },
        }
    )
    return send_message("open_id", open_id, "interactive", content)

def send_confirmation_card(open_id):
    content = json.dumps(
        {
            "type": "template",
            "data": {
                "template_id": CONFIRMATION_CARD_ID
            },
        }
    ) 
    return send_message("open_id", open_id, "interactive", content)

def send_rec_card(open_id):
    
    template_variables = get_json()

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

    content = json.dumps(
        {
            "type": "template",
            "data": {
                "template_id": REC_CARD_ID,
                "template_variable": template_variables,
            },
        }
    )

    return send_message("open_id", open_id, "interactive", content)

def update_done_card():
    request: UpdateCardRequest = UpdateCardRequest.builder() \
        .request_body(UpdateCardRequestBody.builder()
            .card(Card.builder()
                .type("card_json")
                .data("{\"body\":{\"elements\":[{\"content\":\"截至今日，项目完成度已达80%\",\"tag\":\"markdown\"}]},\"header\":{\"title\":{\"content\":\"项目进度更新提醒\",\"tag\":\"plain_text\"}},\"schema\":\"2.0\"}")
                .build())
            .uuid("a0d69e20-1dd1-458b-k525-dfeca4015204")
            .sequence(1)
            .build()) \
        .build()

    # 发起请求
    response: UpdateCardResponse = client.cardkit.v1.card.update(request)
    return


def do_p2_im_chat_access_event_bot_p2p_chat_entered_v1(
    data: P2ImChatAccessEventBotP2pChatEnteredV1,
) -> None:
    # open_id = data.event.operator_id.open_id
    # send_welcome_card(open_id)
    return

def do_p2_application_bot_menu_v6(data: P2ApplicationBotMenuV6) -> None:
    print(f"[ onP2BotMenuV6 access ], data: {data}")
    open_id = data.event.operator.operator_id.open_id
    event_key = data.event.event_key

    # 通过菜单 event_key 区分不同菜单。 你可以在开发者后台配置菜单的event_key
    # Use event_key to distinguish different menus. You can configure the event_key of the menu in the developer console.
    if event_key == "send_card":
        send_rec_card(open_id)
    elif event_key == "pop_video":
        send_confirmation_card(open_id)


def do_p2_im_message_receive_v1(data: lark.im.v1.P2ImMessageReceiveV1) -> None:
    data = json.loads(lark.JSON.marshal(data, indent=4))
    sender_open_id = data["event"]["sender"]["sender_id"]["open_id"]
    chat_id = data["event"]["message"]["message_id"]

def do_p2_card_action_trigger(data: P2CardActionTrigger) -> P2CardActionTriggerResponse:
    open_id = data.event.operator.open_id
    action = data.event.action
    # 通过 action 区分不同按钮， 你可以在卡片搭建工具配置按钮的action。此处处理用户点击了告警卡片中的已处理按钮
    # Use action to distinguish different buttons. You can configure the action of the button in the card building tool.
    # Here, handle the scenario where the user clicks the "Mark as resolved" button on the alarm card.
    if action.value["action"] == "send_card":
        notes = ""
        send_rec_card(open_id)
        return P2CardActionTriggerResponse({})

    elif action.value["action"] == "resend_card":
        row_num = data.event.action.form_value["vid_num"]
        dislike_videos(row_num)
        send_rec_card(open_id)
        return

    elif action.value["action"] == "done":
        update_done_card()
        return

    else:
        return P2CardActionTriggerResponse({})



def do_message_event(data: lark.CustomizedEvent) -> None:
    return
    print(f'[ do_customized_event access ], type: message, data: {lark.JSON.marshal(data, indent=4)}')

## Bot Client Initialization
event_handler = lark.EventDispatcherHandler.builder("", "") \
    .register_p2_im_message_receive_v1(do_p2_im_message_receive_v1) \
    .register_p2_application_bot_menu_v6(do_p2_application_bot_menu_v6) \
    .register_p2_im_chat_access_event_bot_p2p_chat_entered_v1(do_p2_im_chat_access_event_bot_p2p_chat_entered_v1) \
    .register_p2_card_action_trigger(do_p2_card_action_trigger)\
    .build()

client = lark.Client.builder().app_id("cli_a8e1d9f91abb900c").app_secret("jglESE0cQ2CpSUCPQitQhu0MUiUmVAOm").build()
wsClient = lark.ws.Client(
    "cli_a8e1d9f91abb900c",
    "jglESE0cQ2CpSUCPQitQhu0MUiUmVAOm",
    event_handler=event_handler,
    log_level=lark.LogLevel.DEBUG,
)

def main():
    wsClient.start()

if __name__ == "__main__":
    main()