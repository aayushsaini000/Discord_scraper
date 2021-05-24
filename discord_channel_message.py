import requests
from database import DBConnection
import traceback
import sys
import logging
from logging.handlers import RotatingFileHandler
import config


class DiscordChannelMessage:
    db_obj = DBConnection()

    def getChannelMessages(self):
        try:
            channel_id = config.CHANNEL_ID
            url = f"https://discord.com/api/v8/channels/{channel_id}/messages"
            last_msg = self.db_obj.getLastMsgId()
            headers={'Authorization': config.TOKEN}
            if last_msg:
                message_id = self.db_obj.getLastMsgId().get('message_id')
                # message_id = "835482010217742416666fgfdgfgfg"
                payload = {"after": message_id, "limit": 100}
                res = requests.get(url, params=payload, headers=headers)
            else:
                res = requests.get(url, headers=headers, params={"limit": 100})
            json_response = res.json()
            if res.status_code == 200:
                messages_list = json_response
                logger.info(f"Total messages found: {len(messages_list)}")
                if messages_list:
                    try:
                        for msg_dict in messages_list[::-1]:
                            row_dict = dict()
                            msg_id = msg_dict['id']
                            author_dict = msg_dict['author']
                            username = author_dict.get("username", None)
                            bot = author_dict.get("bot", None)
                            embeds_list = msg_dict['embeds']
                            if embeds_list:
                                for embed_dict in embeds_list:
                                    row_dict['title'] = embed_dict['title']
                                    fields = embed_dict['fields']
                                    for field_dict in fields:
                                        if "Country" in str(field_dict['name']):
                                            row_dict['Country'] = field_dict.get('value').replace("\n", " ")
                                        else:
                                            field_value = field_dict.get('value').replace("\n", " ")
                                            row_dict[str(field_dict['name'])] = field_value
                            
                            row_dict['message_id'] = msg_id
                            msg_timestamp = msg_dict['timestamp']
                            row_dict['timestamp'] = msg_timestamp
                            row_dict['username'] = username
                            row_dict['bot'] = bot
                            # db entry
                            self.db_obj.addNewMsg(row_dict)
                        # update last message_id and datetime in  discord message lookup
                        doc = self.db_obj.updateMessageLookup(msg_id, msg_timestamp)
                        logger.info(f"Last message updated in discord message lookup doc: {msg_id}")
                    except Exception:
                        logger.error("getChannelMessages error while looping over msgs lists", exc_info=True)
                        # fetch last insrted message 
                        last_inserted_msg = self.db_obj.getLastInsertedMessage()
                        if last_inserted_msg:
                            # update discord message lookup
                            message_id = last_inserted_msg.get('message_id')
                            msg_timestamp = last_inserted_msg.get('timestamp')
                            doc = self.db_obj.updateMessageLookup(message_id, msg_timestamp)
                            logger.info(f"Last message updated in discord message lookup doc: {msg_id}")
            else:
                logger.error(f"Discord getChannelMessages error response status code: {res.status_code}")
                logger.error(f"Discord getChannelMessages API error code: {json_response['code']}")
                logger.error(f"Discord getChannelMessages API error message: {json_response['message']}")
                errors_dict = json_response['errors']
                if errors_dict:
                    for error_type, error_msg_dict in errors_dict.items():
                        logger.error(f"Discord getChannelMessages API error type: {error_type}")
                        for dict_obj in error_msg_dict['_errors']:
                            error = f"""Discord getChannelMessages API error type {error_type} error code: {dict_obj['code']}, error message: {dict_obj['message']}"""
                            logger.error(error)
        except Exception as e:
            logger.error("Something else error occured.", exc_info=True)


def main():
    cls_instance = DiscordChannelMessage()
    cls_instance.getChannelMessages()


if __name__ == '__main__':
    rfh = logging.handlers.RotatingFileHandler(
        filename='discord_script.log', 
        mode='a',
        maxBytes=20*1024*1024,
        backupCount=2,
        encoding=None,
        delay=0
    )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)-2s {%(pathname)s:%(lineno)d} %(levelname)-4s %(message)s",
        datefmt="%y-%m-%d %H:%M:%S",
        handlers=[
            rfh
        ]
    )

    logger = logging.getLogger('main')
    main()
