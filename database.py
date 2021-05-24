from pymongo import MongoClient
import pymongo

class DBConnection:
    connection = MongoClient()
    db = connection['local']

    def getLastMsgId(self):
        collection = self.db['discord_message_lookup']
        res = collection.find_one()
        return res
    
    def addNewMsg(self, msg_dict):
        collection = self.db['discord_message']
        doc = collection.insert_one(msg_dict)
        return doc.inserted_id
    
    def updateMessageLookup(self, message_id, timestamp):
        newvalues = { "$set": { 'message_id': message_id , "timestamp": timestamp} }
        collection = self.db['discord_message_lookup']
        doc = collection.update({}, newvalues, upsert=True)
        return doc
    
    def getLastInsertedMessage(self):
        collection = self.db['discord_message']
        doc = collection.find_one(sort=[( '_id', pymongo.DESCENDING )])
        return doc
