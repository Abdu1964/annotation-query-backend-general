from app.models.source import Source


class SourceStorageService():
    def __init__(self):
        pass

    @staticmethod
    def save(user):
        data = Source(
            data_source=user["data_source"],
        )

        id = data.save()
        return id

    @staticmethod
    def get():
        data = Source.find({})
        return data[0]

    @staticmethod
    def upsert_by_id(data):
        try: 
            source = Source.find(query={}, one=False)
            if source:
                id = source[0]["_id"]
                Source.update({"_id": id}, {"$set": data}, many=False)
            else:
                Source(**data).save()
        except Exception as e:
            Source(**data).save()

    @staticmethod
    def delete(id):
        data = Source.delete({"_id": id})
        return data