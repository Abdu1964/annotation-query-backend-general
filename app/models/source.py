from pymongoose.mongo_types import Schema

class Source(Schema):
    schema_name = 'source'

    # Attributes
    id = None
    data_source = None

    def __init__(self, **kwargs):
        self.schema = {
            "data_source": any
        }
               
        super().__init__(self.schema_name, self.schema, kwargs)

    def __str__(self):
        return f"""data_source: {self.data_source}"""
