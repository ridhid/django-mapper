from functools import partial
from django.db.models.loading import get_model


class BaseFieldParser(object):

    def __init__(self, model, name, schema):
        self.model = model
        self.name = name

        options = self.validate(schema)
        self.query = options['query']
        self.hook = options['hook']
        self.rel_to = options['model']
        self.rel_to_field = options['field']

    @classmethod
    def validate_query(cls, query):
        if isinstance(query, dict):
            query = query.get('query')

        if query is None:
            raise ValueError('field must contain a "query" in description')
        return query

    @classmethod
    def validate_model(cls, schema):
        model = schema.get('model')
        field = schema.get('field')

        if model and field is None:
            raise ValueError('field must contain a "field" attr '
                             'if "model" has been specified')

        if field and model is None:
            raise ValueError('field must contain a "model" attr if '
                             '"field" has been specified')

        if model and isinstance(model, basestring):
            rel = get_model(model.split('.'))
            if rel is None:
                raise ValueError('model {model} not found'
                                 .format(model=model))
        else:
            raise ValueError('field "model" must contain information '
                             'about model as "app_label.model_name"')

        if field:
            if field not in model._meta.get_all_field_names():
                raise ValueError('model {model} not contain field {field}'
                                 .format(model=model, field=field))
        else:
            raise ValueError('field must contain a "field" field')

        return model, field

    @classmethod
    def validate_hook(cls, schema):
        hook = schema.get('hook')
        if hook in HookRegistry.hooks:
            return HookRegistry.hooks[hook]
        raise ValueError('hook {hook} not found'.format(hook=hook))

    @classmethod
    def validate(cls, schema):
        query = cls.validate_query(schema)
        if isinstance(schema, dict):
            hook = cls.validate_hook(schema)
            model, field = cls.validate_model(schema)
        else:
            model = field = hook = None

        return {'query': query,
                'hook': hook,
                'model': model,
                'field': field}

    def _get_raw_value(self, raw_data):
        raise NotImplementedError

    @staticmethod
    def _get_foreign_value(value, model, field):
        return model.get(**{field: value})

    def parse(self, raw_data):
        value = self._get_raw_value(raw_data)
        if self.hook:
            value = self.hook
        if self.rel_to and self.rel_to_field:
            value = self._get_foreign_value(value,
                                            model=self.rel_to,
                                            field=self.rel_to_field)
        return value


class BaseManyToManyParseField(BaseFieldParser):
    field_parser_cls = BaseFieldParser

    def __init__(self, model, name, schema):
        self.name = name
        self.left_model = model

        options = self.validate(schema)
        self.query = options['query']
        self.right_model = options['model']
        self.right_model_field = options['field']
        self.through_model = options['through']
        self.through_fields = options['through_fields']

    @classmethod
    def validate_through_model(cls, schema):
        through = schema.get('through')
        if through is not None:
            through = get_model(through.split('.'))
            if through is None:
                raise ValueError('model {model} not found'.
                                 format(model=through))

        fields = schema.get('fields')
        if fields and not through:
            raise ValueError('many to many relation description must contain'
                             '"through" if "fields" is specified')
        elif fields:
            through_all_fields = through._meta.get_all_field_names()
            for field in fields:
                if field not in through_all_fields:
                    raise ValueError('model {model} not contain: {field}'
                                     .format(model=through, field=field))
        return through, fields

    @classmethod
    def validate(cls, schema):
        query = cls.validate_query(schema)
        hook = cls.validate_hook(schema)

        model, field = cls.validate_model(schema)
        through, through_fields = cls.validate_through_model(schema)

        return {'query': query,
                'hook': hook,
                'model': model,
                'field': field,
                'through': through,
                'through_fields': through_fields}

    def _get_raw_value(self, raw_data):
        raise NotImplementedError

    def parse(self, raw_data):
        value = self._get_raw_value(raw_data)
        if self.hook:
            value = self.hook
        left_value = self._get_foreign_value(self.model, self.name)
        right_value = self._get_foreign_value(self.ri)
        return value


class BaseModelParser(object):
    DEFAULT_BULK_INSERT = 1000

    field_parser_cls = BaseFieldParser
    field_parser_m2m_cls = BaseManyToManyParseField

    def __init__(self, model, schema):
        self.model = self.validate_model(model)
        self.query, fields_info, fields_info_m2m = self.validate(schema)

        self.fields = self.make_fields(fields_info)
        self.fields_m2m = self.make_fields_m2m(fields_info_m2m)

    @classmethod
    def make_fields(cls, model, fields_info):
        return map(partial(cls.field_parser_cls, model),
                   zip(fields_info.items()))

    @classmethod
    def make_fields_m2m(cls, model, fields_info):
        return map(partial(cls.field_parser_m2m_cls, model),
                   zip(fields_info.items()))

    @staticmethod
    def validate_model(model_name):
        if not isinstance(model_name, basestring):
            raise ValueError('{key} must contain "app_label.model_name" '
                             'info'.format(key=model_name))

        model = get_model(*model_name.split('.'))
        if model is None:
            raise ValueError('{model} not found'.format(model=model_name))

        return model

    @staticmethod
    def validate(schema):
        query = schema.get('query')
        if query is None:
            raise ValueError('schema must contain "query" key')

        fields = schema.get('fields')
        if fields is None:
            raise ValueError('schema must contain "fields" key')
        else:
            if not isinstance(fields, dict):
                raise TypeError('{fields} must be a dict'.format(fields))

        fields_m2m = schema.get('rels', ())
        return query, fields, fields_m2m

    def parse(self, source):
        instances = []
        for i, raw_data in enumerate(self.get_item_source(source)):
            instance = self.model(**self.get_item_source(raw_data))
            instances.append(instance)

            if not i % self.DEFAULT_BULK_INSERT:
                self.model.objects.bulk_insert(instances)
                instances = []

    def parse_m2m(self, source):
        instances = []
        for i, raw_data in enumerate(self.get_item_source(source)):
            for field in self.fields_m2m:
                left_instance =

    def get_item_source(self, source):
        raise NotImplementedError

    def get_item_data(self, raw_data):
        return {field.name: field.parse(raw_data) for field in self.fields}


class BaseMapperBackend(object):
    source = None
    parsers = None
    parser_cls = BaseModelParser

    def __init__(self):
        self.errors = 0
        self.readed = 0
        self.loaded = 0

    def load(self, file_name, schemes):
        """
        :param file_name: full name of source file
        :type file_name: basestring
        :param schemes: parsing info grouped by model, for example
            {'app_label.model_name':
                {'query': 'node.path',
                 'fields': {'plain_field': 'node.path',
                            'plain_field': {'query': 'path',
                                            'hook': callable},
                 'rels': {
                    'm2m_field': {'query': 'node.path',
                                  'through': 'app_label.model_name'}}}

        :type schemes: dict
        :return:
        """
        self.source = self.load_source(file_name)
        self.parsers = self.load_parsers(schemes)

    def load_source(self, file_name):
        """
        :param file_name: full path name
        :type file_name: basestring
        """
        raise NotImplementedError

    def load_parsers(self, schemes):
        """
        :param schemes: schemes of mapping
        :type schemes: dict
        :return: schema
        """
        parsers = []
        for model, parsing_info in schemes.iteritems():
            parser = self.parser_cls(model, parsing_info)
            parsers.append(parser)
        return parsers


class HookRegistry(object):
    """
    .. note: implement Singleton pattern
    """
    instance = None
    hooks = {}

    def __new__(cls, *args, **kwargs):
        if cls.instance is None:
            cls.instance = super(HookRegistry, cls).__new__(*args, **kwargs)
        return cls.instance

    @classmethod
    def registry(cls, name, hook):
        cls.hooks[name] = hook