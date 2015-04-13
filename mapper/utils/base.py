from functools import partial
from django.db.models.loading import get_model


class BaseFieldParser(object):

    def __init__(self, model, name, options):
        self.model = model
        self.name = name

        options = self.validate(options)
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
    def validate_model(cls, options):
        model = options.get('model')
        field = options.get('field')

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
    def validate_hook(cls, options):
        hook = options.get('hook')
        if hook in HookRegistry.hooks:
            return HookRegistry.hooks[hook]
        raise ValueError('hook {hook} not found'.format(hook=hook))

    @classmethod
    def validate(cls, options):
        query = cls.validate_query(options)
        if isinstance(options, dict):
            hook = cls.validate_hook(options)
            model, field = cls.validate_model(options)
        else:
            model = field = hook = None

        return {'query': query,
                'hook': hook,
                'model': model,
                'field': field}

    def _get_raw_value(self, raw_data, query):
        raise NotImplementedError

    @staticmethod
    def _get_foreign_value(value, model, field):
        return model.get(**{field: value})

    def parse(self, raw_data):
        value = self._get_raw_value(raw_data, query=self.query)
        if self.hook:
            value = self.hook
        if self.rel_to and self.rel_to_field:
            value = self._get_foreign_value(value,
                                            model=self.rel_to,
                                            field=self.rel_to_field)
        return value


class BaseManyToManyParseField(BaseFieldParser):
    field_parser_cls = BaseFieldParser

    def __init__(self, model, name, options):
        self.name = name
        self.left_model = model

        options = self.validate(options)
        self.query = options['query']
        self.right_model = options['model']
        self.right_model_field = options['field']
        self.through_model = options['through']
        self.through_fields = options['through_fields']
        self.left_field = options['left_field']
        self.right_field = options['right_field']

    @classmethod
    def validate_through_model(cls, options):
        through = options.get('through')
        through_all_fields = ()
        if through is not None:
            through = get_model(through.split('.'))
            if through is None:
                raise ValueError('model {model} not found'.
                                 format(model=through))
            through_all_fields = through._meta.get_all_field_names()

        left_field = options.get('left_field')
        right_field = options.get('right_field')

        if through and not left_field:
            raise ValueError('many to many relations description must contain'
                             '"left_field" field, if contain "through" field')
        if through and not right_field:
            raise ValueError('many to many relations description must contain'
                             '"right_field" field, if contain "through" field')

        if left_field not in through_all_fields:
            raise ValueError('model {model} not contain: {field}'
                             .format(field=left_field))

        if right_field not in through_all_fields:
            raise ValueError('model {model} not contain: {field}'
                             .format(field=right_field))

        fields = options.get('fields')
        if fields and not through:
            raise ValueError('many to many relation description must contain'
                             '"through" if "fields" is specified')
        elif fields:
            for field in fields:
                if field not in through_all_fields:
                    raise ValueError('model {model} not contain: {field}'
                                     .format(model=through, field=field))
        return {'model': through,
                'fields': fields,
                'left_field': left_field,
                'right_field': right_field}

    @classmethod
    def validate(cls, options):
        query = cls.validate_query(options)
        hook = cls.validate_hook(options)

        model, field = cls.validate_model(options)
        through_result = cls.validate_through_model(options)

        return {'query': query,
                'hook': hook,
                'model': model,
                'field': field,
                'left_field': through_result['left_field'],
                'right_field': through_result['right_field'],
                'through': through_result['model'],
                'through_fields': through_result['fields']}

    def _get_raw_value(self, raw_data, query):
        raise NotImplementedError

    def parse(self, raw_data):
        value = self._get_raw_value(raw_data, query=self.query)
        if self.hook:
            value = self.hook
        value = self._get_foreign_value(value,
                                        self.right_model,
                                        self.right_model_field)
        return value

    def get_through_instance(self, raw_data):
        if self.through_model:
            fields = ()
            if self.through_fields:
                fields = map(partial(self.field_parser_cls,
                                     self.through_model),
                             zip(self.through_fields.items()))

            data = {field.name: field.parse(raw_data) for field in fields}
            return self.through_model(**data)


class BaseModelParser(object):

    field_parser_cls = BaseFieldParser
    field_parser_m2m_cls = BaseManyToManyParseField

    def __init__(self, model, name, options):
        self.name = name
        self.model = self.validate_model(model)

        options = self.validate(options)
        self.query = options['query']
        self.fields = self.make_fields(self.model, options['fields'])
        self.fields_m2m = self.make_fields_m2m(self.model,
                                               options['fields_m2m'])

    @classmethod
    def make_fields(cls, model, fields):
        return map(partial(cls.field_parser_cls, model),
                   zip(fields.items()))

    @classmethod
    def make_fields_m2m(cls, model, fields):
        return map(partial(cls.field_parser_m2m_cls, model),
                   zip(fields.items()))

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
    def validate(options):
        query = options.get('query')
        if query is None:
            raise ValueError('options must contain "query" key')

        fields = options.get('fields')
        if fields is None:
            raise ValueError('options must contain "fields" key')
        else:
            if not isinstance(fields, dict):
                raise TypeError('{fields} must be a dict'.format(fields))

        fields_m2m = options.get('rels', ())
        return {'query': query,
                'fields': fields,
                'fields_m2m': fields_m2m}

    def parse(self, source):
        for raw_data in self.get_item_source(source):
            self.model.objects.get_or_create(**self.get_item_data(raw_data))

    def parse_m2m(self, source):
        for raw_data in self.get_item_source(source):
            left_instances = self.model.objects.filter(
                **self.get_item_source(source)
            )
            for left_instance in left_instances:
                for field in self.fields_m2m:
                    right_instance = field.parse(raw_data)
                    if field.through_model:
                        through = field.get_through_instance(raw_data)
                        setattr(through, field.left_field, left_instance)
                        setattr(through, field.right_field, right_instance)
                        through.save()
                    else:
                        right_manager = getattr(left_instance, field.field)
                        right_manager.add(right_instance)

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

    def load(self, file_name, options):
        """
        :param file_name: full name of source file
        :type file_name: basestring
        :param options: parsing info grouped by model, for example
            {'app_label.model_name':
                {'query': 'node.path',
                 'fields': {'plain_field': 'node.path',
                            'plain_field': {'query': 'path',
                                            'hook': callable},
                 'rels': {
                    'm2m_field': {'query': 'node.path',
                                  'through': 'app_label.model_name'}}}

        :type options: dict
        :return:
        """
        self.source = self.load_source(file_name)
        self.parsers = self.load_parsers(options)

        for parser in self.parsers:
            parser.parse()

        for parser in self.parsers:
            parser.parser_m2m()

    def load_source(self, file_name):
        """
        :param file_name: full path name
        :type file_name: basestring
        """
        raise NotImplementedError

    def load_parsers(self, options):
        """
        :param options: options of mapping
        :type options: dict
        :return: options
        """
        parsers = []
        for model, parsing_info in options.iteritems():
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