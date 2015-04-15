from django.db.models.base import Model
from django.utils.text import capfirst
import warnings
from functools import partial
from django.db.models.loading import get_model


class HookRegistry(object):
    instance = None
    hooks = {}

    @classmethod
    def registry(cls, name, hook):
        cls.hooks[name] = hook


def date_hook(value):
    return value

HookRegistry.registry('date', date_hook)
HookRegistry.registry('capfirst', capfirst)


class BaseValidator(object):
    """
    Base Node Validator.
    Contain basic exception and entry point for validation.
    Include total field tuple, required field tuple and field dependencies

    .. note: for validate options call `validate`
    """
    node_type = 'Node'
    fields = ()
    required = ()
    dependencies = ()

    query_divider = '.'

    @classmethod
    def field_broken_error(cls, field, reason=None, state_format=None):
        msg = '{node_type}: broken "{field}"'.format(
            node_type=cls.node_type,
            field=field
        )
        if reason:
            msg = ', because: '.join((msg, reason))
        if state_format:
            msg = ', needed: '.join((msg, state_format))
        raise ValueError(msg)

    @classmethod
    def field_required_error(cls, field, state_format=None):
        msg = '{node_type}: required "{field}"'.format(
            node_type=cls.node_type,
            field=field
        )
        if state_format:
            msg = ', for example: '.join((msg, state_format))
        raise ValueError(msg)

    @classmethod
    def field_dependencies_error(cls, field, dependence):
        msg = '{node_type}: field {field} require {dependence}'.format(
            node_type=cls.node_type, field=field, dependence=dependence
        )
        raise ValueError(msg)

    @classmethod
    def field_unknow_warn(cls, field):
        msg = '{field} not include in Validator.fields attribute'.format(
            field=field
        )
        warnings.warn(msg)

    @classmethod
    def find_broken_dependency(cls, options):
        fields = filter(lambda x: options.get(x[0]), cls.dependencies)
        for field, deps in fields:
            for dependence in filter(lambda x: not options.get(x), deps):
                cls.field_dependencies_error(field, dependence)

    @classmethod
    def run_validators(cls, options):
        for key, value in options.items()[:]:
            if key not in cls.fields:
                cls.field_unknow_warn(key)

            validator = getattr(cls, 'validate_{}'.format(key), None)
            if validator and value is not None:
                options[key] = validator(options)
        return options

    @classmethod
    def find_missing_required(cls, options):
        for key in filter(lambda x: x not in options, cls.required):
            cls.field_required_error(key)

    @classmethod
    def fill_empty_fields(cls, options):
        for key in filter(lambda x: x not in options, cls.fields):
            options[key] = None
        return options

    @classmethod
    def validate(cls, options):
        # find broken dependency
        cls.find_broken_dependency(options)
        cls.find_missing_required(options)

        options = cls.run_validators(options)
        options = cls.fill_empty_fields(options)

        return options


class BaseFieldValidator(BaseValidator):

    node_type = 'Model field'
    fields = ('query', 'model', 'field', 'hook')
    required = ('query', )
    dependencies = (
        ('model', ('field', )),
        ('field', ('model', ))
    )

    @classmethod
    def validate(cls, options):
        if isinstance(options, basestring):
            options = {'query': options}
        return super(BaseFieldValidator, cls).validate(options)

    @classmethod
    def validate_query(cls, options):
        query = None
        if isinstance(options, dict):
            query = options.get('query')
        elif isinstance(options, basestring):
            query = options

        if query is None:
            cls.field_broken_error('query',
                                   '{query} wrong'.format(query=options),
                                   'must be a string or dict with options')
        return query

    @classmethod
    def validate_model(cls, options):
        model = options.get('model')

        if model and isinstance(model, basestring):
            model = get_model(*model.split('.'))
            if model is None:
                cls.field_broken_error('model {model}'.format(model=model),
                                       '{} not found Model'.format(model),
                                       'app_label.model_name')

        elif not issubclass(model, Model):
            cls.field_broken_error(
                'model',
                '{model} is not model description '
                'or inherited from django.db.models.Model'.format(model=model),
                'app_label.model_name or '
                'class inherited from django.db.models.Model'
            )
        return model

    @classmethod
    def validate_field(cls, options):
        field = options.get('field')
        model = cls.validate_model(options)

        if field not in model._meta.get_all_field_names():
            cls.field_broken_error(
                'field',
                '{model} not contain field {field}'.format(
                    model=model,
                    field=field
                ),
                'field name, included in model'
            )

        return field

    @classmethod
    def validate_hook(cls, options):
        hook = options.get('hook')
        if callable(hook):
            hook = hook
        elif hook in HookRegistry.hooks:
            hook = HookRegistry.hooks[hook]
        else:
            cls.field_broken_error(
                'hook {hook}'.format(hook=hook),
                '{hook} not found in HookRegistry'.format(hook=hook),
                'call HookRegistry.registry({hook}, function)'
                .format(hook=hook)
            )
        return hook


class BaseManyToManyValidator(BaseFieldValidator):

    node_type = 'Many to Many field parser'
    fields = ('query', 'model', 'field', 'through', 'left_field',
              'right_field', 'hook', 'fields')
    required = ('query', 'model', 'field')
    dependencies = (
        ('model', ('field', )),
        ('field', ('model', )),
        ('left_field', ('through', )),
        ('right_field', ('through', )),
        ('through', ('left_field', 'right_field')),
        ('fields', ('through', ))
    )
    plain_validator_cls = BaseValidator

    @classmethod
    def validate_through(cls, options):
        through = options.get('through')
        if isinstance(through, basestring):
            through = get_model(*through.split('.'))
            if not through:
                cls.field_broken_error(
                    'through',
                    '{through} not found'.format(through=through),
                    'app_label.model_name'
                )
        elif not issubclass(through, Model):
            cls.field_broken_error(
                'through',
                '{through} is not isntance of django.db.models.Model'.format(
                    through=through,
                    type=type(through)
                ),
                'app_label.model_name'
            )
        return through

    @classmethod
    def validate_fields(cls, options):
        validated = []
        fields = options.get('fields') or []
        for field, options in fields.items():
            validated.append(cls.plain_validator_cls.validate(options))
        return fields


class BaseFieldParser(object):
    validator = BaseFieldValidator

    class ParseMultipleData(Exception):
        def __init__(self, model, name, source, query):
            self.model = model
            self.name = name
            self.source = source
            self.query = query

        def __unicode__(self):
            return u'model: {model}\nfield: {field}\n' \
                   u'{query}: multiple data found \n' \
                   u'source: {source}'.format(query=self.query,
                                              source=self.source)

    class ParseNotFound(Exception):
        def __init__(self, model, name, source, query):
            self.model = model
            self.name = name
            self.source = source
            self.query = query

        def __unicode__(self):
            return u'model: {model}\nfield: {field}\n' \
                   u'{query}: data not found\n' \
                   u'source: {source}'.format(query=self.query,
                                              source=self.source)

    def __init__(self, model, name, options):
        self.model = model
        self.name = name

        options = self.validator.validate(options)
        self.query = options['query']
        self.hook = options['hook']
        self.rel_to = options['model']
        self.rel_to_field = options['field']

    def get_raw_value(self, raw_data, query):
        raise NotImplementedError

    def process_raw_data(self, raw_data, query):
        value = self.get_raw_value(raw_data, query=query)

        if not value or (hasattr(value, '__iter__') and not len(value)):
            raise self.ParseNotFound(self.model, self.name, raw_data, query)

        elif hasattr(value, '__iter__') and len(value) > 1:
            raise self.ParseMultipleData(self.model, self.name,
                                         raw_data, query)

        return value[0] if hasattr(value, '__iter__') else value

    @staticmethod
    def _get_foreign_value(value, model, field):
        inst, created = model.objects.get_or_create(**{field: value})
        return inst

    def parse(self, raw_data):
        value = self.process_raw_data(raw_data, query=self.query)
        if self.hook:
            value = self.hook(value)
        if self.rel_to and self.rel_to_field:
            value = self._get_foreign_value(value,
                                            model=self.rel_to,
                                            field=self.rel_to_field)
        return value

    def __unicode__(self):
        return u'{model}->{field}'.format(model=self.model, field=self.name)


class BaseManyToManyParseField(BaseFieldParser):
    validator = BaseManyToManyValidator
    field_parser_cls = BaseFieldParser

    def __init__(self, model, name, options):
        self.name = name
        self.left_model = model

        options = self.validator.validate(options)
        self.query = options['query']
        self.hook = options['hook']
        self.right_model = options['model']
        self.right_model_field = options['field']
        self.through_model = options['through']
        self.through_fields = options['fields']
        self.left_field = options['left_field']
        self.right_field = options['right_field']

    def get_raw_value(self, raw_data, query):
        raise NotImplementedError

    def parse(self, raw_data):
        value = self.process_raw_data(raw_data, query=self.query)
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
                             self.through_fields.keys(),
                             self.through_fields.values())

            data = {field.name: field.parse(raw_data) for field in fields}
            return self.through_model(**data)

    def __unicode__(self):
        if self.through_model is None:
            return u'{left} <-> {right}'.format(left=self.left_model,
                                                right=self.right_model)
        return u'{left} <- {through} -> {right}'.format(
            left=self.left_model,
            right=self.right_model,
            through=self.through_model
        )


class BaseModelParser(object):

    field_parser_cls = BaseFieldParser
    field_parser_m2m_cls = BaseManyToManyParseField

    def __init__(self, model, options):
        self.model = self.validate_model(model)

        options = self.validate(options)
        self.query = options['query']
        self.fields = self.make_fields(self.model, options['fields'])
        self.fields_m2m = self.make_fields_m2m(self.model,
                                               options['fields_m2m'])

    @classmethod
    def make_fields(cls, model, fields):
        return map(partial(cls.field_parser_cls, model),
                   fields.keys(),
                   fields.values())

    @classmethod
    def make_fields_m2m(cls, model, fields):
        return map(partial(cls.field_parser_m2m_cls, model),
                   fields.keys(),
                   fields.values())

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
        for raw_data in self.get_source_iterator(source, self.query):
            self.model.objects.get_or_create(**self.get_item_data(raw_data))

    def parse_m2m(self, source):
        for raw_data in self.get_source_iterator(source, query=self.query):
            left_instances = self.model.objects.filter(
                **self.get_item_data(raw_data=source)
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

    def get_source_iterator(self, source, query):
        raise NotImplementedError

    def get_item_data(self, raw_data):
        return {field.name: field.parse(raw_data) for field in self.fields}


class BaseMapperBackend(object):
    parser_cls = BaseModelParser

    def __init__(self):
        self.source = None
        self.parsers = None

        self.errors = 0
        self.readed = 0
        self.loaded = 0

    def load(self, file_name, options):
        """
        :param file_name: full name of source file
        :type file_name: basestring
        :param options: parsing info grouped by model, for example
        ['mapper.Event': {  # app_label.model_name
                            # for model description
            'query': 'channel.events',  # query to instance data
                                        # build through divider '.'
            'fields': {    # plain fields description
                           # contain model_field: query in simple case
                           # contain model_field: dict in other case
                'title': 'title',   # simple case
                'organizer': {  # more complex case
                                # must contain query key
                    'query': 'organizer',
                    'model': 'mapper.Organizer',  # if specify model
                                                  # field parser be
                                                  # found instance
                                                  # of this model
                    'field': 'title',   # required if models specify
                                        # set field for
                                        # search in model
                    'hook': 'hook'  # set hook name apply for value
                                    # hook must be registry in
                                    # HookRegistry
                },
            },
            'rels': {  # set M2M relations
                       # model describe as key for options
                       # set as 'left_field' for model describe
                       # in 'rels' section
                       # model describe in this section
                       # named 'right_field'
                'places': {
                    'query': 'place', # query to instance data
                                      # build through divider '.'
                    'model': 'mapper.Place', # required
                    'field': 'event_set',    # required
                    'through': 'mapper.EventDate',
                    'left_field': 'event',
                    'right_field': 'place',
                    'hook': 'capfirst',
                    'fields': {
                        'date': {
                            'query': 'date',
                            'hook': 'date'
                        }
                    }
                }
            }
        },
        ...]
        :type options: dict
        :return:
        """
        self.source = self.load_source(file_name)
        self.parsers = self.load_parsers(options)

        for parser in self.parsers:
            parser.parse(self.source)

        for parser in self.parsers:
            parser.parse_m2m(self.source)

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
        for model, parser_options in options.iteritems():
            parser = self.parser_cls(model, parser_options)
            parsers.append(parser)
        return parsers