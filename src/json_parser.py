class FlattenJsonParser:
    def __init__(self, child_separator: str = '_', exclude_fields=None, flatten_lists=False, keys_to_ignore=None):
        self.child_separator = child_separator
        self.exclude_fields = exclude_fields
        self.flatten_lists = flatten_lists
        self.keys_to_ignore = keys_to_ignore
        if self.keys_to_ignore is None:
            self.keys_to_ignore = set()

    def parse_data(self, data):
        for i, row in enumerate(data):
            data[i] = self._flatten_row(row)
        return data

    def parse_row(self, row: dict):
        return self._flatten_row(row)

    @staticmethod
    def _construct_key(parent_key, separator, child_key):
        if parent_key:
            return "".join([parent_key, separator, child_key])
        else:
            return child_key

    def _flatten_row(self, nested_dict):
        if len(nested_dict) == 0:
            return {}

        flattened_dict = dict()

        def _flatten(dict_object, key_name=None, name_with_parent=''):
            if isinstance(dict_object, dict):
                for key in dict_object:
                    if key not in self.keys_to_ignore:
                        new_parent_name = self._construct_key(name_with_parent, self.child_separator, key)
                        _flatten(dict_object[key], key_name=key, name_with_parent=new_parent_name)
                    else:
                        flattened_dict[key] = dict_object[key]
            elif isinstance(dict_object, (list, set, tuple)):
                if self.flatten_lists:
                    for index, item in enumerate(dict_object):
                        new_key_name = self._construct_key(name_with_parent, self.child_separator, str(index))
                        _flatten(item, key_name=new_key_name)
                else:
                    flattened_dict[name_with_parent] = dict_object
            else:
                flattened_dict[name_with_parent] = dict_object

        _flatten(nested_dict, None)
        return flattened_dict
