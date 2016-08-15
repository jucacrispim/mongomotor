# from motor_extensions.py

"""MongoMotor specific extensions to Sphinx."""

import inspect

from docutils.nodes import list_item, paragraph, title_reference
from sphinx import addnodes
from sphinx.addnodes import (desc, desc_content, versionmodified,
                             desc_signature, seealso, pending_xref)
from sphinx.util.inspect import safe_getattr

import mongomotor


# This is a place to store info while parsing, to be used before generating.
mm_info = {}


def find_by_path(root, classes):
    if not classes:
        return [root]

    _class = classes[0]
    rv = []
    for child in root.children:
        if isinstance(child, _class):
            rv.extend(find_by_path(child, classes[1:]))

    return rv


def get_parameter_names(parameters_node):
    parameter_names = []
    for list_item_node in find_by_path(parameters_node, [list_item]):
        title_ref_nodes = find_by_path(
            list_item_node, [paragraph, (title_reference, pending_xref)])

        parameter_names.append(title_ref_nodes[0].astext())

    return parameter_names


def process_mongomotor_nodes(app, doctree):
    # Search doctree for MongoMotor's methods and attributes whose docstrings
    #  were copied from MongoEngine, and fix them up for MongoMotor:
    #   1. Remove all version annotations like "New in version 2.0" since
    #      PyMongo's version numbers are meaningless in Motor's docs.
    #   2. Remove "seealso" directives that reference PyMongo's docs.
    #
    # We do this here, rather than by registering a callback to Sphinx's
    # 'autodoc-process-signature' event, because it's way easier to handle the
    # parsed doctree before it's turned into HTML than it is to update the RST.

    for objnode in doctree.traverse(desc):
        if objnode['objtype'] in ('method', 'attribute', 'classmethod'):
            signature_node = find_by_path(objnode, [desc_signature])[0]
            name = '.'.join([
                signature_node['module'], signature_node['fullname']])

            assert name.startswith('mongomotor.')
            obj_mm_info = mm_info.get(name)
            if obj_mm_info:
                desc_content_node = find_by_path(objnode, [desc_content])[0]
                if (obj_mm_info.get('is_async_method') or
                        obj_mm_info.get('has_coroutine_annotation')):
                    coro_annotation = addnodes.desc_annotation(
                        'coroutine ', 'coroutine ',
                        classes=['coro-annotation'])
                    signature_node.insert(0, coro_annotation)

                if obj_mm_info['is_pymongo_docstring']:
                    # Remove all "versionadded", "versionchanged" and
                    # "deprecated" directives from the docs we imported from
                    # PyMongo
                    version_nodes = find_by_path(
                        desc_content_node, [versionmodified])

                    for version_node in version_nodes:
                        version_node.parent.remove(version_node)

                    # Remove all "seealso" directives that contain :doc:
                    # references from PyMongo's docs
                    seealso_nodes = find_by_path(desc_content_node, [seealso])

                    for seealso_node in seealso_nodes:
                        if 'reftype="doc"' in str(seealso_node):
                            seealso_node.parent.remove(seealso_node)


def get_mongomotor_attr(motor_class, name, *defargs):
    """If any Motor attributes can't be accessed, grab the equivalent PyMongo
    attribute. While we're at it, store some info about each attribute
    in the global mm_info dict.
    """

    attr = safe_getattr(motor_class, name)
    method_class = safe_getattr(attr, 'im_class', None)
    from_pymongo = not safe_getattr(
        method_class, '__module__', '').startswith('mongomotor')

    # Store some info for process_motor_nodes()
    full_name = '%s.%s.%s' % (
        motor_class.__module__, motor_class.__name__, name)

    short = full_name.replace('.document.', '.')

    has_coroutine_annotation = getattr(attr, 'coroutine_annotation', False)
    is_async_method = getattr(attr, 'is_async_method', False)
    is_cursor_method = getattr(attr, 'is_motorcursor_chaining_method', False)

    # attr.doc is set by statement like 'error = AsyncRead(doc="OBSOLETE")'.
    is_pymongo_doc = ((from_pymongo or is_async_method or is_cursor_method)
                      and not getattr(attr, 'doc', None))

    mm_info[full_name] = mm_info[short] = {
        'has_coroutine_annotation': has_coroutine_annotation,
        # These sub-attributes are set in motor.asynchronize()
        'is_async_method': is_async_method,
        'is_pymongo_docstring': is_pymongo_doc,
    }

    return attr


# def get_motor_argspec(pymongo_method, is_async_method):
#     args, varargs, kwargs, defaults = inspect.getargspec(pymongo_method)

#     # This part is copied from Sphinx's autodoc.py
#     if args and args[0] in ('cls', 'self'):
#         del args[0]

#     defaults = list(defaults) if defaults else []

#     if is_async_method:
#         # Add 'callback=None' argument
#         args.append('callback')
#         defaults.append(None)

#     return args, varargs, kwargs, defaults


# # Adapted from MethodDocumenter.format_args
# def format_motor_args(pymongo_method, is_async_method):
#     argspec = get_motor_argspec(pymongo_method, is_async_method)
#     formatted_argspec = inspect.formatargspec(*argspec)
#     # escape backslashes for reST
#     return formatted_argspec.replace('\\', '\\\\')


# def process_motor_signature(
#         app, what, name, obj, options, signature, return_annotation):
#     if name in mm_info and mm_info[name].get('pymongo_method'):
#         # Real sig obscured by decorator, reconstruct it
#         pymongo_method = mm_info[name]['pymongo_method']
#         is_async_method = mm_info[name]['is_async_method']
#         args = format_motor_args(pymongo_method, is_async_method)
#         return args, return_annotation


def setup(app):
    app.add_autodoc_attrgetter(type(mongomotor.document.DocumentBase),
                               get_mongomotor_attr)
    app.add_autodoc_attrgetter(type(mongomotor.queryset.QuerySet),
                               get_mongomotor_attr)
    # app.connect('autodoc-process-signature', process_motor_signature)
    app.connect("doctree-read", process_mongomotor_nodes)
