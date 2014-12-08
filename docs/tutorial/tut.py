# -*- coding: utf-8 -*-

from mongomotor import connect, Document, EmbeddedDocument
from mongomotor.fields import (StringField, ReferenceField, ListField,
                               EmbeddedDocumentField)

connect('mongomotortut')

class Comment(EmbeddedDocument):
    content = StringField()
    name = StringField(max_length=120)


class User(Document):
    email = StringField(required=True)
    first_name = StringField(max_length=50)
    last_name = StringField(max_length=50)


class Post(Document):
    title = StringField(max_length=120, required=True)
    author = ReferenceField(User)
    tags = ListField(StringField(max_length=30))
    comments = ListField(EmbeddedDocumentField(Comment))

    meta = {'allow_inheritance': True}


class TextPost(Post):
    content = StringField()


class ImagePost(Post):
    image_path = StringField()


class LinkPost(Post):
    link_url = StringField()


from tornado import gen, ioloop
from tornado.web import RequestHandler, HTTPError, URLSpec, Application


class TextPostHandler(RequestHandler):

    @gen.coroutine
    def get(self, operation):
        if not operation:
            operation = 'get'
        operations = {'get': self.get_object, 'list': self.list_objects}
        if operation not in operations.keys():
            raise HTTPError(404)

        method = operation.get(operation)
        ret = yield method()
        self.write(ret)

    @gen.coroutine
    def put(self, operation):
        if not operation:
            operation = 'putpost'
        operations = {'putpost': self.put_post,
                      'putcomment': self.put_comment}

        if operation not in operations:
            raise HTTPError(404)

        method = operations.get(operation)
        ret = yield method()
        self.write(ret)

    @gen.coroutine
    def delete(self):
        post_id = self.request.arguments['id'][0].decode()
        post = yield TextPost.objects.get(id=post_id)
        yield post.delete()

        self.write(post)

    @gen.coroutine
    def put_post(self):
        params = {k: v.decode() for k, v
                  in self.request.arguments.items()}
        post = TextPost(**params)
        yield post.save()
        return post

    @gen.coroutine
    def put_comment(self):
        post_id = self.request.arguments['post_id']
        post = yield TextPost.objects.get(id=post_id)
        params = self.request.arguments
        del params['post_id']
        commment = Comment(**params)
        post_comments = yield post.comments
        post_comments.append(comment)
        post.comments = post_comments
        yield post.save()
        return comment

    @gen.coroutine
    def get_object(self):
        post_id = self.request.arguments['id'].decode()
        post = yield TextPost.objects.get(id=post_id)
        return post

    @gen.coroutine
    def list_objects(self):
        qs = TextPost.objects.filter(**self.request.arguments)
        ret = []
        for future in qs:
            post = yield future
            ret.append(post)
        return ret


class UserHandler(RequestHandler):

    @gen.coroutine
    def get(self, operation):
        if not operation:
            operation = 'get'
        operations = {'get': self.get_object, 'list': self.list_objects}
        if operation not in operations:
            raise HTTPError(404)

        method = operations.get(operation)
        ret = yield method()
        self.write(ret)

    @gen.coroutine
    def put(self):
        params = {k: v[0].decode() for k, v
                  in self.request.arguments.items()}
        user = User(**params)
        yield user.save()
        self.write(user.to_json())

    @gen.coroutine
    def delete(self):
        user_id = self.request.arguments['id'][0].decode()
        user = yield User.objects.get(id=post_id)
        yield user.delete()

        self.write(user.to_json())

    @gen.coroutine
    def get_object(self):
        user_id = self.request.arguments['id'][0].decode()
        user = yield User.objects.get(id=user_id)
        return user.to_json()

    @gen.coroutine
    def list_objects(self):
        params = {k: v[0].decode() for k, v
                  in self.request.arguments}
        qs = User.objects.filter(**self.request.arguments)
        ret = []
        for future in qs:
            user = yield future
            ret.append(user)
        return ret


post_url = URLSpec('/textpost/(.*)$', TextPostHandler, name='textposthandler')
user_url = URLSpec('/user/$', UserHandler, name='userhandler')
app = Application([post_url, user_url])

if __name__ == '__main__':

    port = 8888
    app.listen(port)
    ioloop.IOLoop.instance().start()
