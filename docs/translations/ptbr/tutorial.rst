Uso do mongomotor
=================

Usar o mongomotor é bem similar ao uso do mongoengine. Para escrever seus
modelos não há diferença, a não ser no import. Por isso, usaremos o mesmo
exemplo usado no tutorial do mongoengine. Vamos criar um blog simples.

Pra começar, vamos escrever os seguintes modelos:

.. code-block:: python

    from mongomotor import connect, Document, EmbeddedDocument
    from mongomotor.fields import (StringField, ReferenceField, ListField,
				   EmbeddedDocumentField)

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


Agora, como estamos usando o motor como driver para o mongodb, precisamos
criar uma aplicação tornado.

.. code-block:: python

    from tornado import gen
    from tornado.web import RequestHandler, HTTPError


    class TextPostHandler(RequestHandler):

	@gen.coroutine
	def get(self, operation=None):
	    if operation is None:
		operation = 'get'

	    operations = {'get': self.get_object, 'list': self.list_objects}
	    if operation not in operations.keys():
		raise HTTPError(404)

	    method = operations.get(operation)
	    ret = yield method()
	    self.write(ret)

	@gen.coroutine
	def put(self, operation='putpost'):
	    operations = {'putpost': self.put_post,
			  'putcomment': self.put_comment}

	    if operation not in operations:
		raise HTTPError(404)

	    method = operations.get(operation)
	    ret = yield method()
	    self.write(ret)

	@gen.coroutine
	def delete(self):
	    post_id = self.request.arguments['id']
	    post = yield TextPost.objects.get(id=post_id)
	    yield post.delete()

	    self.write(post)

	@gen.coroutine
	def put_post(self):
	    post = TextPost(**self.request.arguments)
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
	    post_id = self.request.arguments['id']
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

    url = URLSpec('/textpost/(.*)$', TextPostHandler, name='textposthandler')
    app = Application([url])

    if __name__ == '__main__':

	port = 8888
	app.listen(port)
	ioloop.IOLoop.instance().start()


Depois desse código já podemos subir a nossa aplicação.

.. code-block:: sh

    $ python tut.py
