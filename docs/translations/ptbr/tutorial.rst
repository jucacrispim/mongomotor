Uso do mongomotor
=================

Usar o mongomotor é bem similar ao uso do mongoengine. Para escrever seus
modelos não há diferença, a não ser no import. Por isso, usaremos o mesmo
exemplo usado no tutorial do mongoengine. Vamos criar um tumblelog simples.


Definindo nossos documentos
+++++++++++++++++++++++++++

Para começar, vamos definir os seguintes documentos:

.. code-block:: python

    # Os imports são como os do mongoengine, só alterando ``mongoengine``
    # para ``mongomotor``.
    from mongomotor import connect, Document, EmbeddedDocument
    from mongomotor.fields import (StringField, ReferenceField, ListField,
				   EmbeddedDocumentField)
    from tornado import gen

    # Primeiro criando a conexão com o banco de dados.
    connect('mongomotor-test')


    # Aqui os documentos iguais aos do tutorial do mongoengine
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


Agora, o uso é praticamente igual ao do mongoengine. Vejamos:


Adicionando dados ao nosso tumblelog
++++++++++++++++++++++++++++++++++++

Para adicionar um novo documento à base de dados, faremos tudo
como no mongoengine, a diferença é que quando formos usar o método
save, usaremos ``yield``

.. code-block:: python

    author = User(email='ross@example.com', first_name='Nice', last_name='Guy')
    yield author.save()

    post1 = TextPost(title='Fun with MongoMotor', author=author)
    post1.content = 'Took a look at MongoEngine today, looks pretty cool.'
    post1.tags = ['mongodb', 'mongoengine', 'mongomotor']
    yield post1.save()

    post2 = LinkPost(title='MongoMotor Documentation', author=author)
    post2.link_url = 'http://mongomotor-ptbr.readthedocs.org/pt/latest/'
    post2.tags = ['mongomotor']
    yield post2.save()


Acessando nossos dados
++++++++++++++++++++++

Agora que já temos alguns posts, podemos acessá-los. Novamente é como o
mongoengine, só com uns ``yield`` por aí. Vamos lá acessar os nossos dados:

.. code-block:: python

    # Aqui listando todos os posts que heraram de Post
    for post_future in Post.objects:
        post = yield post_future
        print(post.title)

    # Aqui só os TextPost do ator ``author``
    for post_future in TextPost.objects.filter(author=author):
        post = yield post_future
        print(post.content)

    # E aqui filtrando por tags
    for post_future in TextPost.objects(tags='mongomotor'):
        post = yield post_future
        print(post.content)

Bom, aí você pode estar se perguntando, com esse monte de ``yield`` no loop,
mas o mongomotor faz uma consulta ao banco em cada iteração? Não, não faz.
É o mesmo comportamento do motor, veja mais aqui <LINK>

Quando usamos get() e delete() também precisamos usar ``yield``, assim:

.. code-block:: python

    post = yield TextPost.objects.get(title='Fun with MongoMotor')
    yield post.delete()

Bom, é isso.
