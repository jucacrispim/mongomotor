# -*- coding: utf-8 -*-

from mongomotor import connect, Document, EmbeddedDocument
from mongomotor.fields import (StringField, ReferenceField, ListField,
                               EmbeddedDocumentField)
from tornado import gen

# criando a conexão com o banco de dados.
connect('mongomotor-test')


# Aqui os modelos iguais aos do tutorial do mongoengine
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
    slug = StringField(max_length=120, required=True)

    meta = {'allow_inheritance': True}

    @gen.coroutine
    def save(self, *args, **kwargs):
        """Se não houver slug, criamos um"""
        if not self.slug:
            # enough for demo purposes... ;)
            self.slug = self.title.replace(' ', '-').lower()

        yield super(Post, self).save(*args, **kwargs)


class TextPost(Post):
    content = StringField()


class ImagePost(Post):
    image_path = StringField()


class LinkPost(Post):
    link_url = StringField()



# Agora aqui uma aplicação tornado pra testar o mongomotor.
import json
from tornado import ioloop, gen
from tornado.web import RequestHandler, HTTPError, URLSpec, Application


class TextPostHandler(RequestHandler):

    @gen.coroutine
    def get(self, slug=None):
        """No tornado, o método get é o que trata as requisições GET.
        O funcionamento do nosso será o seguinte:
        Se o request for direto em ``/``, isto é, não tiver slug nenhum,
        listaremos os posts, se houver um slug
        (a url seria algo assim: ``/slug-do-post``) exibiremos o post
        correspondente.
        """
        if not slug:
            ret = yield self.list_objects()
        else:
            ret = yield self.get_object(slug)

        self.write(ret)

    @gen.coroutine
    def put(self, operation):
        """Método responsável por tratar requisições PUT.
        Aqui funciona assim: Pode-se enviar PUT requests para
        ``/``, ``/putpost`` ou ``/putcomment``,
        sendo ``/`` ou ``/putpost`` para cadastrar
        um post e ``/putcomment`` para cadastrar um comentário
        """
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
    def delete(self, operation):
        """Método que trata DELETE requests.
        Aqui serve pra apagar um post.
        """
        # id vem como parâmetro do request
        post_id = self.request.arguments['id'][0].decode()
        # primeiro pegar o post com o id desejado
        # repare no yield quando usamos get()
        post = yield TextPost.objects.get(id=post_id)
        # e agora apagar o post, também usando yield com delete()
        yield post.delete()

        self.write(json.dumps((yield self._post2dict(post))))

    @gen.coroutine
    def put_post(self):
        """Método que cadastra um novo post
        """
        params = {k: v[0] for k, v in self.request.arguments.items()}

        # criando uma nova instância
        post = TextPost(**params)
        # salvando no banco de dados.
        # Repare no yield que usamos com save()
        yield post.save()
        return json.dumps((yield self._post2dict(post)))

    @gen.coroutine
    def put_comment(self):
        """Método que cadastra um novo comentário
        """
        # primeiro pegamos o post que será o alvo do comentário
        post_id = self.request.arguments['post_id'][0].decode()
        # não esqueça que precisamos usar yield com get()
        post = yield TextPost.objects.get(id=post_id)

        params = self.request.arguments.copy()
        del params['post_id']

        # Agora simplismente criar o comentário
        comment = Comment(**params)
        # adicionar na lista de comentários do post
        post.comments = (yield post.comments).append(comment)
        # e salvar o post, sempre com yield
        yield post.save()

        return json.dumps(self._comment2dict(comment))

    @gen.coroutine
    def get_object(self, slug):
        """Método que pega um post do banco de dados, através do slug
        """
        post = yield TextPost.objects.get(slug=slug)
        return json.dumps((yield self._post2dict(post)))

    @gen.coroutine
    def list_objects(self):
        """
        Método que lista os posts
        """

        qs = TextPost.objects.filter(**self.request.arguments)
        # qs.to_list() transforma o queryset em uma lista de documentos.
        posts = yield qs.to_list()
        post_list = []

        for p in posts:
            d = yield self._post2dict(p)
            post_list.append(d)
        return json.dumps(post_list)

    @gen.coroutine
    def _post2dict(self, post):

        post_dict = {}
        post_dict['id'] = str(post.id)
        post_dict['title'] = post.title
        author = yield post.author
        post_dict['author'] = author.name if author else ''
        post_dict['content'] = post.content
        return post_dict

    def _comment2dict(self, comment):
        comment_dict = {}
        comment_dict['content'] = comment.content
        comment_dict['name'] = comment.name


# Criando o mapeamento de url para a aplicação
url = URLSpec('/textpost/(.*)$', TextPostHandler, name='textposthandler')
app = Application([url])

# com este mapeamento, as urls que podemos acessar são:
# GET em /textpost/ para listar os posts
# GET em /textpost/<slug> para pegar um post específico
# DELETE em /textpost/ para apagar um post
# PUT em /textpost/ ou /textpost/putpost para criar um novo post
# PUT em /textpost/putcomment para criar um novo comentário

if __name__ == '__main__':

    # Aqui simplesmente iniciamos o server do tornado na porta 8888
    port = 8888
    app.listen(port)
    ioloop.IOLoop.instance().start()
