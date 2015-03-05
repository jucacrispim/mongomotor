# -*- coding: utf-8 -*-

import requests

BASE_URL = 'http://localhost:8888/textpost/'
COMMENT_URL = BASE_URL + 'putcomment'

POSTS = [{'title': 'cacilds!',
          'content': "Mussum ipsum cacilds, vidis litro abertis."},

         {'title': 'Suco de cevadis!',
          'content': 'Suco de cevadis, é um leite divinis, qui tem lupuliz, maltis, aguis e fermentis.'}]

print('Cadastrando os novos posts')

for p in POSTS:
    response = requests.put(BASE_URL, params=p)
    response.connection.close()


print('Listando os posts')

response = requests.get(BASE_URL)
response.connection.close()
post_list = response.json()

for p in post_list:
    print(p['title'])
    print(p['content'])
    print('\n')


print('Fazendo um comentário no post %s' % post_list[1]['title'])

comment = {'name': 'Mussum', 'content': 'E o mézis tambénzis!',
           'post_id': post_list[1]['id']}

requests.put(COMMENT_URL, params=comment)


print('Apagando os posts')

for p in post_list:
    response = requests.delete(BASE_URL, params = {'id': p['id']})
    response.connection.close()
