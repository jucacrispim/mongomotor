import requests
BASE_URL = 'http://localhost:8888/'
POST_URL = BASE_URL + 'textpost/'
USER_URL = BASE_URL + 'user/'
COMMENT_URL = POST_URL + 'putcomment'

print("Creating user Zé Mané")
user_params = {'email': 'email@somewhere.com',
               'first_name': 'Zé',
               'last_name': 'Mané'}

response = requests.put(USER_URL, params=user_params)
response.connection.close()
user_id = response.json()['_id']['$oid']
print("User created: %s" % user_id)

print("Creating Test Post")
post_params = {'title': 'Test post',
               'author': user_id,
               'tags': ['test', 'test1'],
               'content': 'blablabla!!'}

response = requests.put(POST_URL, params=post_params)
response.connection.close()
post_id = response.json()['_id']['$oid']
print("Post created: %s" % post_id)

print("Inserting Comment on post")
comment_params = {'post_id': post_id,
                  'content': 'comment!',
                  'name': 'Commenter'}

response = requests.put(COMMENT_URL, params=comment_params)
response.connection.close()
print('Comment added %s' % str(response.json()))

print("Retrieving post %s" % post_id)
response = requests.get(POST_URL, params={'id': post_id})
response.connection.close()
print('Post %s retrieved:\n%s' % (post_id, str(response.json())))
