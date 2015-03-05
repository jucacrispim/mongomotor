Bem-vindo à documentação do mongomotor!
=======================================

O mongomotor é uma pequena integração do mongoengine, um document object
mapper para python e mongodb com o motor, um driver assíncrono para mongodb
feito usando o mainloop do tornado.

Usando o mongomotor você pode escrever seus modelos como você já faz utilizando
o mongoengine e fazer as operações em banco de dados assincronamente utilizando
o motor.


Instalação
==========

Primeiro, clone o projeto no gitorious:

.. code-block:: sh

    $ git clone https://gitorious.org/mongomotor/mongomotor.git

Depois, instale as dependências:

.. code-block:: sh

    $ cd mongomotor && pip install -r requirements.txt

E por fim, rode os testes:

.. code-block:: sh

    $ python setup.py test

Começando
+++++++++
.. toctree::
   :maxdepth: 1

   tutorial
