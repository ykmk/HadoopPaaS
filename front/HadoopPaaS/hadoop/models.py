from django.db import models
from django.contrib.auth.models import User


class Cluster(models.Model):
    user = models.OneToOneField(User) #クラスタ作成者
    master = models.OneToOneField(Node) #クラスタのマスタノード
    name = models.CharField(max_length=50) #クラスタ名
    release_date = models.DateField() #クラスタ作成日時


class Node(models.Model):
    cluster = models.ForeignKey(Cluster) #ノードの所属するクラスタ
    ip = models.CharField(max_length=15) #IPアドレス
