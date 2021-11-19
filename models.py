from django_neomodel import DjangoNode
from neomodel import StringProperty, UniqueIdProperty, RelationshipFrom


class Kgmodel(DjangoNode):#定义模型的标签用于管理模型
    uid = UniqueIdProperty()
    name = StringProperty(max_length=100)
    class Meta:
        app_label = 'kgmodel'

