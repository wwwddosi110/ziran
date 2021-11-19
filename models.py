from django_neomodel import DjangoNode
from neomodel import StringProperty, UniqueIdProperty, Relationship, ZeroOrMore
from kgmodel.models import Kgmodel


class graphnode(DjangoNode):#定义一个图谱的标签
    uid = UniqueIdProperty()
    name=StringProperty(max_length=100)
    kg_re_model=Relationship(Kgmodel,'使用',cardinality=ZeroOrMore)
    class Meta:
        app_label = 'graph'
