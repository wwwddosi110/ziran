from django.urls import path
from graph import views
app_name = 'graph'

urlpatterns = [

     path('create_entry/', views.create_entry, name='create/entry/'),
     path('create_info/', views.create_info, name='create/info/'),
     path('create_base/', views.create_base, name='create/base/'),
     path('create_generate/', views.create_generate, name='create'),
     path('index/', views.index, name='index'),
     path('detail/<int:pk>/', views.detail, name='detail'),
     path('delete/<int:pk>/', views.delete, name='delete'),

     path('node_route/', views.node_route, name='node_route'),

     path('node_index/<int:pk>/', views.node_search, name='node_index'),
     path('node_index_select/', views.node_index_select, name='node_index_select'),

     path('relation_index/', views.relation_index, name='relation_index'),
     path('relation_index_select/', views.relation_index_select, name='relation_index_select'),

     path('node_origin/<int:pk>/', views.node_origin, name='node_origin'),
     path('node_click/<int:pk>/', views.node_click, name='node_click'),

     path('add_data/<int:pk>/', views.add_data, name='add_data'),

     path('graph_route/<int:pk>/', views.graph_route, name='graph_route'),

     path('entity_label_choose/<int:pk>/', views.entity_label_choose, name='entity_label_choose'),
     path('relation_label_choose/<int:pk>/', views.relation_label_choose, name='relation_label_choose'),

]

