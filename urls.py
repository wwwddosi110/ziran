from django.conf.urls import url
from django.urls import path
from kgmodel import views

app_name = 'model_manager'

urlpatterns =[
     path('index/', views.index, name='index'),
     path('entry/', views.entry, name='index'),
     path('create/',views.create,name='create'),
     path('detail/<int:pk>/', views.detail, name='detail'),
     path('delete/<int:pk>/', views.delete, name='delete'),
     path('edit/<int:pk>/', views.edit, name='edit')
]