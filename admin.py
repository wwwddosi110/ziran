from django.contrib import admin
from django_neomodel import admin as neo_admin
from graph.models import graphnode

class GraphAdmin(admin.ModelAdmin):
    list_display = ('name',)
neo_admin.register(graphnode,GraphAdmin)