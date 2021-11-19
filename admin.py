from django.contrib import admin
from django_neomodel import admin as neo_admin
from kgmodel.models import Kgmodel

class KgModelAdmin(admin.ModelAdmin):
    list_display = ('name',)
neo_admin.register(Kgmodel,KgModelAdmin)