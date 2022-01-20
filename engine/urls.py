from django.urls import path
from . import views

app_name = 'engine'
urlpatterns = [
    path('', views.index, name='index'),
    path('query/', views.rawQuery, name='query'),
    # path('query2/', views.rawQuery2, name='query2'),
]