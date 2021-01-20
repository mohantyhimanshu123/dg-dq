from django.urls import path
from . import views
 
urlpatterns = [
    path("", views.home, name = "home"),
    path("add", views.add, name = "add"),
    path("index", views.index, name = 'index'),
    path("upload/", views.upload, name = 'upload'),

]