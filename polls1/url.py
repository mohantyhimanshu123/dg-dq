from django.urls import path
from . import views

urlpatterns = [
    path("", views.polls1, name = 'polls1'),
]