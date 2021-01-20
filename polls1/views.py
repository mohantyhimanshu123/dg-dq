from django.shortcuts import render
from django.http import HttpResponse

def polls1(request):
    return HttpResponse(" This is the second app")

# Create your views here.
