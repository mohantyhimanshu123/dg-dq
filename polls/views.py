from django.http import HttpResponse
from django.shortcuts import render, redirect
from .models import talents
from .forms import files

def home(request):
        return render(request, 'home.html', {'name': 'Himanshu'})

def add(request):
        num1 = int(request.POST['num1'])
        num2 = int(request.POST['num2'])
        result  = num1 + num2
        return render(request, 'results.html', {'add': result})

def index(request):
        talent1 = talents()
        talent1.name = 'Data warehousing'
        talent1.desc = 'It includes machine learning and algorithms for proper implementation of data and also helps in proper management of Big Data'
        talent2 = talents()
        talent2.name = 'Machine Learning'
        talent2.desc = 'It includes machine learning and algorithms for proper implementation of data and also helps in proper management of Big Data'
        return render(request, 'index.html', {'talent1': talent1, 'talent2': talent2} )

def upload(request):
        if request.method == 'POST':
                form = files(request.POST, request.FILES)
                if form.is_valid():
                        form.save()
        else:
                form = files() 
        return render(request, 'index.html',
        {
                'form':form
        })
