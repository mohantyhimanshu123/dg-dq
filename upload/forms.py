from .models import csvfileupload
from  django import forms

class csvform(forms.ModelForm):
    csvfile = forms.FileField()
    
    class Meta:
        model = csvfileupload
        fields = ["csvfile"]