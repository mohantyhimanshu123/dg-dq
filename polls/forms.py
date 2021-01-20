from django import forms
from .models import data_sheet

class files(forms.ModelForm):
    class Meta:
        model = data_sheet
        fields = ('name', 'csv')