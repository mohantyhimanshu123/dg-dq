from django.db import models

# Create your models here.

class talents:
    image = str
    name = str
    desc = str

class data_sheet(models.Model):
    name = models.CharField(max_length=100)
    csv = models.FileField(upload_to="files/", max_length=100)

    def __str__(self):
        return self.name
    