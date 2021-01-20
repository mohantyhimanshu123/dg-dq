from django.db import models

class csv_upload(models.Model):
    file = models.FileField()

class csvfileupload(models.Model):
    csvfile = models.FileField(upload_to='csv_files/', null = True, verbose_name="")
    def __str__(self):
         return str(self.csvfile)