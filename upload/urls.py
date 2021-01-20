from django.urls import path
from django.conf.urls.static import static
from . import views
from django.conf import settings

urlpatterns = [
    path('', views.home, name = 'home'),
    path('table', views.table, name = 'table'),
    path('raw', views.raw, name = 'raw'),
    path('zipopen1', views.csv1, name = 'csv1'),
    path('zipopen2', views.csv2, name = 'csv2'),
    path('merger', views.merger, name = 'merger'),
    path('format', views.format_table, name = 'format'),
    path('analyze', views.analyze, name = 'analyze'),
    path('phone', views.phone1, name = 'phone'),
    path('phone_a', views.phone_a1, name = 'phone_a'),
    path('zip', views.zip1, name = 'zip'),
    path('zip_a', views.zip_a1, name = 'zip_a'),
    path('email', views.email1, name = 'email'),
    path('email_a', views.email_a1, name = 'email_a'),
    path('address', views.address1, name = 'address'),
    path('address_a', views.address_a1, name = 'address_a'),
    path('phone3', views.phone1, name = 'phone3'),
    path('phone_a3', views.phone_a1, name = 'phone_a3'),
    path('zip3', views.zip1, name = 'zip3'),
    path('zip_a3', views.zip_a1, name = 'zip_a3'),
    path('email3', views.email1, name = 'email3'),
    path('email_a3', views.email_a1, name = 'email_a3'),
    path('address3', views.address1, name = 'address3'),
    path('address_a3', views.address_a1, name = 'address_a3'),
    path('column_outliers', views.op_out, name = 'column_outliers'),
    path('total_outliers', views.total_out, name = 'total_outliers'),
    path('data_lineage', views.data_lineage, name = 'data_lineage'),
    #path('uploadcsv/',csvformview.as_view(),name = "uploadcsv" ),
    #path('csvdata/', csv_view, name = 'csv_data'),
] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT) 