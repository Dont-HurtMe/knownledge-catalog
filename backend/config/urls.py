from django.contrib import admin
from django.urls import path
from api import views

urlpatterns = [ 
    path('admin/', admin.site.urls), 
    path('api/upload/', views.upload_document),
    path('api/catalog/<uuid:doc_id>/init/', views.init_pages),
    path('api/transaction/<uuid:doc_id>/update/', views.update_transaction),
    path('api/catalog/<uuid:doc_id>/complete/', views.complete_catalog),
]
