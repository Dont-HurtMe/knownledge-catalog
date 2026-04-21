from django.contrib import admin
from django.urls import path, include
from rest_framework.routers import DefaultRouter
from drf_spectacular.views import SpectacularAPIView, SpectacularSwaggerView
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView 
from api import views

router = DefaultRouter()
router.register(r'catalog', views.KnowledgeCatalogViewSet, basename='catalog')
router.register(r'departments', views.DepartmentViewSet, basename='departments')
router.register(r'folders', views.FolderViewSet, basename='folders')

urlpatterns = [ 
    path('admin/', admin.site.urls), 
    path('api/token/', TokenObtainPairView.as_view(), name='token_obtain_pair'),
    path('api/token/refresh/', TokenRefreshView.as_view(), name='token_refresh'),
    path('api/upload/', views.KnowledgeCatalogViewSet.as_view({'post': 'create'}), name='upload-legacy'),
    path('api/', include(router.urls)),
    path('api/schema/', SpectacularAPIView.as_view(), name='schema'),
    path('api/docs/', SpectacularSwaggerView.as_view(url_name='schema'), name='swagger-ui'),
]