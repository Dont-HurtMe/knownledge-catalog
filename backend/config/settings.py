from pathlib import Path
from datetime import timedelta
import os
BASE_DIR = Path(__file__).resolve().parent.parent
SECRET_KEY = os.environ.get('DJANGO_SECRET_KEY', 'django-insecure-dev-key')
SHARED_JWT_SECRET = os.environ.get('SHARED_JWT_SECRET', 'my-super-secret-key-for-microservice')
DEBUG = True
ALLOWED_HOSTS = ['*']
INSTALLED_APPS = [
    "unfold",  
    "unfold.contrib.filters",  
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'drf_spectacular',
    'rest_framework',
    'api',
]
MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]
ROOT_URLCONF = 'config.urls'
WSGI_APPLICATION = 'config.wsgi.application'
TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': os.environ.get('DB_NAME', 'knowledgedb'),
        'USER': os.environ.get('DB_USER', 'dbadmin'),
        'PASSWORD': os.environ.get('DB_PASSWORD', 'qwer1234'),
        'HOST': os.environ.get('DB_HOST', 'postgres'),
        'PORT': os.environ.get('DB_PORT', '5432'),
    }
}
# เพิ่มเข้าไปใน settings.py
REST_FRAMEWORK = {
    'DEFAULT_SCHEMA_CLASS': 'drf_spectacular.openapi.AutoSchema',
    'DEFAULT_AUTHENTICATION_CLASSES': (
        'rest_framework_simplejwt.authentication.JWTAuthentication', 
        'rest_framework.authentication.SessionAuthentication', 
    ),
    'DEFAULT_PERMISSION_CLASSES': [
        'rest_framework.permissions.IsAuthenticated', 
    ]
}

SIMPLE_JWT = {
    'ACCESS_TOKEN_LIFETIME': timedelta(days=1), # Token มีอายุ 1 วัน
    'REFRESH_TOKEN_LIFETIME': timedelta(days=7),
}

SPECTACULAR_SETTINGS = {
    'TITLE': 'Knowledge Pipeline API',
    'VERSION': '2.0.0',
    'SERVE_INCLUDE_SCHEMA': False,
    'SECURITY': [{'jwt': []}],
    'SECURITY_DEFINITIONS': {
        'jwt': {
            'type': 'http',
            'scheme': 'bearer',
            'bearerFormat': 'JWT',
        }
    }
}

LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_TZ = True
STATIC_URL = 'static/'
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'
