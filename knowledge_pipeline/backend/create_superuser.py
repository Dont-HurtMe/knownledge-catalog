import os, django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()
from django.contrib.auth import get_user_model
User = get_user_model()
su_username = os.environ.get('SUPER_USER_BACKEND', 'admin')
if not User.objects.filter(username=su_username).exists():
    User.objects.create_superuser(su_username, f'{su_username}@example.com', os.environ.get('SUPER_USER_BACKEND_PASSWORD', 'qwer1234'))
