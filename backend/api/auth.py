import jwt
from rest_framework.authentication import BaseAuthentication
from rest_framework.exceptions import AuthenticationFailed
from django.conf import settings

class DummyUser:
    def __init__(self, user_id, role):
        self.id = str(user_id)
        self.is_authenticated = True
        self.is_staff = (role == 'admin')
        self.is_superuser = self.is_staff

class StatelessJWTAuthentication(BaseAuthentication):
    def authenticate(self, request):
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return None 
        token = auth_header.split(' ')[1]
        try:
            payload = jwt.decode(token, settings.SHARED_JWT_SECRET, algorithms=['HS256'])
            user_id = payload.get('user_id')
            role = payload.get('role', 'user')
            if not user_id:
                raise AuthenticationFailed('Token does not contain user_id')
            user = DummyUser(user_id=user_id, role=role)
            return (user, token)
            
        except jwt.ExpiredSignatureError:
            raise AuthenticationFailed('Token has expired')
        except jwt.InvalidTokenError:
            raise AuthenticationFailed('Invalid token signature')