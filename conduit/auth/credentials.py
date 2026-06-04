"""
Credentials and User Profile management.
"""

from typing import Dict, List, Optional, Set

class UserProfile:
    """Represents a user profile with associated roles and permissions."""
    
    def __init__(self, username: str, roles: Set[str], permissions: Set[str]):
        self.username = username
        self.roles = roles
        self.permissions = permissions
        
    def has_role(self, role: str) -> bool:
        return role in self.roles
        
    def has_permission(self, permission: str) -> bool:
        return permission in self.permissions


class CredentialsManager:
    """
    Manages client credentials, authentication, and access control.
    """
    
    def __init__(self):
        self._users: Dict[str, str] = {}  # username -> password_hash
        self._profiles: Dict[str, UserProfile] = {}
        
    def add_user(
        self,
        username: str,
        password: str,
        roles: Optional[List[str]] = None,
        permissions: Optional[List[str]] = None
    ) -> None:
        """Add or update a user's credentials and profile."""
        import hashlib
        password_hash = hashlib.sha256(password.encode('utf-8')).hexdigest()
        self._users[username] = password_hash
        self._profiles[username] = UserProfile(
            username=username,
            roles=set(roles or ["user"]),
            permissions=set(permissions or [])
        )
        
    def verify(self, username: str, password_hash: str) -> bool:
        """Verify user's password hash."""
        if username not in self._users:
            return False
        return self._users[username] == password_hash
        
    def get_profile(self, username: str) -> Optional[UserProfile]:
        """Get user profile."""
        return self._profiles.get(username)
