from __future__ import annotations
import os

from .commons import Commons

from cache.cacheable import cacheable

# This class is not used yet
@cacheable
class Jupyterhub:
    USER_TOKEN_FILE = "jupyter_user_token.pkl"

    def get_user_token(self):
        """
        Get the user token for jupyterhub.
        """
        return self.jupyter_token

    def __init__(self, connection: Commons):
        self.connection = connection
        self.jupyter_base_url = os.getenv("JUPYTERHUB_URL")

    