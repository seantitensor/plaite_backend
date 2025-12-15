"""Firebase client initialization and management."""

import firebase_admin
from firebase_admin import credentials, firestore, storage

from plaite.config import FirebaseConfig

_app: firebase_admin.App | None = None


def init_firebase(config: FirebaseConfig) -> firebase_admin.App:
    """Initialize Firebase app with the given config."""
    global _app

    if _app is not None:
        return _app

    if not config.credentials_path.exists():
        raise FileNotFoundError(f"Credentials file not found: {config.credentials_path}")

    cred = credentials.Certificate(str(config.credentials_path))
    _app = firebase_admin.initialize_app(cred, {"storageBucket": config.storage_bucket})

    return _app


def get_client(config: FirebaseConfig) -> firestore.Client:
    """Get Firestore client, initializing Firebase if needed."""
    init_firebase(config)
    return firestore.client()


def get_collection(config: FirebaseConfig):
    """Get the recipes collection reference."""
    client = get_client(config)
    return client.collection(config.collection)


def get_storage_bucket(config: FirebaseConfig):
    """Get the Firebase Storage bucket."""
    init_firebase(config)
    return storage.bucket()


def reset_client():
    """Reset the Firebase client (useful for testing or switching environments)."""
    global _app
    if _app is not None:
        firebase_admin.delete_app(_app)
        _app = None
