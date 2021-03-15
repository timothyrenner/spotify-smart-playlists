from setuptools import setup, find_packages

setup(
    name="spotify-smart-playlists",
    packages=find_packages(exclude=["scripts"]),
    author="Tim Renner",
    install_requires=[
        "python-dotenv",
        "spotipy",
        "loguru",
        "ghapi",
        "pynacl",
        "toolz",
        "dvc[gs]",
        "pandas",
    ],
)