import os
import pathlib
from io import open
from setuptools import setup, find_packages
#Setup para la packaging
from package import config
REQUIRED_PACKAGES = ["apache-beam==2.35.0"]

here = pathlib.Path(__file__).parent.resolve()
long_description = (here / 'README.md').read_text(encoding='utf-8')



setup(
    name=config.PACKAGE_NAME,
    version=config.PACKAGE_VERSION,
    url=config.__URL__,
    author=config.__DEV__,
    author_email = config.__DEV_EMAIL__,
    description=config.__DESCRIPTION__,
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),    
    include_package_data=True,
    install_requires=REQUIRED_PACKAGES,
    package_data= {
        config.PACKAGE_NAME: [
            "data/*.log"
        ],
    },
)