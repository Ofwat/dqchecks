from setuptools import setup, find_packages

# read the contents of your README file
from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name="ofwat-dqchecks",
    version="0.0.74",
    author="Ofwat",
    description='Excel validations',
    long_description=long_description,
    long_description_content_type='text/markdown',
    keywords="excel data validation",
    packages=find_packages(),
    url="https://github.com/Ofwat/dqchecks",
    project_urls={
        'Source': 'https://github.com/Ofwat/dqchecks',
        'Tracker': 'https://github.com/Ofwat/dqchecks/issues',
    },
    install_requires=[
        "pandas",
        "openpyxl",
        "numpy"
    ],
)
