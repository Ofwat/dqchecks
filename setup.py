from setuptools import setup, find_packages

setup(
    name="ofwat-dqchecks",
    version="0.0.22",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "openpyxl",
        "numpy"
    ],
)
