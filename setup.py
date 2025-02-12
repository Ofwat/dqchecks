from setuptools import setup, find_packages

setup(
    name="dqchecks",
    version="0.0.4",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "openpyxl",
        "datetime",
        "numpy"
    ],
)