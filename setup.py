from setuptools import setup, find_packages

setup(
    name="dqchecks",
    version="0.0.5",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "openpyxl",
        "numpy"
    ],
)