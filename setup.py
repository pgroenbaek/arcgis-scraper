from setuptools import setup, find_packages

setup(
    name="arcgis-scraper",
    version="1.0.1",
    packages=find_packages(),
    install_requires=[
        "pandas>=1.0"
    ],
    author="Peter Grønbæk Andersen",
    author_email="peter@grnbk.io",
    description="An ArcGIS REST API scraper",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/pgroenbaek/arcgis-scraper",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    license="MIT",
)