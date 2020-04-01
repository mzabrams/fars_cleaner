import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="fars-cleaner",
    version="1.0.0",
    author="Mitchell Abrams",
    author_email="mitchell.abrams@duke.edu",
    description="A python package for loading and preprocessing the FARS database",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/mzabrams/fars-cleaner",
    packages=setuptools.find_packages(),
    classifiers=(
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Development Status :: 4 - Beta",
    ),
)