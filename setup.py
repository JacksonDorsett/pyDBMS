import pathlib
from setuptools import setup

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# This call to setup() does all the work
setup(
    name="pyDBMS",
    version="0.1.2",
    description="Easy to use, light weight ORM",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/JacksonDorsett/pydb",
    author="Jackson Dorsett",
    author_email="dorsettj@gmail.com",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
    packages=['pyDBMS'],
    include_package_data=True,
    install_requires=[],
    # entry_points={
    #     "console_scripts": [
    #         "realpython=reader.__main__:main",
    #     ]
    # },
)
