#### CFPB Open Source Project Template Instructions

1. Create a new project.
2. [Copy these files into the new project](#installation)
3. Update the README, replacing the contents below as prescribed.
4. Add any libraries, assets, or hard dependencies whose source code will be included
   in the project's repository to the _Exceptions_ section in the [TERMS](TERMS.md).
  - If no exceptions are needed, remove that section from TERMS.
5. If working with an existing code base, answer the questions on the [open source checklist](opensource-checklist.md)
6. Delete these instructions and everything up to the _Project Title_ from the README.
7. Write some great software and tell people about it.

> Keep the README fresh! It's the first thing people see and will make the initial impression.

## Installation

To install pydb to your machine, run the following script from the root of your project's directory:

```
pip3 install pydb
```

----

# Pydb

**Description**:  Pydb provides users with a light-weight, easy to use ORM(Object Relational Mapping) for multiple DBMS systems. The primary goal is to make communicating between databases as easy as possible using a unified base model type and abstract database interface. Pydb is designed to be easily extended to other languages as needed by the user. 


## Dependencies

When pydb is installed all libraries that will also be installed through pip. 
Pydb currently has no external dependencies until non-native databases are supported.

## Usage

Show users how to use the software.
Be specific.
Use appropriate formatting when showing code snippets.

## How to test the software

Pydb is developed using Test-Driven Development. All the unittests can be run using the following command in the root directory:
```bash
python3 -m unittest discover
```

## Known issues

Pydb currently only supports the Sqlite database as the requirements are being elicited.

## Getting help

If you have questions, concerns, bug reports, etc, please file an issue in this repository's Issue Tracker.


## Getting involved

If you are interested in contributing fixes or features to MonoGame, please read our [CONTRIBUTOR](CONTRIBUTING.md) guide first.
