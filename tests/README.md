# Tests

## Test environment
Create the folder `venv` within the project folder. This folder will contain your virtual test environment:
```
mkdir venv
```
Create a virtual environment for your tests and activate it:
```
python3 -m venv venv/testenv
source venv/testenv/bin/activate
```
Install the various required libraries:
```
(testenv) pip install .
(testenv) pip install pytest
```

## Required environment variables

## Run unit tests
To run all unit tests in the `tests` folder, run in the project folder (make sure your test environment is activated):
```
(testenv) python -m unittest discover --buffer
```
A more verbose output can be obtained like so:
```
(testenv) python -m unittest discover --buffer -v
```
Prior committing new code to the repository all tests must run successfully.
