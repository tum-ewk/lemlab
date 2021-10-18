
"""
This file reads all the test files inside the same folder and executes them by command shell using pytest
The parameter verbose, can be set to true to display more information during testing, but note that it will display
all information, so the flow of data on the command window can be big
The tests are executed via command shell as it is quite faster than calling the functions with pytest

Note: some files (like test_equals.py) have more than one test, so that is why the results are bigger
than the number of test files
"""
import os
from pathlib import Path
import subprocess

verbose = False


def run_all_tests():
    parent_path = Path(__file__).parent
    list_tests = os.listdir(parent_path)
    # we first filter out non test files (like test utils)
    for test_file in list_tests:
        if str(test_file).find("lem_blockchain_") == -1:
            list_tests.remove(test_file)

    print("Starting testing of all test files")
    tests_passed = 0
    tests_failed = 0
    for test_file in list_tests:
        test_file = os.path.abspath(test_file)
        if verbose:
            proccess = subprocess.Popen(["pytest", "-o", "log_cli=True", "-s", str(test_file)],
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        else:
            proccess = subprocess.Popen(["pytest", str(test_file)],
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        for line in proccess.stdout:
            print(line)
            passed = str(line).lower().find("passed")
            failed = str(line).lower().find("failed")
            # the number of tests passed or failed is printed two spaces before the start of the word passed or failed
            if passed != -1:
                try:
                    tests_passed += int(str(line)[passed - 2])
                except ValueError:
                    tests_passed += 1
                print("Tests passed so far", tests_passed)
            elif failed != -1:
                try:
                    tests_failed += int(str(line)[failed - 2])
                except ValueError:
                    tests_failed += 1
                print("Tests failed so far", tests_failed)

    print("==================== TESTING FINISHED ======================")
    print(f"Tests passed: {tests_passed} \nTests failed: {tests_failed}")
    print("============================================================")


if __name__ == '__main__':
    run_all_tests()
