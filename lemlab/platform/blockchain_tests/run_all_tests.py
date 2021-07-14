import os
from pathlib import Path
import subprocess

verbose = False


def run_all_tests():
    parent_path = Path(__file__).parent
    list_tests = os.listdir(parent_path)
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
            proccess = subprocess.Popen(["pytest", str(test_file)])

        for line in proccess.stdout:
            print(line)
            passed = str(line).lower().find("passed")
            failed = str(line).lower().find("failed")
            # the number of tests passed of failed is printed two spaces before the start of the word passed or failed
            if passed != -1:
                tests_passed += int(line[passed - 2])
            elif failed != -1:
                tests_failed += int(line[failed - 2])

    print(f"Tests passed: {tests_passed} \nTests failed: {tests_failed}")


if __name__ == '__main__':
    run_all_tests()
