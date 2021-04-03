# Common errors when installing the environment

Here we describe the common errors reported and how to fix them.

## Operational System - MAC OS

### Error Message: 

error: command 'clang' failed with exit status 1

```
Requirement already satisfied: pbr!=2.1.0,>=2.0.0 in ./.venv/lib/python3.7/site-packages (from stevedore->doc8==0.8.1->-r requirements-dev.txt (line 7)) (5.5.1)
Using legacy 'setup.py install' for python-Levenshtein, since package 'wheel' is not installed.
Installing collected packages: awswrangler, python-Levenshtein
  Attempting uninstall: awswrangler
    Found existing installation: awswrangler 2.6.0
    Uninstalling awswrangler-2.6.0:
      Successfully uninstalled awswrangler-2.6.0
  Running setup.py develop for awswrangler
    Running setup.py install for python-Levenshtein ... error
    ERROR: Command errored out with exit status 1:
     command: /Users/$USER/Projects/aws-data-wrangler/.venv/bin/python3 -u -c 'import sys, setuptools, tokenize; sys.argv[0] = '"'"'/private/var/folders/f5/6dztqmhd6tb0mlxyzg2w32xdrmsqz_/T/pip-install-eobnhvyq/python-levenshtein_3ce6f911c6454215835eb21e204f9564/setup.py'"'"'; __file__='"'"'/private/var/folders/f5/6dztqmhd6tb0mlxyzg2w32xdrmsqz_/T/pip-install-eobnhvyq/python-levenshtein_3ce6f911c6454215835eb21e204f9564/setup.py'"'"';f=getattr(tokenize, '"'"'open'"'"', open)(__file__);code=f.read().replace('"'"'\r\n'"'"', '"'"'\n'"'"');f.close();exec(compile(code, __file__, '"'"'exec'"'"'))' install --record /private/var/folders/f5/6dztqmhd6tb0mlxyzg2w32xdrmsqz_/T/pip-record-mh08s0za/install-record.txt --single-version-externally-managed --compile --install-headers /Users/$USER/Projects/aws-data-wrangler/.venv/include/site/python3.7/python-Levenshtein
         cwd: /private/var/folders/f5/6dztqmhd6tb0mlxyzg2w32xdrmsqz_/T/pip-install-eobnhvyq/python-levenshtein_3ce6f911c6454215835eb21e204f9564/
    Complete output (31 lines):
    running install
    running build
    running build_py
    creating build
    creating build/lib.macosx-10.13-x86_64-3.7
    creating build/lib.macosx-10.13-x86_64-3.7/Levenshtein
    copying Levenshtein/StringMatcher.py -> build/lib.macosx-10.13-x86_64-3.7/Levenshtein
    copying Levenshtein/__init__.py -> build/lib.macosx-10.13-x86_64-3.7/Levenshtein
    running egg_info
    writing python_Levenshtein.egg-info/PKG-INFO
    writing dependency_links to python_Levenshtein.egg-info/dependency_links.txt
    writing entry points to python_Levenshtein.egg-info/entry_points.txt
    writing namespace_packages to python_Levenshtein.egg-info/namespace_packages.txt
    writing requirements to python_Levenshtein.egg-info/requires.txt
    writing top-level names to python_Levenshtein.egg-info/top_level.txt
    reading manifest file 'python_Levenshtein.egg-info/SOURCES.txt'
    reading manifest template 'MANIFEST.in'
    warning: no previously-included files matching '*pyc' found anywhere in distribution
    warning: no previously-included files matching '*so' found anywhere in distribution
    warning: no previously-included files matching '.project' found anywhere in distribution
    warning: no previously-included files matching '.pydevproject' found anywhere in distribution
    writing manifest file 'python_Levenshtein.egg-info/SOURCES.txt'
    copying Levenshtein/_levenshtein.c -> build/lib.macosx-10.13-x86_64-3.7/Levenshtein
    copying Levenshtein/_levenshtein.h -> build/lib.macosx-10.13-x86_64-3.7/Levenshtein
    running build_ext
    building 'Levenshtein._levenshtein' extension
    creating build/temp.macosx-10.13-x86_64-3.7
    creating build/temp.macosx-10.13-x86_64-3.7/Levenshtein
    clang -Wno-unused-result -Wsign-compare -Wunreachable-code -fno-common -dynamic -DNDEBUG -g -fwrapv -O3 -Wall -I/usr/local/include -I/usr/local/opt/openssl@1.1/include -I/usr/local/opt/sqlite/include -I/Users/$USER/Projects/aws-data-wrangler/.venv/include -I/usr/local/Cellar/python/3.7.7/Frameworks/Python.framework/Versions/3.7/include/python3.7m -c Levenshtein/_levenshtein.c -o build/temp.macosx-10.13-x86_64-3.7/Levenshtein/_levenshtein.o
    xcrun: error: invalid active developer path (/Library/Developer/CommandLineTools), missing xcrun at: /Library/Developer/CommandLineTools/usr/bin/xcrun
    error: command 'clang' failed with exit status 1
    ----------------------------------------
ERROR: Command errored out with exit status 1: /Users/$USER/Projects/aws-data-wrangler/.venv/bin/python3 -u -c 'import sys, setuptools, tokenize; sys.argv[0] = '"'"'/private/var/folders/f5/6dztqmhd6tb0mlxyzg2w32xdrmsqz_/T/pip-install-eobnhvyq/python-levenshtein_3ce6f911c6454215835eb21e204f9564/setup.py'"'"'; __file__='"'"'/private/var/folders/f5/6dztqmhd6tb0mlxyzg2w32xdrmsqz_/T/pip-install-eobnhvyq/python-levenshtein_3ce6f911c6454215835eb21e204f9564/setup.py'"'"';f=getattr(tokenize, '"'"'open'"'"', open)(__file__);code=f.read().replace('"'"'\r\n'"'"', '"'"'\n'"'"');f.close();exec(compile(code, __file__, '"'"'exec'"'"'))' install --record /private/var/folders/f5/6dztqmhd6tb0mlxyzg2w32xdrmsqz_/T/pip-record-mh08s0za/install-record.txt --single-version-externally-managed --compile --install-headers /Users/$USER/Projects/aws-data-wrangler/.venv/include/site/python3.7/python-Levenshtein Check the logs for full command output.
(.venv) ~/Projects/aws-data-wrangler> pip list
```

### Solution

https://stackoverflow.com/questions/64618257/pip-install-in-a-mac

```
xcode-select --install
```

-----

### Error Message: 

ImportError: dlopen(/Users/$USER/Projects/aws-data-wrangler/.venv/lib/python3.7/site-packages/pyodbc.cpython-37m-darwin.so, 2): Library not loaded: /usr/local/opt/unixodbc/lib/libodbc.2.dylib


```
ImportError while loading conftest '/Users/$USER/Projects/aws-data-wrangler/tests/conftest.py'.
tests/conftest.py:6: in <module>
    import awswrangler as wr
awswrangler/__init__.py:10: in <module>
    from awswrangler import (  # noqa
awswrangler/sqlserver.py:20: in <module>
    import pyodbc  # pylint: disable=import-error
E   ImportError: dlopen(/Users/$USER/Projects/aws-data-wrangler/.venv/lib/python3.7/site-packages/pyodbc.cpython-37m-darwin.so, 2): Library not loaded: /usr/local/opt/unixodbc/lib/libodbc.2.dylib
E     Referenced from: /Users/$USER/Projects/aws-data-wrangler/.venv/lib/python3.7/site-packages/pyodbc.cpython-37m-darwin.so
E     Reason: image not found
(.venv) ~/Projects/aws-data-wrangler> python
Python 3.7.7 (default, Mar 10 2020, 15:43:27)
[Clang 10.0.0 (clang-1000.11.45.5)] on darwin
Type "help", "copyright", "credits" or "license" for more information.
>>> import awswrangler
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/Users/$USER/Projects/aws-data-wrangler/awswrangler/__init__.py", line 10, in <module>
    from awswrangler import (  # noqa
  File "/Users/$USER/Projects/aws-data-wrangler/awswrangler/sqlserver.py", line 20, in <module>
    import pyodbc  # pylint: disable=import-error
ImportError: dlopen(/Users/$USER/Projects/aws-data-wrangler/.venv/lib/python3.7/site-packages/pyodbc.cpython-37m-darwin.so, 2): Library not loaded: /usr/local/opt/unixodbc/lib/libodbc.2.dylib
  Referenced from: /Users/$USER/Projects/aws-data-wrangler/.venv/lib/python3.7/site-packages/pyodbc.cpython-37m-darwin.so
  Reason: image not found
>>>
KeyboardInterrupt
```

### Solution

https://stackoverflow.com/questions/54302793/dyld-library-not-loaded-usr-local-opt-unixodbc-lib-libodbc-2-dylib

```
brew install unixodbc
```

-----