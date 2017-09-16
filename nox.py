#MIT License
#
#Copyright (c) 2017 Willian Fuks
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in all
#copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#SOFTWARE.

import os
import nox

@nox.session
@nox.parametrize('python_version', ['3.6'])
def unit_tests(session, python_version):
    """Run just unit testings"""

    session.intepreter = 'python{}'.format(python_version)

    # Set virtualenv dirname
    session.virtualenv_dirname = 'unit-' + python_version

    session.install('mock', 'pytest', 'pytest-cov')
    session.install('-e', '.')

    session.run('py.test',
                '--quite',
                '--cov=tests.unit',
                '--cov-append',
                '--cov-config=.coveragerc',
                '--cov-report=',
                '--cov-fail-under=100',
                os.path.join('tests', 'unit'),
                *session.posargs
                )

@nox.session
@nox.parametrize('python_version', ['3.6'])
def system_tests(session, python_version):
    """Run tests against a live spark (preferably a local cluster)."""

    session.interpreter = 'python{}'.format(python_version)

    session.virtualenv_dirname = 'sys-' + python_version

    session.install('mock', 'pytest')
    session.install('-e', '.')

    session.run('py.test',
                '--quiet',
                os.path.join('tests', 'system.py'),
                *session.posargs
                )
