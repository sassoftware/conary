#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


# implement decorators for tagging api calls in conary code

def apiDecorator(api_type):
    def _apiDecorator(func):
        lines = ['']
        marked = False
        if not func.__doc__:
            func.__doc__ = func.__name__
        lines = func.__doc__.split('\n')
        for idx, line in enumerate(lines):
            if '@' in line:
                l = line.replace('\t', '        ')
                l2 = line.lstrip()
                indent = len(l) - len(l2)
                lines.insert(idx, (' ' * indent) + '(%s)' % api_type)
                marked = True
                break

        if not marked:
            lines[0] = lines[0] + ' (%s)' % api_type

        try:
            func.__doc__ = '\n'.join(lines)
        except TypeError:
            # maybe a C function.
            pass
        return func
    return _apiDecorator

publicApi = apiDecorator('PUBLIC API')
developerApi = apiDecorator('DEVELOPER API')
