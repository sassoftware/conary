/*
 * Copyright (c) SAS Institute Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#include <stdio.h>

int main(void) {
    printf("ac_cv_sizeof_char=%d\n", sizeof(char));
    printf("ac_cv_sizeof_short=%d\n", sizeof(short));
    printf("ac_cv_sizeof_int=%d\n", sizeof(int));
    printf("ac_cv_sizeof_long=%d\n", sizeof(long));
    printf("ac_cv_sizeof_long_long=%d\n", sizeof(long long));
    printf("ac_cv_sizeof_double=%d\n", sizeof(double));
    printf("ac_cv_sizeof_char_p=%d\n", sizeof(char *));
}
