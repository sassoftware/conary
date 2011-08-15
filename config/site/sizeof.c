/*
 * Copyright (c) rPath, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
